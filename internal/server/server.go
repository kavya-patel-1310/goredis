// Package server implements the GoRedis server: networking, the command
// executor, keyspace maintenance (expiry, eviction), pub/sub, transactions,
// blocking operations and persistence.
//
// # Concurrency model
//
// Every connection gets its own goroutine that parses requests and writes
// replies, so socket I/O and protocol parsing run in parallel across cores.
// Command *execution* is serialized by a single mutex (s.mu), mirroring
// Redis' single-threaded executor: every command is atomic, MULTI/EXEC is
// trivially isolated, and data structures need no fine-grained locking.
// (This is the same split Redis 6+ uses with io-threads.)
package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds server settings. Fields marked (runtime) can be changed
// with CONFIG SET.
type Config struct {
	Addr             string
	Dir              string
	AppendOnly       bool
	AppendFilename   string
	AppendFsync      string // always | everysec | no  (runtime)
	SnapshotFilename string
	SaveSeconds      int    // auto-snapshot if SaveChanges writes happened in SaveSeconds (0 = off)
	SaveChanges      int64  //
	MaxMemory        int64  // bytes, 0 = unlimited (runtime)
	MaxMemoryPolicy  string // (runtime)
	MaxMemorySamples int    // (runtime)
	RequirePass      string // (runtime)
	SlowlogSlowerUs  int64  // (runtime)
	SlowlogMaxLen    int    // (runtime)
	MaxClients       int
	Logger           *log.Logger
}

func DefaultConfig() Config {
	return Config{
		Addr:             ":6379",
		Dir:              ".",
		AppendFilename:   "appendonly.aof",
		AppendFsync:      "everysec",
		SnapshotFilename: "dump.grdb",
		SaveSeconds:      60,
		SaveChanges:      1000,
		MaxMemoryPolicy:  "noeviction",
		MaxMemorySamples: 5,
		SlowlogSlowerUs:  10000,
		SlowlogMaxLen:    128,
		MaxClients:       10000,
	}
}

// Server is a GoRedis instance.
type Server struct {
	cfg Config
	log *log.Logger

	mu        sync.Mutex // the executor lock — see package comment
	db        *DB
	commands  map[string]*Command
	now       int64   // unix ms of the command being executed
	curClient *Client // client whose command is executing
	dirty     int64   // writes since last snapshot
	loading   bool    // replaying the AOF

	clients    map[int64]*Client
	nextID     atomic.Int64
	numClients atomic.Int64

	watched      map[string]map[*Client]struct{}
	blockingKeys map[string][]*Client
	readyKeys    []string
	readySet     map[string]struct{}

	pubsub     pubsub
	monitors   map[*Client]struct{}
	monSinks   map[int]chan string
	nextSinkID int

	aof           *aof
	lastSave      time.Time
	lastSaveOK    bool
	bgSaveRunning bool
	rewriting     bool
	lastBgSaveDur time.Duration

	slowlog slowlog
	stats   stats

	ln        net.Listener
	startTime time.Time
	quit      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

type stats struct {
	totalCommands    atomic.Int64
	totalConnections atomic.Int64
	rejectedConns    atomic.Int64
	netInput         atomic.Int64
	netOutput        atomic.Int64
	hits             atomic.Int64
	misses           atomic.Int64
	expired          atomic.Int64
	evicted          atomic.Int64
	pubsubMessages   atomic.Int64

	histMu  sync.Mutex
	history []Sample
	opsSec  atomic.Int64
	lastOps int64
}

// Sample is one point of the 1-second metrics history used by the dashboard.
type Sample struct {
	T          int64   `json:"t"`
	OpsPerSec  int64   `json:"ops"`
	UsedMemory int64   `json:"mem"`
	Keys       int     `json:"keys"`
	Clients    int64   `json:"clients"`
	HitRate    float64 `json:"hitRate"`
	NetInKBps  float64 `json:"inKBps"`
	NetOutKBps float64 `json:"outKBps"`
}

// New creates a server and loads persisted data (AOF or snapshot).
func New(cfg Config) (*Server, error) {
	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	s := &Server{
		cfg:          cfg,
		log:          cfg.Logger,
		db:           newDB(),
		clients:      make(map[int64]*Client),
		watched:      make(map[string]map[*Client]struct{}),
		blockingKeys: make(map[string][]*Client),
		readySet:     make(map[string]struct{}),
		pubsub:       newPubSub(),
		monitors:     make(map[*Client]struct{}),
		monSinks:     make(map[int]chan string),
		startTime:    time.Now(),
		lastSave:     time.Now(),
		lastSaveOK:   true,
		quit:         make(chan struct{}),
	}
	s.slowlog.maxLen = cfg.SlowlogMaxLen
	s.commands = buildCommandTable()
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	if err := s.loadData(); err != nil {
		return nil, err
	}
	if cfg.AppendOnly {
		a, err := openAOF(filepath.Join(cfg.Dir, cfg.AppendFilename), cfg.AppendFsync)
		if err != nil {
			return nil, err
		}
		s.aof = a
	}
	return s, nil
}

func (s *Server) loadData() error {
	start := time.Now()
	aofPath := filepath.Join(s.cfg.Dir, s.cfg.AppendFilename)
	snapPath := filepath.Join(s.cfg.Dir, s.cfg.SnapshotFilename)
	if s.cfg.AppendOnly {
		if _, err := os.Stat(aofPath); err == nil {
			n, err := s.replayAOF(aofPath)
			if err != nil {
				return fmt.Errorf("loading AOF: %w", err)
			}
			s.log.Printf("DB loaded from append only file: %d commands, %d keys in %v", n, s.db.Len(), time.Since(start).Round(time.Millisecond))
			return nil
		}
	}
	if _, err := os.Stat(snapPath); err == nil {
		if err := s.loadSnapshot(snapPath); err != nil {
			return fmt.Errorf("loading snapshot: %w", err)
		}
		s.log.Printf("DB loaded from snapshot: %d keys in %v", s.db.Len(), time.Since(start).Round(time.Millisecond))
	}
	return nil
}

// ListenAndServe starts accepting connections and blocks until Close.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Addr returns the bound address (useful when listening on port 0).
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	s.wg.Add(1)
	go s.cron()
	if s.aof != nil {
		s.wg.Add(1)
		go s.aofFsyncLoop()
	}
	s.log.Printf("GoRedis ready to accept connections on %s", ln.Addr())
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil
			default:
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			return err
		}
		if s.numClients.Load() >= int64(s.cfg.MaxClients) {
			s.stats.rejectedConns.Add(1)
			conn.Write([]byte("-ERR max number of clients reached\r\n"))
			conn.Close()
			continue
		}
		go s.serveConn(conn)
	}
}

// Close shuts the server down gracefully: stop accepting, flush and fsync
// the AOF, and write a final snapshot if snapshotting is enabled.
func (s *Server) Close() error { return s.shutdown(!s.cfg.AppendOnly && s.cfg.SaveSeconds > 0) }

func (s *Server) shutdown(save bool) error {
	var err error
	s.closeOnce.Do(func() {
		close(s.quit)
		s.mu.Lock()
		if s.ln != nil {
			s.ln.Close()
		}
		for _, c := range s.clients {
			c.close()
		}
		s.mu.Unlock()
		s.wg.Wait()
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.aof != nil {
			err = s.aof.close()
		}
		if save {
			if e := s.saveSnapshotLocked(); e != nil {
				err = e
			}
		}
		s.log.Printf("GoRedis is now ready to exit, bye bye...")
	})
	return err
}

// Done is closed when the server shuts down (e.g. via SHUTDOWN).
func (s *Server) Done() <-chan struct{} { return s.quit }

// ---------------------------------------------------------------------------
// Connection handling

func (s *Server) serveConn(conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
		tc.SetKeepAlive(true)
	}
	c := newClient(s, conn)
	s.mu.Lock()
	s.clients[c.id] = c
	s.mu.Unlock()
	s.numClients.Add(1)
	s.stats.totalConnections.Add(1)
	defer s.freeClient(c)

	batch := make([][][]byte, 0, 32)
	for {
		args, err := c.r.ReadCommand()
		if err != nil {
			c.protocolError(err)
			return
		}
		// Gather every complete command already buffered (pipelining) so the
		// whole batch executes under a single acquisition of the executor lock.
		batch = append(batch[:0], args)
		var pending error
		for len(batch) < 256 && c.r.Buffered() > 0 && c.r.HasCompleteCommand() {
			a, err := c.r.ReadCommand()
			if err != nil {
				pending = err
				break
			}
			batch = append(batch, a)
		}

		s.mu.Lock()
		for _, args := range batch {
			s.processCommand(c, args)
			if blk := c.blocked; blk != nil {
				c.commit()
				s.mu.Unlock()
				s.waitBlocked(c, blk)
				s.mu.Lock()
			}
			if c.quit.Load() || c.closed.Load() {
				break
			}
		}
		c.commit()
		c.lastInteraction.Store(s.now)
		s.mu.Unlock()

		if pending != nil {
			c.protocolError(pending)
			return
		}
		if c.pendingOut() > 64*1024 {
			c.flushAll()
		}
		if c.quit.Load() || c.closed.Load() {
			c.flushAll()
			return
		}
	}
}

func (s *Server) freeClient(c *Client) {
	c.close()
	s.mu.Lock()
	delete(s.clients, c.id)
	s.unwatchAll(c)
	if c.blocked != nil {
		s.unblock(c)
	}
	s.pubsub.unsubscribeAll(c)
	delete(s.monitors, c)
	s.mu.Unlock()
	s.numClients.Add(-1)
}

// ---------------------------------------------------------------------------
// Background maintenance ("serverCron")

func (s *Server) cron() {
	defer s.wg.Done()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	n := 0
	lastIn, lastOut := s.stats.netInput.Load(), s.stats.netOutput.Load()
	s.stats.lastOps = s.stats.totalCommands.Load()
	for {
		select {
		case <-s.quit:
			return
		case <-tick.C:
		}
		n++
		s.mu.Lock()
		s.now = time.Now().UnixMilli()
		s.activeExpireCycle()
		s.mu.Unlock()

		if n%10 == 0 {
			// 1-second metrics sample for INFO and the dashboard.
			total := s.stats.totalCommands.Load()
			ops := total - s.stats.lastOps
			s.stats.lastOps = total
			s.stats.opsSec.Store(ops)
			in, out := s.stats.netInput.Load(), s.stats.netOutput.Load()
			h, m := s.stats.hits.Load(), s.stats.misses.Load()
			hr := 0.0
			if h+m > 0 {
				hr = float64(h) / float64(h+m)
			}
			s.mu.Lock()
			smp := Sample{T: time.Now().UnixMilli(), OpsPerSec: ops, UsedMemory: s.db.used, Keys: s.db.Len(),
				Clients: s.numClients.Load(), HitRate: hr,
				NetInKBps: float64(in-lastIn) / 1024, NetOutKBps: float64(out-lastOut) / 1024}
			s.maybeAutoSave()
			s.mu.Unlock()
			lastIn, lastOut = in, out
			s.stats.histMu.Lock()
			s.stats.history = append(s.stats.history, smp)
			if len(s.stats.history) > 300 {
				s.stats.history = s.stats.history[len(s.stats.history)-300:]
			}
			s.stats.histMu.Unlock()
		}
	}
}

// activeExpireCycle is Redis' adaptive expiry algorithm: sample 20 random
// keys that have a TTL, delete the expired ones, and repeat while more than
// 25% of the sample was expired (bounded by a time budget). This reclaims
// memory from keys that are never accessed again, without scanning the
// whole keyspace.
func (s *Server) activeExpireCycle() {
	const sample = 20
	deadline := time.Now().Add(5 * time.Millisecond)
	for iter := 0; ; iter++ {
		if len(s.db.volatile) == 0 {
			return
		}
		expired := 0
		for i := 0; i < sample && len(s.db.volatile) > 0; i++ {
			e := s.db.randomVolatile()
			if e.expireAt <= s.now {
				s.expireKey(e)
				expired++
			}
		}
		if expired*4 <= sample {
			return
		}
		if iter%16 == 0 && time.Now().After(deadline) {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Keyspace helpers used by command handlers (s.mu held)

func (s *Server) isExpired(e *entry) bool { return e.expireAt > 0 && e.expireAt <= s.now }

func (s *Server) lookupRead(key string) *entry {
	e := s.db.get(key)
	if e != nil && s.isExpired(e) {
		s.expireKey(e)
		e = nil
	}
	if e == nil {
		s.stats.misses.Add(1)
		return nil
	}
	s.stats.hits.Add(1)
	e.lru = s.now
	return e
}

func (s *Server) lookupWrite(key string) *entry {
	e := s.db.get(key)
	if e != nil && s.isExpired(e) {
		s.expireKey(e)
		return nil
	}
	if e != nil {
		e.lru = s.now
	}
	return e
}

func (s *Server) expireKey(e *entry) {
	s.db.remove(e)
	s.stats.expired.Add(1)
	s.touchWatched(e.key)
}

func (s *Server) deleteEntry(e *entry) {
	s.db.remove(e)
	s.touchWatched(e.key)
}

func (s *Server) newEntry(key string, t ValueType) *entry {
	return &entry{key: key, typ: t, lru: s.now, eidx: -1}
}

// setKey stores a fresh entry, replacing any existing value (and its TTL).
func (s *Server) setKey(e *entry) {
	if old := s.db.get(e.key); old != nil {
		s.db.remove(old)
	}
	s.db.add(e)
}

// evictIfNeeded frees memory according to maxmemory-policy. Like Redis it
// uses *approximated* LRU: sample N random keys and evict the least recently
// used one, repeating until under the limit. Returns false if the limit
// cannot be met (noeviction, or no eligible keys).
func (s *Server) evictIfNeeded() bool {
	max := s.cfg.MaxMemory
	if max <= 0 || s.db.used <= max {
		return true
	}
	policy := s.cfg.MaxMemoryPolicy
	if policy == "noeviction" {
		return false
	}
	volatileOnly := strings.HasPrefix(policy, "volatile-")
	for s.db.used > max {
		var best *entry
		for i := 0; i < s.cfg.MaxMemorySamples; i++ {
			var e *entry
			if volatileOnly {
				e = s.db.randomVolatile()
			} else {
				e = s.db.randomEntry()
			}
			if e == nil {
				break
			}
			switch {
			case best == nil:
				best = e
			case strings.HasSuffix(policy, "-lru") && e.lru < best.lru:
				best = e
			case policy == "volatile-ttl" && e.expireAt < best.expireAt:
				best = e
			}
			if strings.HasSuffix(policy, "-random") {
				break
			}
		}
		if best == nil {
			return false
		}
		s.deleteEntry(best)
		s.stats.evicted.Add(1)
		s.propagate([][]byte{[]byte("DEL"), []byte(best.key)})
	}
	return true
}

func (s *Server) logf(format string, a ...any) { s.log.Printf(format, a...) }
