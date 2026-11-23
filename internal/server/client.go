package server

import (
	"errors"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"goredis/internal/resp"
)

const pubsubOutputLimit = 32 * 1024 * 1024 // like client-output-buffer-limit pubsub

// Client is one connected client (or an internal pseudo-client used for
// AOF replay and the web console).
type Client struct {
	s    *Server
	id   int64
	conn net.Conn
	addr string
	r    *resp.Reader
	w    resp.Writer // reply buffer for the command being executed (s.mu held)
	ctx  Ctx

	// Output: replies are committed into `out` under omu. Whoever needs the
	// bytes on the wire calls flush(); asynchronous producers (PUBLISH,
	// MONITOR, blocking-pop wakeups) append under omu and schedule a flush,
	// so a slow consumer can never block the executor.
	omu        sync.Mutex
	out        []byte
	spare      []byte
	flushSched bool
	fmu        sync.Mutex

	closed          atomic.Bool
	quit            atomic.Bool
	lastInteraction atomic.Int64
	createdAt       time.Time

	// Fields below are guarded by s.mu.
	name       string
	lastCmd    string
	authed     bool
	internal   bool
	noBlock    bool
	inMulti    bool
	inExec     bool
	execProp   bool
	multiErr   bool
	dirtyCAS   bool
	multiQueue [][][]byte
	watched    []string
	subs       map[string]struct{}
	psubs      map[string]struct{}
	monitor    bool
	blocked    *blockState
	wroteAOF   bool
}

func newClient(s *Server, conn net.Conn) *Client {
	c := &Client{s: s, id: s.nextID.Add(1), conn: conn, createdAt: time.Now()}
	if conn != nil {
		c.addr = conn.RemoteAddr().String()
		c.r = resp.NewReader(&hookReader{c: c})
	}
	c.lastInteraction.Store(time.Now().UnixMilli())
	return c
}

// newInternalClient creates a connection-less client.
func (s *Server) newInternalClient(name string) *Client {
	c := newClient(s, nil)
	c.internal = true
	c.authed = true
	c.noBlock = true
	c.name = name
	c.addr = name
	return c
}

// hookReader wraps the socket. It runs *only* when the parser has consumed
// everything buffered and must block on the network — exactly the moment to
// flush pending replies. This gives automatic reply batching for pipelined
// requests (one write per batch) without ever deadlocking a client that
// waits for replies before sending more.
type hookReader struct{ c *Client }

func (h *hookReader) Read(p []byte) (int, error) {
	h.c.flushAll()
	n, err := h.c.conn.Read(p)
	h.c.s.stats.netInput.Add(int64(n))
	return n, err
}

func (c *Client) protocolError(err error) {
	var pe *resp.ProtocolError
	if errors.As(err, &pe) {
		c.w.Error("ERR " + pe.Error())
		c.commit()
		c.flushAll()
	}
}

// commit moves the current reply buffer into the output buffer.
func (c *Client) commit() {
	if len(c.w.Buf) == 0 {
		return
	}
	c.omu.Lock()
	c.out = append(c.out, c.w.Buf...)
	c.omu.Unlock()
	c.w.Reset()
}

func (c *Client) pendingOut() int {
	c.omu.Lock()
	defer c.omu.Unlock()
	return len(c.out)
}

// appendOut adds bytes to the output buffer without scheduling a flush (the
// client's own goroutine will flush). Used for blocked-client wakeups.
func (c *Client) appendOut(b []byte) {
	c.omu.Lock()
	c.out = append(c.out, b...)
	c.omu.Unlock()
}

// pushAsync is used by other goroutines (pub/sub, monitor). It never blocks
// on the network; it enforces an output buffer limit for slow consumers.
func (c *Client) pushAsync(b []byte) {
	if c.internal || c.closed.Load() {
		return
	}
	c.omu.Lock()
	c.out = append(c.out, b...)
	if len(c.out) > pubsubOutputLimit {
		c.omu.Unlock()
		c.s.logf("client %s closed for overcoming of output buffer limits", c.addr)
		c.close()
		return
	}
	sched := !c.flushSched
	c.flushSched = true
	c.omu.Unlock()
	if sched {
		go c.flushLoop()
	}
}

// flushAll makes sure the AOF has the client's writes (so a reply is never
// sent before its command is durable per the fsync policy), then writes the
// output buffer to the socket.
func (c *Client) flushAll() {
	if c.wroteAOF && c.s.aof != nil {
		c.s.aof.flush()
		c.wroteAOF = false
	}
	c.flush()
}

func (c *Client) flush() {
	if c.conn == nil {
		return
	}
	c.omu.Lock()
	if len(c.out) == 0 || c.flushSched {
		c.omu.Unlock()
		return
	}
	c.flushSched = true
	c.omu.Unlock()
	c.flushLoop()
}

func (c *Client) flushLoop() {
	c.fmu.Lock()
	defer c.fmu.Unlock()
	for {
		c.omu.Lock()
		if len(c.out) == 0 || c.closed.Load() {
			c.flushSched = false
			c.out = c.out[:0]
			c.omu.Unlock()
			return
		}
		data := c.out
		c.out = c.spare[:0]
		c.omu.Unlock()

		n, err := c.conn.Write(data)
		c.s.stats.netOutput.Add(int64(n))

		c.omu.Lock()
		if cap(data) <= 1<<20 {
			c.spare = data[:0]
		} else {
			c.spare = nil // don't retain huge buffers
		}
		c.omu.Unlock()
		if err != nil {
			c.close()
		}
	}
}

func (c *Client) close() {
	if c.closed.CompareAndSwap(false, true) && c.conn != nil {
		c.conn.Close()
	}
}

// ---------------------------------------------------------------------------
// Blocking operations (BLPOP / BRPOP / BLMOVE-style)

type blockState struct {
	keys    []string
	left    bool
	timeout time.Duration
	done    chan struct{}
	served  bool
}

// waitBlocked parks the connection goroutine until the client is served by
// a push, times out, or disconnects. The executor lock is NOT held while
// waiting, so other clients keep running.
func (s *Server) waitBlocked(c *Client, b *blockState) {
	c.flushAll()
	var timeout <-chan time.Time
	if b.timeout > 0 {
		t := time.NewTimer(b.timeout)
		defer t.Stop()
		timeout = t.C
	}
	// Detect disconnects while parked: Peek blocks until data or EOF without
	// consuming anything.
	gone := make(chan struct{})
	peekDone := make(chan struct{})
	go func() {
		defer close(peekDone)
		if _, err := c.r.Peek(1); err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			close(gone)
		}
	}()
	disconnected := false
	select {
	case <-b.done:
	case <-timeout:
	case <-gone:
		disconnected = true
	case <-s.quit:
	}
	s.mu.Lock()
	if !b.served {
		s.unblock(c)
		if !disconnected {
			c.w.NullArray()
			c.commit()
		}
	}
	c.blocked = nil
	s.mu.Unlock()

	// Stop the peek goroutine before the main loop touches the reader again.
	c.conn.SetReadDeadline(time.Now())
	<-peekDone
	c.conn.SetReadDeadline(time.Time{})
	if disconnected {
		c.close()
	}
}

// block registers c as waiting on keys (s.mu held).
func (s *Server) block(c *Client, keys []string, left bool, timeout time.Duration) {
	c.blocked = &blockState{keys: keys, left: left, timeout: timeout, done: make(chan struct{})}
	for _, k := range keys {
		s.blockingKeys[k] = append(s.blockingKeys[k], c)
	}
}

func (s *Server) unblock(c *Client) {
	if c.blocked == nil {
		return
	}
	for _, k := range c.blocked.keys {
		ws := s.blockingKeys[k]
		for i, w := range ws {
			if w == c {
				ws = append(ws[:i], ws[i+1:]...)
				break
			}
		}
		if len(ws) == 0 {
			delete(s.blockingKeys, k)
		} else {
			s.blockingKeys[k] = ws
		}
	}
}

// signalKeyReady is called by list pushes; waiting clients are served after
// the pushing command has been propagated (so the AOF order is push → pop).
func (s *Server) signalKeyReady(key string) {
	if _, ok := s.blockingKeys[key]; !ok {
		return
	}
	if _, ok := s.readySet[key]; ok {
		return
	}
	s.readySet[key] = struct{}{}
	s.readyKeys = append(s.readyKeys, key)
}

func (s *Server) serveReadyKeys() {
	for len(s.readyKeys) > 0 {
		keys := s.readyKeys
		s.readyKeys = nil
		clear(s.readySet)
		for _, key := range keys {
			for {
				ws := s.blockingKeys[key]
				if len(ws) == 0 {
					break
				}
				e := s.lookupWrite(key)
				if e == nil || e.typ != TList {
					break
				}
				cl := ws[0]
				b := cl.blocked
				var v []byte
				var cmd string
				if b.left {
					v, _ = e.list.PopFront()
					cmd = "LPOP"
				} else {
					v, _ = e.list.PopBack()
					cmd = "RPOP"
				}
				s.unblock(cl)
				b.served = true
				var w resp.Writer
				w.Array(2)
				w.BulkString(key)
				w.Bulk(v)
				cl.appendOut(w.Buf)
				s.dirty++
				if e.list.Len() == 0 {
					s.deleteEntry(e)
				} else {
					s.db.resync(e)
					s.touchWatched(key)
				}
				s.propagate([][]byte{[]byte(cmd), []byte(key)})
				close(b.done)
			}
		}
	}
}

// clientInfo renders one CLIENT LIST line.
func (c *Client) info(now time.Time) string {
	age := int64(now.Sub(c.createdAt).Seconds())
	idle := (now.UnixMilli() - c.lastInteraction.Load()) / 1000
	flags := "N"
	switch {
	case c.blocked != nil:
		flags = "b"
	case len(c.subs)+len(c.psubs) > 0:
		flags = "P"
	case c.inMulti:
		flags = "x"
	case c.monitor:
		flags = "O"
	}
	return "id=" + strconv.FormatInt(c.id, 10) + " addr=" + c.addr + " name=" + c.name +
		" age=" + strconv.FormatInt(age, 10) + " idle=" + strconv.FormatInt(idle, 10) +
		" flags=" + flags + " db=0 sub=" + strconv.Itoa(len(c.subs)) + " psub=" + strconv.Itoa(len(c.psubs)) +
		" multi=" + strconv.Itoa(multiLen(c)) + " cmd=" + c.lastCmd
}

func multiLen(c *Client) int {
	if !c.inMulti {
		return -1
	}
	return len(c.multiQueue)
}
