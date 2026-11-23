package server

import (
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"goredis/internal/resp"
)

// Command flags.
const (
	fWrite    = 1 << iota // modifies the dataset (propagated to the AOF)
	fRead                 // read-only
	fDenyOOM              // may grow memory: rejected when over maxmemory
	fAdmin                // administrative
	fPubSub               // allowed in subscriber mode
	fNoAuth               // allowed before AUTH
	fFast                 // O(1) or O(log n)
	fNoMulti              // cannot be queued inside MULTI
	fBlocking             // may block the client
)

var flagNames = []struct {
	f    int
	name string
}{{fWrite, "write"}, {fRead, "readonly"}, {fDenyOOM, "denyoom"}, {fAdmin, "admin"},
	{fPubSub, "pubsub"}, {fNoAuth, "no-auth"}, {fFast, "fast"}, {fNoMulti, "no-multi"}, {fBlocking, "blocking"}}

// Command describes a command, Redis-style: arity (positive = exact,
// negative = minimum) and a key specification (first, last, step) that the
// executor uses for WATCH invalidation and memory accounting.
type Command struct {
	Name                    string
	Arity                   int
	Flags                   int
	FirstKey, LastKey, Step int
	Handler                 func(*Ctx)

	calls atomic.Int64
	usec  atomic.Int64
	fails atomic.Int64
}

// Ctx is the execution context passed to handlers.
type Ctx struct {
	s    *Server
	c    *Client
	cmd  *Command
	args [][]byte
	w    *resp.Writer
	prop [][][]byte // rewritten form(s) for AOF propagation (nil = args)
	err  bool
}

func (ctx *Ctx) arg(i int) string { return string(ctx.args[i]) }
func (ctx *Ctx) nargs() int       { return len(ctx.args) }

var (
	posInf = math.Inf(1)
	negInf = math.Inf(-1)
)

// Errors.
const (
	errWrongType  = "WRONGTYPE Operation against a key holding the wrong kind of value"
	errSyntax     = "ERR syntax error"
	errNotInt     = "ERR value is not an integer or out of range"
	errNotFloat   = "ERR value is not a valid float"
	errNoKey      = "ERR no such key"
	errOverflow   = "ERR increment or decrement would overflow"
	errIndexRange = "ERR index out of range"
)

func (ctx *Ctx) error(msg string) {
	ctx.err = true
	ctx.w.Error(msg)
}

func (ctx *Ctx) wrongType() { ctx.error(errWrongType) }
func (ctx *Ctx) syntax()    { ctx.error(errSyntax) }

func (ctx *Ctx) intArg(i int) (int64, bool) {
	n, err := strconv.ParseInt(string(ctx.args[i]), 10, 64)
	if err != nil {
		ctx.error(errNotInt)
		return 0, false
	}
	return n, true
}

func (ctx *Ctx) floatArg(i int) (float64, bool) {
	f, ok := parseFloat(string(ctx.args[i]))
	if !ok {
		ctx.error(errNotFloat)
	}
	return f, ok
}

func parseFloat(s string) (float64, bool) {
	switch strings.ToLower(s) {
	case "inf", "+inf":
		return posInf, true
	case "-inf":
		return negInf, true
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f != f { // reject NaN
		return 0, false
	}
	return f, true
}

// typed fetches key and checks its type. Returns (nil, true) if the key
// doesn't exist, (nil, false) after replying WRONGTYPE.
func (ctx *Ctx) typed(key string, t ValueType, write bool) (*entry, bool) {
	var e *entry
	if write {
		e = ctx.s.lookupWrite(key)
	} else {
		e = ctx.s.lookupRead(key)
	}
	if e != nil && e.typ != t {
		ctx.wrongType()
		return nil, false
	}
	return e, true
}

// dirty marks the dataset as modified by this command.
func (ctx *Ctx) dirty(n int) { ctx.s.dirty += int64(n) }

// alsoPropagate replaces the AOF representation of this command.
func (ctx *Ctx) propagateAs(args ...[]byte) { ctx.prop = append(ctx.prop, args) }

func bs(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

// ---------------------------------------------------------------------------

func (s *Server) lookupCommand(name []byte) *Command {
	var buf [32]byte
	if len(name) > len(buf) {
		return nil
	}
	for i, ch := range name {
		if ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		buf[i] = ch
	}
	return s.commands[string(buf[:len(name)])]
}

func arityOK(cmd *Command, n int) bool {
	return (cmd.Arity > 0 && n == cmd.Arity) || (cmd.Arity < 0 && n >= -cmd.Arity)
}

// processCommand validates a request and executes or queues it (s.mu held).
func (s *Server) processCommand(c *Client, args [][]byte) {
	s.curClient = c
	cmd := s.lookupCommand(args[0])
	if len(s.monitors) > 0 || len(s.monSinks) > 0 {
		s.feedMonitors(c, args)
	}
	if cmd == nil {
		var sb strings.Builder
		for _, a := range args[1:] {
			sb.WriteString("'" + truncate(string(a), 64) + "' ")
		}
		c.w.Error("ERR unknown command '" + truncate(string(args[0]), 64) + "', with args beginning with: " + sb.String())
		if c.inMulti {
			c.multiErr = true
		}
		return
	}
	c.lastCmd = cmd.Name
	if !arityOK(cmd, len(args)) {
		c.w.Error("ERR wrong number of arguments for '" + cmd.Name + "' command")
		if c.inMulti {
			c.multiErr = true
		}
		return
	}
	if s.cfg.RequirePass != "" && !c.authed && cmd.Flags&fNoAuth == 0 {
		c.w.Error("NOAUTH Authentication required.")
		return
	}
	if len(c.subs)+len(c.psubs) > 0 && cmd.Flags&fPubSub == 0 {
		c.w.Error("ERR Can't execute '" + cmd.Name + "': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")
		return
	}
	if c.inMulti && cmd.Name != "exec" && cmd.Name != "discard" && cmd.Name != "multi" &&
		cmd.Name != "watch" && cmd.Name != "quit" && cmd.Name != "reset" {
		if cmd.Flags&fNoMulti != 0 {
			c.w.Error("ERR Command not allowed inside a transaction")
			c.multiErr = true
			return
		}
		c.multiQueue = append(c.multiQueue, args)
		c.w.Simple("QUEUED")
		return
	}
	if cmd.Flags&fDenyOOM != 0 && s.cfg.MaxMemory > 0 && !s.loading {
		if !s.evictIfNeeded() {
			c.w.Error("OOM command not allowed when used memory > 'maxmemory'.")
			return
		}
	}
	s.call(c, cmd, args)
}

// call executes a command and performs all post-execution bookkeeping:
// stats, slowlog, memory accounting, WATCH invalidation, AOF propagation
// and serving clients blocked on keys that became ready.
func (s *Server) call(c *Client, cmd *Command, args [][]byte) {
	ctx := &c.ctx
	*ctx = Ctx{s: s, c: c, cmd: cmd, args: args, w: &c.w}
	dirtyBefore := s.dirty
	start := time.Now()
	s.now = start.UnixMilli()

	cmd.Handler(ctx)

	dur := time.Since(start)
	if !s.loading {
		us := dur.Microseconds()
		cmd.calls.Add(1)
		cmd.usec.Add(us)
		if ctx.err {
			cmd.fails.Add(1)
		}
		s.stats.totalCommands.Add(1)
		if s.cfg.SlowlogSlowerUs >= 0 && us >= s.cfg.SlowlogSlowerUs {
			s.slowlog.add(args, us, c)
		}
	}

	if cmd.Flags&fWrite != 0 && s.dirty != dirtyBefore {
		s.forEachKey(cmd, args, func(key string) {
			if e := s.db.get(key); e != nil {
				s.db.resync(e)
			}
			s.touchWatched(key)
		})
		if s.aof != nil && !s.loading {
			if ctx.prop != nil {
				for _, p := range ctx.prop {
					s.propagateFrom(c, p)
				}
			} else {
				s.propagateFrom(c, args)
			}
		}
	}
	if len(s.readyKeys) > 0 {
		s.serveReadyKeys()
	}
}

func (s *Server) forEachKey(cmd *Command, args [][]byte, fn func(string)) {
	if cmd.FirstKey == 0 {
		return
	}
	last := cmd.LastKey
	if last < 0 {
		last = len(args) + last
	}
	for i := cmd.FirstKey; i <= last && i < len(args); i += cmd.Step {
		fn(string(args[i]))
	}
}

// propagate appends a command to the AOF on behalf of the current client.
func (s *Server) propagate(args [][]byte) { s.propagateFrom(s.curClient, args) }

func (s *Server) propagateFrom(c *Client, args [][]byte) {
	if s.aof == nil || s.loading {
		return
	}
	if c != nil && c.inExec && !c.execProp {
		c.execProp = true
		s.aof.feed(bs("MULTI"))
	}
	s.aof.feed(args)
	if c != nil {
		c.wroteAOF = true
	}
}

// ---------------------------------------------------------------------------
// WATCH support (optimistic locking)

func (s *Server) touchWatched(key string) {
	if len(s.watched) == 0 {
		return
	}
	for c := range s.watched[key] {
		c.dirtyCAS = true
	}
}

func (s *Server) touchAllWatched() {
	for _, cs := range s.watched {
		for c := range cs {
			c.dirtyCAS = true
		}
	}
}

func (s *Server) unwatchAll(c *Client) {
	for _, k := range c.watched {
		if m := s.watched[k]; m != nil {
			delete(m, c)
			if len(m) == 0 {
				delete(s.watched, k)
			}
		}
	}
	c.watched = nil
	c.dirtyCAS = false
}

// ---------------------------------------------------------------------------
// MONITOR

func (s *Server) feedMonitors(c *Client, args [][]byte) {
	if s.loading {
		return
	}
	var sb strings.Builder
	now := time.Now()
	sb.WriteString(strconv.FormatInt(now.Unix(), 10))
	sb.WriteByte('.')
	us := strconv.Itoa(now.Nanosecond() / 1000)
	sb.WriteString(strings.Repeat("0", 6-len(us)) + us)
	sb.WriteString(" [0 " + c.addr + "]")
	for _, a := range args {
		sb.WriteString(" " + strconv.Quote(string(a)))
	}
	line := sb.String()
	if len(s.monitors) > 0 {
		var w resp.Writer
		w.Simple(line)
		for m := range s.monitors {
			if m != c {
				m.pushAsync(w.Buf)
			}
		}
	}
	for _, ch := range s.monSinks {
		select {
		case ch <- line:
		default: // drop if the dashboard can't keep up
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ---------------------------------------------------------------------------
// SLOWLOG

type slowlogEntry struct {
	ID       int64    `json:"id"`
	Time     int64    `json:"time"`
	Duration int64    `json:"durationUs"`
	Args     []string `json:"args"`
	Client   string   `json:"client"`
	Name     string   `json:"clientName"`
}

type slowlog struct {
	entries []slowlogEntry // newest first
	nextID  int64
	maxLen  int
}

func (l *slowlog) add(args [][]byte, us int64, c *Client) {
	if l.maxLen <= 0 {
		return
	}
	n := min(len(args), 32)
	a := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if i == 31 && len(args) > 32 {
			a = append(a, "... ("+strconv.Itoa(len(args)-31)+" more arguments)")
			break
		}
		a = append(a, truncate(string(args[i]), 128))
	}
	e := slowlogEntry{ID: l.nextID, Time: time.Now().Unix(), Duration: us, Args: a, Client: c.addr, Name: c.name}
	l.nextID++
	l.entries = append([]slowlogEntry{e}, l.entries...)
	if len(l.entries) > l.maxLen {
		l.entries = l.entries[:l.maxLen]
	}
}
