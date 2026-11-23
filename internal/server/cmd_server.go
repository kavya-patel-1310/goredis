package server

import (
	"crypto/subtle"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"goredis/internal/glob"
)

const Version = "1.0.0"

func cmdPing(ctx *Ctx) {
	if ctx.c.subCount() > 0 {
		ctx.w.Array(2)
		ctx.w.BulkString("pong")
		if ctx.nargs() > 1 {
			ctx.w.Bulk(ctx.args[1])
		} else {
			ctx.w.BulkString("")
		}
		return
	}
	if ctx.nargs() > 1 {
		ctx.w.Bulk(ctx.args[1])
		return
	}
	ctx.w.Simple("PONG")
}

func cmdEcho(ctx *Ctx) { ctx.w.Bulk(ctx.args[1]) }

func cmdQuit(ctx *Ctx) {
	ctx.w.OK()
	ctx.c.quit.Store(true)
}

func cmdAuth(ctx *Ctx) {
	pass := ctx.arg(ctx.nargs() - 1) // AUTH [username] password
	want := ctx.s.cfg.RequirePass
	if want == "" {
		ctx.error("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
		return
	}
	if subtle.ConstantTimeCompare([]byte(pass), []byte(want)) != 1 {
		ctx.error("WRONGPASS invalid username-password pair or user is disabled.")
		return
	}
	ctx.c.authed = true
	ctx.w.OK()
}

func cmdSelect(ctx *Ctx) {
	if ctx.arg(1) != "0" {
		ctx.error("ERR DB index is out of range (GoRedis supports a single database)")
		return
	}
	ctx.w.OK()
}

func cmdHello(ctx *Ctx) {
	if ctx.nargs() > 1 && ctx.arg(1) != "2" {
		ctx.error("NOPROTO unsupported protocol version")
		return
	}
	for i := 2; i+1 < ctx.nargs(); i++ {
		if strings.EqualFold(ctx.arg(i), "AUTH") && i+2 < ctx.nargs() {
			if ctx.s.cfg.RequirePass != "" && ctx.arg(i+2) != ctx.s.cfg.RequirePass {
				ctx.error("WRONGPASS invalid username-password pair or user is disabled.")
				return
			}
			ctx.c.authed = true
			i += 2
		} else if strings.EqualFold(ctx.arg(i), "SETNAME") {
			ctx.c.name = ctx.arg(i + 1)
			i++
		}
	}
	if ctx.s.cfg.RequirePass != "" && !ctx.c.authed {
		ctx.error("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time")
		return
	}
	ctx.w.Array(14)
	for _, kv := range [][2]string{{"server", "goredis"}, {"version", Version}, {"mode", "standalone"}, {"role", "master"}} {
		ctx.w.BulkString(kv[0])
		ctx.w.BulkString(kv[1])
	}
	ctx.w.BulkString("proto")
	ctx.w.Int(2)
	ctx.w.BulkString("id")
	ctx.w.Int(ctx.c.id)
	ctx.w.BulkString("modules")
	ctx.w.Array(0)
}

func cmdReset(ctx *Ctx) {
	c := ctx.c
	resetMulti(c)
	ctx.s.unwatchAll(c)
	ctx.s.pubsub.unsubscribeAll(c)
	delete(ctx.s.monitors, c)
	c.monitor = false
	c.name = ""
	if ctx.s.cfg.RequirePass != "" {
		c.authed = false
	}
	ctx.w.Simple("RESET")
}

func cmdClient(ctx *Ctx) {
	s, c := ctx.s, ctx.c
	switch strings.ToUpper(ctx.arg(1)) {
	case "ID":
		ctx.w.Int(c.id)
	case "SETNAME":
		if ctx.nargs() != 3 || strings.ContainsAny(ctx.arg(2), " \n") {
			ctx.error("ERR Client names cannot contain spaces, newlines or special characters.")
			return
		}
		c.name = ctx.arg(2)
		ctx.w.OK()
	case "GETNAME":
		if c.name == "" {
			ctx.w.Null()
		} else {
			ctx.w.BulkString(c.name)
		}
	case "SETINFO":
		ctx.w.OK()
	case "INFO":
		ctx.w.BulkString(c.info(time.Now()) + "\n")
	case "LIST":
		var sb strings.Builder
		ids := make([]int64, 0, len(s.clients))
		for id := range s.clients {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		now := time.Now()
		for _, id := range ids {
			sb.WriteString(s.clients[id].info(now))
			sb.WriteByte('\n')
		}
		ctx.w.BulkString(sb.String())
	case "KILL":
		if ctx.nargs() == 4 && strings.EqualFold(ctx.arg(2), "ID") {
			id, err := strconv.ParseInt(ctx.arg(3), 10, 64)
			if cl, ok := s.clients[id]; err == nil && ok {
				cl.close()
				ctx.w.Int(1)
				return
			}
			ctx.w.Int(0)
			return
		}
		for _, cl := range s.clients {
			if cl.addr == ctx.arg(2) {
				cl.close()
				ctx.w.OK()
				return
			}
		}
		ctx.error("ERR No such client")
	default:
		ctx.error("ERR unknown subcommand '" + ctx.arg(1) + "'. Try CLIENT ID|SETNAME|GETNAME|LIST|KILL|INFO.")
	}
}

func cmdTime(ctx *Ctx) {
	now := time.Now()
	ctx.w.Array(2)
	ctx.w.BulkInt(now.Unix())
	ctx.w.BulkInt(int64(now.Nanosecond() / 1000))
}

func cmdMonitor(ctx *Ctx) {
	if ctx.c.internal {
		ctx.error("ERR MONITOR is not supported from this client; use the Monitor tab")
		return
	}
	ctx.c.monitor = true
	ctx.s.monitors[ctx.c] = struct{}{}
	ctx.w.OK()
}

func cmdSlowlog(ctx *Ctx) {
	l := &ctx.s.slowlog
	switch strings.ToUpper(ctx.arg(1)) {
	case "GET":
		n := 10
		if ctx.nargs() > 2 {
			v, ok := ctx.intArg(2)
			if !ok {
				return
			}
			n = int(v)
		}
		if n < 0 || n > len(l.entries) {
			n = len(l.entries)
		}
		ctx.w.Array(n)
		for _, e := range l.entries[:n] {
			ctx.w.Array(6)
			ctx.w.Int(e.ID)
			ctx.w.Int(e.Time)
			ctx.w.Int(e.Duration)
			ctx.w.BulkStrings(e.Args)
			ctx.w.BulkString(e.Client)
			ctx.w.BulkString(e.Name)
		}
	case "LEN":
		ctx.w.Int(int64(len(l.entries)))
	case "RESET":
		l.entries = nil
		ctx.w.OK()
	default:
		ctx.error("ERR unknown subcommand '" + ctx.arg(1) + "'. Try SLOWLOG GET|LEN|RESET.")
	}
}

func cmdSave(ctx *Ctx) {
	if err := ctx.s.saveSnapshotLocked(); err != nil {
		ctx.error("ERR " + err.Error())
		return
	}
	ctx.w.OK()
}

func cmdBgSave(ctx *Ctx) {
	if err := ctx.s.bgSave(); err != nil {
		ctx.error("ERR " + err.Error())
		return
	}
	ctx.w.Simple("Background saving started")
}

func cmdBgRewriteAOF(ctx *Ctx) {
	if err := ctx.s.bgRewriteAOF(); err != nil {
		ctx.error("ERR " + err.Error())
		return
	}
	ctx.w.Simple("Background append only file rewriting started")
}

func cmdLastSave(ctx *Ctx) { ctx.w.Int(ctx.s.lastSave.Unix()) }

func cmdShutdown(ctx *Ctx) {
	s := ctx.s
	save := !s.cfg.AppendOnly && s.cfg.SaveSeconds > 0
	if ctx.nargs() > 1 {
		switch strings.ToUpper(ctx.arg(1)) {
		case "NOSAVE":
			save = false
		case "SAVE":
			save = true
		default:
			ctx.syntax()
			return
		}
	}
	s.logf("User requested shutdown...")
	// Shutdown must run without the executor lock held.
	go func() { s.shutdown(save) }()
}

// DEBUG SLEEP <seconds> | DEBUG POPULATE <count> [prefix] [size]
func cmdDebug(ctx *Ctx) {
	s := ctx.s
	switch strings.ToUpper(ctx.arg(1)) {
	case "SLEEP":
		if ctx.nargs() < 3 {
			ctx.syntax()
			return
		}
		f, ok := ctx.floatArg(2)
		if !ok {
			return
		}
		time.Sleep(time.Duration(f * float64(time.Second)))
		ctx.w.OK()
	case "POPULATE":
		if ctx.nargs() < 3 {
			ctx.syntax()
			return
		}
		n, ok := ctx.intArg(2)
		if !ok {
			return
		}
		prefix := "key"
		if ctx.nargs() > 3 {
			prefix = ctx.arg(3)
		}
		size := int64(0)
		if ctx.nargs() > 4 {
			if size, ok = ctx.intArg(4); !ok {
				return
			}
		}
		created := 0
		for i := int64(0); i < n; i++ {
			k := prefix + ":" + strconv.FormatInt(i, 10)
			if s.lookupWrite(k) != nil {
				continue
			}
			v := []byte("value:" + strconv.FormatInt(i, 10))
			if size > 0 {
				b := make([]byte, size)
				copy(b, v)
				v = b
			}
			e := s.newEntry(k, TString)
			e.str = v
			s.db.add(e)
			s.propagate([][]byte{[]byte("SET"), []byte(k), v})
			created++
		}
		ctx.dirty(created)
		ctx.w.OK()
	default:
		ctx.error("ERR DEBUG subcommand must be SLEEP or POPULATE")
	}
}

// ---------------------------------------------------------------------------
// CONFIG

type configParam struct {
	get func(s *Server) string
	set func(s *Server, v string) error
}

var configParams = map[string]configParam{
	"maxmemory": {
		get: func(s *Server) string { return strconv.FormatInt(s.cfg.MaxMemory, 10) },
		set: func(s *Server, v string) error {
			n, err := ParseMemory(v)
			if err == nil {
				s.cfg.MaxMemory = n
				s.evictIfNeeded()
			}
			return err
		},
	},
	"maxmemory-policy": {
		get: func(s *Server) string { return s.cfg.MaxMemoryPolicy },
		set: func(s *Server, v string) error {
			v = strings.ToLower(v)
			if !ValidPolicy(v) {
				return fmt.Errorf("invalid maxmemory-policy")
			}
			s.cfg.MaxMemoryPolicy = v
			return nil
		},
	},
	"maxmemory-samples": {
		get: func(s *Server) string { return strconv.Itoa(s.cfg.MaxMemorySamples) },
		set: func(s *Server, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > 64 {
				return fmt.Errorf("argument must be between 1 and 64")
			}
			s.cfg.MaxMemorySamples = n
			return nil
		},
	},
	"appendfsync": {
		get: func(s *Server) string { return s.cfg.AppendFsync },
		set: func(s *Server, v string) error {
			v = strings.ToLower(v)
			if v != "always" && v != "everysec" && v != "no" {
				return fmt.Errorf("argument must be 'always', 'everysec' or 'no'")
			}
			s.cfg.AppendFsync = v
			if s.aof != nil {
				s.aof.setPolicy(v)
			}
			return nil
		},
	},
	"appendonly": {
		get: func(s *Server) string { return yesno(s.aof != nil) },
	},
	"save": {
		get: func(s *Server) string {
			if s.cfg.SaveSeconds <= 0 {
				return ""
			}
			return fmt.Sprintf("%d %d", s.cfg.SaveSeconds, s.cfg.SaveChanges)
		},
		set: func(s *Server, v string) error {
			if strings.TrimSpace(v) == "" {
				s.cfg.SaveSeconds = 0
				return nil
			}
			var sec int
			var ch int64
			if _, err := fmt.Sscanf(v, "%d %d", &sec, &ch); err != nil {
				return fmt.Errorf("invalid save parameters")
			}
			s.cfg.SaveSeconds, s.cfg.SaveChanges = sec, ch
			return nil
		},
	},
	"requirepass": {
		get: func(s *Server) string { return s.cfg.RequirePass },
		set: func(s *Server, v string) error { s.cfg.RequirePass = v; return nil },
	},
	"slowlog-log-slower-than": {
		get: func(s *Server) string { return strconv.FormatInt(s.cfg.SlowlogSlowerUs, 10) },
		set: func(s *Server, v string) error {
			n, err := strconv.ParseInt(v, 10, 64)
			if err == nil {
				s.cfg.SlowlogSlowerUs = n
			}
			return err
		},
	},
	"slowlog-max-len": {
		get: func(s *Server) string { return strconv.Itoa(s.slowlog.maxLen) },
		set: func(s *Server, v string) error {
			n, err := strconv.Atoi(v)
			if err == nil {
				s.slowlog.maxLen = n
				if len(s.slowlog.entries) > n {
					s.slowlog.entries = s.slowlog.entries[:n]
				}
			}
			return err
		},
	},
	"maxclients": {
		get: func(s *Server) string { return strconv.Itoa(s.cfg.MaxClients) },
	},
	"dir": {
		get: func(s *Server) string { return s.cfg.Dir },
	},
	"databases": {
		get: func(s *Server) string { return "1" },
	},
}

func yesno(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// ValidPolicy reports whether p is a supported maxmemory policy.
func ValidPolicy(p string) bool {
	switch p {
	case "noeviction", "allkeys-lru", "volatile-lru", "allkeys-random", "volatile-random", "volatile-ttl":
		return true
	}
	return false
}

// ParseMemory parses "100mb", "1gb", "512k", "1048576".
func ParseMemory(v string) (int64, error) {
	v = strings.ToLower(strings.TrimSpace(v))
	mult := int64(1)
	for _, u := range []struct {
		suf string
		m   int64
	}{{"gb", 1 << 30}, {"mb", 1 << 20}, {"kb", 1 << 10}, {"g", 1e9}, {"m", 1e6}, {"k", 1e3}, {"b", 1}} {
		if strings.HasSuffix(v, u.suf) {
			mult = u.m
			v = strings.TrimSuffix(v, u.suf)
			break
		}
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("argument must be a memory value")
	}
	return n * mult, nil
}

func cmdConfig(ctx *Ctx) {
	s := ctx.s
	switch strings.ToUpper(ctx.arg(1)) {
	case "GET":
		if ctx.nargs() < 3 {
			ctx.syntax()
			return
		}
		var names []string
		for name := range configParams {
			for i := 2; i < ctx.nargs(); i++ {
				if glob.Match(strings.ToLower(ctx.arg(i)), name) {
					names = append(names, name)
					break
				}
			}
		}
		sort.Strings(names)
		ctx.w.Array(2 * len(names))
		for _, n := range names {
			ctx.w.BulkString(n)
			ctx.w.BulkString(configParams[n].get(s))
		}
	case "SET":
		if ctx.nargs() < 4 || ctx.nargs()%2 != 0 {
			ctx.error("ERR wrong number of arguments for 'config|set' command")
			return
		}
		for i := 2; i < ctx.nargs(); i += 2 {
			name := strings.ToLower(ctx.arg(i))
			p, ok := configParams[name]
			if !ok || p.set == nil {
				ctx.error("ERR Unknown option or number of arguments for CONFIG SET - '" + name + "'")
				return
			}
			if err := p.set(s, ctx.arg(i+1)); err != nil {
				ctx.error("ERR CONFIG SET failed (possibly related to argument '" + name + "') - " + err.Error())
				return
			}
		}
		ctx.w.OK()
	case "RESETSTAT":
		s.stats.hits.Store(0)
		s.stats.misses.Store(0)
		s.stats.expired.Store(0)
		s.stats.evicted.Store(0)
		for _, c := range s.commands {
			c.calls.Store(0)
			c.usec.Store(0)
			c.fails.Store(0)
		}
		ctx.w.OK()
	default:
		ctx.error("ERR unknown subcommand '" + ctx.arg(1) + "'. Try CONFIG GET|SET|RESETSTAT.")
	}
}

// ---------------------------------------------------------------------------
// COMMAND

func cmdCommand(ctx *Ctx) {
	s := ctx.s
	sub := ""
	if ctx.nargs() > 1 {
		sub = strings.ToUpper(ctx.arg(1))
	}
	switch sub {
	case "COUNT":
		ctx.w.Int(int64(len(s.commands)))
	case "DOCS":
		ctx.w.Array(0)
	case "", "INFO":
		var cmds []*Command
		if sub == "INFO" {
			for i := 2; i < ctx.nargs(); i++ {
				cmds = append(cmds, s.lookupCommand(ctx.args[i]))
			}
		} else {
			for _, c := range s.commands {
				cmds = append(cmds, c)
			}
			sort.Slice(cmds, func(i, j int) bool { return cmds[i].Name < cmds[j].Name })
		}
		ctx.w.Array(len(cmds))
		for _, c := range cmds {
			if c == nil {
				ctx.w.NullArray()
				continue
			}
			ctx.w.Array(6)
			ctx.w.BulkString(c.Name)
			ctx.w.Int(int64(c.Arity))
			var fl []string
			for _, f := range flagNames {
				if c.Flags&f.f != 0 {
					fl = append(fl, f.name)
				}
			}
			ctx.w.Array(len(fl))
			for _, f := range fl {
				ctx.w.Simple(f)
			}
			ctx.w.Int(int64(c.FirstKey))
			ctx.w.Int(int64(c.LastKey))
			ctx.w.Int(int64(c.Step))
		}
	default:
		ctx.error("ERR unknown subcommand '" + ctx.arg(1) + "'. Try COMMAND COUNT|DOCS|INFO.")
	}
}

// ---------------------------------------------------------------------------
// INFO

func cmdInfo(ctx *Ctx) {
	section := "default"
	if ctx.nargs() > 1 {
		section = strings.ToLower(ctx.arg(1))
	}
	ctx.w.BulkString(ctx.s.infoString(section))
}

func (s *Server) infoString(section string) string {
	var sb strings.Builder
	want := func(name string) bool {
		return section == "default" || section == "all" || section == "everything" || section == name
	}
	kv := func(k string, v any) { fmt.Fprintf(&sb, "%s:%v\r\n", k, v) }
	if want("server") {
		sb.WriteString("# Server\r\n")
		kv("redis_version", "7.2.0")
		kv("goredis_version", Version)
		kv("redis_mode", "standalone")
		kv("os", runtime.GOOS+" "+runtime.GOARCH)
		kv("go_version", runtime.Version())
		kv("process_id", os.Getpid())
		kv("tcp_port", portOf(s.Addr()))
		kv("uptime_in_seconds", int64(time.Since(s.startTime).Seconds()))
		kv("uptime_in_days", int64(time.Since(s.startTime).Hours()/24))
		kv("executable_threads", runtime.GOMAXPROCS(0))
		sb.WriteString("\r\n")
	}
	if want("clients") {
		sb.WriteString("# Clients\r\n")
		blocked := 0
		for _, c := range s.clients {
			if c.blocked != nil {
				blocked++
			}
		}
		kv("connected_clients", s.numClients.Load())
		kv("blocked_clients", blocked)
		kv("maxclients", s.cfg.MaxClients)
		kv("watching_keys", len(s.watched))
		sb.WriteString("\r\n")
	}
	if want("memory") {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		sb.WriteString("# Memory\r\n")
		kv("used_memory", s.db.used)
		kv("used_memory_human", HumanBytes(s.db.used))
		kv("used_memory_go_heap", ms.HeapAlloc)
		kv("used_memory_go_sys", ms.Sys)
		kv("maxmemory", s.cfg.MaxMemory)
		kv("maxmemory_human", HumanBytes(s.cfg.MaxMemory))
		kv("maxmemory_policy", s.cfg.MaxMemoryPolicy)
		kv("go_gc_cycles", ms.NumGC)
		sb.WriteString("\r\n")
	}
	if want("persistence") {
		sb.WriteString("# Persistence\r\n")
		kv("loading", b2i(s.loading))
		kv("rdb_changes_since_last_save", s.dirty)
		kv("rdb_bgsave_in_progress", b2i(s.bgSaveRunning))
		kv("rdb_last_save_time", s.lastSave.Unix())
		kv("rdb_last_bgsave_status", okErr(s.lastSaveOK))
		kv("rdb_last_bgsave_time_sec", int64(s.lastBgSaveDur.Seconds()))
		kv("aof_enabled", b2i(s.aof != nil))
		kv("aof_rewrite_in_progress", b2i(s.rewriting))
		if s.aof != nil {
			st := s.aof.stats()
			kv("aof_current_size", st.size)
			kv("aof_last_write_status", okErr(st.lastErr == nil))
			kv("aof_fsync_policy", st.policy)
			kv("aof_pending_bytes", st.pending)
		}
		sb.WriteString("\r\n")
	}
	if want("stats") {
		sb.WriteString("# Stats\r\n")
		kv("total_connections_received", s.stats.totalConnections.Load())
		kv("total_commands_processed", s.stats.totalCommands.Load())
		kv("instantaneous_ops_per_sec", s.stats.opsSec.Load())
		kv("total_net_input_bytes", s.stats.netInput.Load())
		kv("total_net_output_bytes", s.stats.netOutput.Load())
		kv("rejected_connections", s.stats.rejectedConns.Load())
		kv("expired_keys", s.stats.expired.Load())
		kv("evicted_keys", s.stats.evicted.Load())
		kv("keyspace_hits", s.stats.hits.Load())
		kv("keyspace_misses", s.stats.misses.Load())
		kv("pubsub_channels", len(s.pubsub.channels))
		kv("pubsub_patterns", len(s.pubsub.patterns))
		kv("pubsub_messages", s.stats.pubsubMessages.Load())
		sb.WriteString("\r\n")
	}
	if want("replication") {
		sb.WriteString("# Replication\r\nrole:master\r\nconnected_slaves:0\r\n\r\n")
	}
	if section == "all" || section == "everything" || section == "commandstats" {
		sb.WriteString("# Commandstats\r\n")
		names := make([]string, 0, len(s.commands))
		for n, c := range s.commands {
			if c.calls.Load() > 0 {
				names = append(names, n)
			}
		}
		sort.Strings(names)
		for _, n := range names {
			c := s.commands[n]
			calls, us := c.calls.Load(), c.usec.Load()
			fmt.Fprintf(&sb, "cmdstat_%s:calls=%d,usec=%d,usec_per_call=%.2f,failed_calls=%d\r\n",
				n, calls, us, float64(us)/float64(calls), c.fails.Load())
		}
		sb.WriteString("\r\n")
	}
	if want("keyspace") {
		sb.WriteString("# Keyspace\r\n")
		if s.db.Len() > 0 {
			fmt.Fprintf(&sb, "db0:keys=%d,expires=%d,avg_ttl=0\r\n", s.db.Len(), len(s.db.volatile))
		}
	}
	return sb.String()
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func okErr(ok bool) string {
	if ok {
		return "ok"
	}
	return "err"
}

func portOf(a interface{ String() string }) string {
	if a == nil {
		return ""
	}
	s := a.String()
	if i := strings.LastIndexByte(s, ':'); i >= 0 {
		return s[i+1:]
	}
	return s
}

// HumanBytes formats bytes like Redis' used_memory_human.
func HumanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2fG", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2fM", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2fK", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%dB", n)
}
