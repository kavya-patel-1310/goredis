package server

import (
	"strconv"
	"strings"

	"goredis/internal/glob"
)

func cmdDel(ctx *Ctx) {
	n := 0
	for i := 1; i < ctx.nargs(); i++ {
		if e := ctx.s.lookupWrite(ctx.arg(i)); e != nil {
			ctx.s.deleteEntry(e)
			n++
		}
	}
	ctx.dirty(n)
	ctx.w.Int(int64(n))
}

func cmdExists(ctx *Ctx) {
	n := 0
	for i := 1; i < ctx.nargs(); i++ {
		if ctx.s.lookupRead(ctx.arg(i)) != nil {
			n++
		}
	}
	ctx.w.Int(int64(n))
}

func cmdType(ctx *Ctx) {
	e := ctx.s.lookupRead(ctx.arg(1))
	if e == nil {
		ctx.w.Simple("none")
		return
	}
	ctx.w.Simple(e.typ.String())
}

// expireGeneric implements EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT with the
// NX|XX|GT|LT options. Always propagated as PEXPIREAT (absolute time), so
// replaying the AOF later yields the same deadline.
func expireGeneric(ctx *Ctx, unit int64, absolute bool) {
	s := ctx.s
	n, ok := ctx.intArg(2)
	if !ok {
		return
	}
	var nx, xx, gt, lt bool
	for i := 3; i < ctx.nargs(); i++ {
		switch strings.ToUpper(ctx.arg(i)) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GT":
			gt = true
		case "LT":
			lt = true
		default:
			ctx.error("ERR Unsupported option " + ctx.arg(i))
			return
		}
	}
	if nx && (xx || gt || lt) || (gt && lt) {
		ctx.error("ERR NX and XX, GT or LT options at the same time are not compatible")
		return
	}
	at := n * unit
	if !absolute {
		at += s.now
	}
	e := s.lookupWrite(ctx.arg(1))
	if e == nil {
		ctx.w.Int(0)
		return
	}
	cur := e.expireAt
	if (nx && cur != 0) || (xx && cur == 0) ||
		(gt && (cur == 0 || at <= cur)) || (lt && cur != 0 && at >= cur) {
		ctx.w.Int(0)
		return
	}
	ctx.dirty(1)
	if at <= s.now && !s.loading {
		s.deleteEntry(e)
		ctx.propagateAs([]byte("DEL"), ctx.args[1])
		ctx.w.Int(1)
		return
	}
	s.db.setExpire(e, at)
	ctx.propagateAs([]byte("PEXPIREAT"), ctx.args[1], []byte(strconv.FormatInt(at, 10)))
	ctx.w.Int(1)
}

func cmdExpire(ctx *Ctx)    { expireGeneric(ctx, 1000, false) }
func cmdPExpire(ctx *Ctx)   { expireGeneric(ctx, 1, false) }
func cmdExpireAt(ctx *Ctx)  { expireGeneric(ctx, 1000, true) }
func cmdPExpireAt(ctx *Ctx) { expireGeneric(ctx, 1, true) }

func ttlGeneric(ctx *Ctx, ms bool) {
	e := ctx.s.lookupRead(ctx.arg(1))
	switch {
	case e == nil:
		ctx.w.Int(-2)
	case e.expireAt == 0:
		ctx.w.Int(-1)
	default:
		left := e.expireAt - ctx.s.now
		if !ms {
			left = (left + 500) / 1000
		}
		ctx.w.Int(left)
	}
}

func cmdTTL(ctx *Ctx)  { ttlGeneric(ctx, false) }
func cmdPTTL(ctx *Ctx) { ttlGeneric(ctx, true) }

func cmdPersist(ctx *Ctx) {
	e := ctx.s.lookupWrite(ctx.arg(1))
	if e == nil || e.expireAt == 0 {
		ctx.w.Int(0)
		return
	}
	ctx.s.db.setExpire(e, 0)
	ctx.dirty(1)
	ctx.w.Int(1)
}

func cmdKeys(ctx *Ctx) {
	pat := ctx.arg(1)
	all := pat == "*"
	var out []string
	for _, e := range ctx.s.db.keys {
		if ctx.s.isExpired(e) {
			continue
		}
		if all || glob.Match(pat, e.key) {
			out = append(out, e.key)
		}
	}
	ctx.w.BulkStrings(out)
}

// SCAN cursor [MATCH pattern] [COUNT n] [TYPE type]
func cmdScan(ctx *Ctx) {
	cursor, err := strconv.ParseUint(ctx.arg(1), 10, 64)
	if err != nil {
		ctx.error("ERR invalid cursor")
		return
	}
	pat, typ, count := "", "", 10
	for i := 2; i < ctx.nargs(); i++ {
		opt := strings.ToUpper(ctx.arg(i))
		if i+1 >= ctx.nargs() {
			ctx.syntax()
			return
		}
		i++
		switch opt {
		case "MATCH":
			pat = ctx.arg(i)
		case "COUNT":
			n, ok := ctx.intArg(i)
			if !ok {
				return
			}
			if n < 1 {
				ctx.syntax()
				return
			}
			count = int(n)
		case "TYPE":
			typ = strings.ToLower(ctx.arg(i))
		default:
			ctx.syntax()
			return
		}
	}
	var keys []string
	next := ctx.s.db.scan(cursor, count, func(e *entry) {
		if ctx.s.isExpired(e) {
			return
		}
		if pat != "" && pat != "*" && !glob.Match(pat, e.key) {
			return
		}
		if typ != "" && e.typ.String() != typ {
			return
		}
		keys = append(keys, e.key)
	})
	ctx.w.Array(2)
	ctx.w.BulkString(strconv.FormatUint(next, 10))
	ctx.w.BulkStrings(keys)
}

func cmdRandomKey(ctx *Ctx) {
	for i := 0; i < 100; i++ {
		e := ctx.s.db.randomEntry()
		if e == nil {
			break
		}
		if !ctx.s.isExpired(e) {
			ctx.w.BulkString(e.key)
			return
		}
	}
	ctx.w.Null()
}

func renameGeneric(ctx *Ctx, nx bool) {
	s := ctx.s
	src := s.lookupWrite(ctx.arg(1))
	if src == nil {
		ctx.error(errNoKey)
		return
	}
	dst := ctx.arg(2)
	if dst == src.key {
		if nx {
			ctx.w.Int(0)
		} else {
			ctx.w.OK()
		}
		return
	}
	if nx && s.lookupWrite(dst) != nil {
		ctx.w.Int(0)
		return
	}
	s.deleteEntry(src)
	ne := *src
	ne.key = dst
	ne.eidx = -1
	s.setKey(&ne)
	ctx.dirty(1)
	if nx {
		ctx.w.Int(1)
	} else {
		ctx.w.OK()
	}
}

func cmdRename(ctx *Ctx)   { renameGeneric(ctx, false) }
func cmdRenameNX(ctx *Ctx) { renameGeneric(ctx, true) }

func cmdDBSize(ctx *Ctx) { ctx.w.Int(int64(ctx.s.db.Len())) }

func cmdFlushAll(ctx *Ctx) {
	s := ctx.s
	n := s.db.Len()
	s.db = newDB()
	s.touchAllWatched()
	ctx.dirty(max(n, 1))
	ctx.w.OK()
}

func cmdMemoryUsage(ctx *Ctx) {
	switch strings.ToUpper(ctx.arg(1)) {
	case "USAGE":
		if ctx.nargs() < 3 {
			ctx.syntax()
			return
		}
		e := ctx.s.lookupRead(ctx.arg(2))
		if e == nil {
			ctx.w.Null()
			return
		}
		ctx.w.Int(e.size())
	case "STATS":
		ctx.w.Array(4)
		ctx.w.BulkString("used_memory")
		ctx.w.Int(ctx.s.db.used)
		ctx.w.BulkString("keys.count")
		ctx.w.Int(int64(ctx.s.db.Len()))
	default:
		ctx.error("ERR unknown subcommand '" + ctx.arg(1) + "'. Try MEMORY USAGE|STATS.")
	}
}

func cmdObject(ctx *Ctx) {
	if ctx.nargs() < 3 {
		ctx.error("ERR wrong number of arguments for 'object' command")
		return
	}
	e := ctx.s.db.get(ctx.arg(2))
	if e == nil || ctx.s.isExpired(e) {
		ctx.w.Null()
		return
	}
	switch strings.ToUpper(ctx.arg(1)) {
	case "ENCODING":
		enc := map[ValueType]string{TString: "raw", TList: "ringbuffer", THash: "hashtable", TSet: "hashtable", TZSet: "skiplist"}[e.typ]
		if e.typ == TString {
			if _, err := strconv.ParseInt(string(e.str), 10, 64); err == nil {
				enc = "int"
			}
		}
		ctx.w.BulkString(enc)
	case "IDLETIME":
		ctx.w.Int((ctx.s.now - e.lru) / 1000)
	default:
		ctx.error("ERR unknown subcommand '" + ctx.arg(1) + "'. Try OBJECT ENCODING|IDLETIME.")
	}
}
