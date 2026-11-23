package server

import (
	"math"
	"strconv"
	"strings"
)

func cmdGet(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TString, false)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Null()
		return
	}
	ctx.w.Bulk(e.str)
}

// setGeneric stores a string, optionally with an absolute expiry or keeping
// the existing TTL.
func (s *Server) setString(key string, val []byte, expireAt int64, keepTTL bool) {
	old := s.lookupWrite(key)
	e := s.newEntry(key, TString)
	e.str = val
	if keepTTL && old != nil {
		e.expireAt = old.expireAt
	} else {
		e.expireAt = expireAt
	}
	s.setKey(e)
}

// SET key value [NX|XX] [GET] [EX s|PX ms|EXAT ts|PXAT ms-ts|KEEPTTL]
func cmdSet(ctx *Ctx) {
	s := ctx.s
	key, val := ctx.arg(1), ctx.args[2]
	var nx, xx, get, keepTTL bool
	var expireAt int64
	expSet := false
	for i := 3; i < ctx.nargs(); i++ {
		opt := strings.ToUpper(ctx.arg(i))
		switch opt {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GET":
			get = true
		case "KEEPTTL":
			keepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if i+1 >= ctx.nargs() || expSet {
				ctx.syntax()
				return
			}
			i++
			n, ok := ctx.intArg(i)
			if !ok {
				return
			}
			if n <= 0 {
				ctx.error("ERR invalid expire time in 'set' command")
				return
			}
			switch opt {
			case "EX":
				expireAt = s.now + n*1000
			case "PX":
				expireAt = s.now + n
			case "EXAT":
				expireAt = n * 1000
			case "PXAT":
				expireAt = n
			}
			expSet = true
		default:
			ctx.syntax()
			return
		}
	}
	if (nx && xx) || (keepTTL && expSet) {
		ctx.syntax()
		return
	}
	old := s.lookupWrite(key)
	if get {
		if old != nil && old.typ != TString {
			ctx.wrongType()
			return
		}
	}
	if (nx && old != nil) || (xx && old == nil) {
		if get {
			replyOldString(ctx, old)
		} else {
			ctx.w.Null()
		}
		return
	}
	s.setString(key, val, expireAt, keepTTL)
	ctx.dirty(1)
	p := [][]byte{[]byte("SET"), ctx.args[1], val}
	if expSet {
		p = append(p, []byte("PXAT"), []byte(strconv.FormatInt(expireAt, 10)))
	} else if keepTTL {
		p = append(p, []byte("KEEPTTL"))
	}
	ctx.propagateAs(p...)
	if get {
		replyOldString(ctx, old)
	} else {
		ctx.w.OK()
	}
}

func replyOldString(ctx *Ctx, old *entry) {
	if old == nil {
		ctx.w.Null()
	} else {
		ctx.w.Bulk(old.str)
	}
}

func cmdSetNX(ctx *Ctx) {
	if ctx.s.lookupWrite(ctx.arg(1)) != nil {
		ctx.w.Int(0)
		return
	}
	ctx.s.setString(ctx.arg(1), ctx.args[2], 0, false)
	ctx.dirty(1)
	ctx.w.Int(1)
}

// SETEX key seconds value / PSETEX key ms value
func setexGeneric(ctx *Ctx, unit int64) {
	n, ok := ctx.intArg(2)
	if !ok {
		return
	}
	if n <= 0 {
		ctx.error("ERR invalid expire time in '" + ctx.cmd.Name + "' command")
		return
	}
	at := ctx.s.now + n*unit
	ctx.s.setString(ctx.arg(1), ctx.args[3], at, false)
	ctx.dirty(1)
	ctx.propagateAs([]byte("SET"), ctx.args[1], ctx.args[3], []byte("PXAT"), []byte(strconv.FormatInt(at, 10)))
	ctx.w.OK()
}

func cmdSetEX(ctx *Ctx)  { setexGeneric(ctx, 1000) }
func cmdPSetEX(ctx *Ctx) { setexGeneric(ctx, 1) }

func cmdGetSet(ctx *Ctx) {
	old, ok := ctx.typed(ctx.arg(1), TString, true)
	if !ok {
		return
	}
	ctx.s.setString(ctx.arg(1), ctx.args[2], 0, false)
	ctx.dirty(1)
	replyOldString(ctx, old)
}

func cmdGetDel(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TString, true)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Null()
		return
	}
	ctx.w.Bulk(e.str)
	ctx.s.deleteEntry(e)
	ctx.dirty(1)
	ctx.propagateAs([]byte("DEL"), ctx.args[1])
}

func cmdMGet(ctx *Ctx) {
	ctx.w.Array(ctx.nargs() - 1)
	for i := 1; i < ctx.nargs(); i++ {
		e := ctx.s.lookupRead(ctx.arg(i))
		if e == nil || e.typ != TString {
			ctx.w.Null()
		} else {
			ctx.w.Bulk(e.str)
		}
	}
}

func cmdMSet(ctx *Ctx) {
	if ctx.nargs()%2 != 1 {
		ctx.error("ERR wrong number of arguments for 'mset' command")
		return
	}
	for i := 1; i < ctx.nargs(); i += 2 {
		ctx.s.setString(ctx.arg(i), ctx.args[i+1], 0, false)
	}
	ctx.dirty(ctx.nargs() / 2)
	ctx.w.OK()
}

func cmdMSetNX(ctx *Ctx) {
	if ctx.nargs()%2 != 1 {
		ctx.error("ERR wrong number of arguments for 'msetnx' command")
		return
	}
	for i := 1; i < ctx.nargs(); i += 2 {
		if ctx.s.lookupWrite(ctx.arg(i)) != nil {
			ctx.w.Int(0)
			return
		}
	}
	for i := 1; i < ctx.nargs(); i += 2 {
		ctx.s.setString(ctx.arg(i), ctx.args[i+1], 0, false)
	}
	ctx.dirty(ctx.nargs() / 2)
	ctx.w.Int(1)
}

func incrBy(ctx *Ctx, delta int64) {
	s := ctx.s
	e, ok := ctx.typed(ctx.arg(1), TString, true)
	if !ok {
		return
	}
	var cur int64
	if e != nil {
		v, err := strconv.ParseInt(string(e.str), 10, 64)
		if err != nil {
			ctx.error(errNotInt)
			return
		}
		cur = v
	}
	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		ctx.error(errOverflow)
		return
	}
	n := cur + delta
	val := strconv.AppendInt(nil, n, 10)
	if e == nil {
		e = s.newEntry(ctx.arg(1), TString)
		e.str = val
		s.db.add(e)
	} else {
		e.str = val
	}
	ctx.dirty(1)
	ctx.w.Int(n)
}

func cmdIncr(ctx *Ctx) { incrBy(ctx, 1) }
func cmdDecr(ctx *Ctx) { incrBy(ctx, -1) }
func cmdIncrBy(ctx *Ctx) {
	if d, ok := ctx.intArg(2); ok {
		incrBy(ctx, d)
	}
}
func cmdDecrBy(ctx *Ctx) {
	if d, ok := ctx.intArg(2); ok {
		if d == math.MinInt64 {
			ctx.error(errOverflow)
			return
		}
		incrBy(ctx, -d)
	}
}

func cmdIncrByFloat(ctx *Ctx) {
	s := ctx.s
	d, ok := ctx.floatArg(2)
	if !ok {
		return
	}
	e, ok := ctx.typed(ctx.arg(1), TString, true)
	if !ok {
		return
	}
	cur := 0.0
	if e != nil {
		v, ok := parseFloat(string(e.str))
		if !ok {
			ctx.error(errNotFloat)
			return
		}
		cur = v
	}
	n := cur + d
	if math.IsInf(n, 0) || math.IsNaN(n) {
		ctx.error("ERR increment would produce NaN or Infinity")
		return
	}
	val := []byte(strconv.FormatFloat(n, 'f', -1, 64))
	if e == nil {
		e = s.newEntry(ctx.arg(1), TString)
		e.str = val
		s.db.add(e)
	} else {
		e.str = val
	}
	ctx.dirty(1)
	// Propagate the result, not the increment, so replay is exact.
	ctx.propagateAs([]byte("SET"), ctx.args[1], val, []byte("KEEPTTL"))
	ctx.w.Bulk(val)
}

func cmdAppend(ctx *Ctx) {
	s := ctx.s
	e, ok := ctx.typed(ctx.arg(1), TString, true)
	if !ok {
		return
	}
	if e == nil {
		e = s.newEntry(ctx.arg(1), TString)
		e.str = append([]byte(nil), ctx.args[2]...)
		s.db.add(e)
	} else {
		e.str = append(e.str, ctx.args[2]...)
	}
	ctx.dirty(1)
	ctx.w.Int(int64(len(e.str)))
}

func cmdStrlen(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TString, false)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Int(0)
		return
	}
	ctx.w.Int(int64(len(e.str)))
}

func cmdGetRange(ctx *Ctx) {
	start, ok := ctx.intArg(2)
	if !ok {
		return
	}
	end, ok := ctx.intArg(3)
	if !ok {
		return
	}
	e, ok := ctx.typed(ctx.arg(1), TString, false)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.BulkString("")
		return
	}
	n := int64(len(e.str))
	if start < 0 {
		start = max(n+start, 0)
	}
	if end < 0 {
		end = n + end
	}
	end = min(end, n-1)
	if start > end || n == 0 {
		ctx.w.BulkString("")
		return
	}
	ctx.w.Bulk(e.str[start : end+1])
}

func cmdSetRange(ctx *Ctx) {
	s := ctx.s
	off, ok := ctx.intArg(2)
	if !ok {
		return
	}
	if off < 0 || off > 512*1024*1024 {
		ctx.error("ERR offset is out of range")
		return
	}
	e, ok := ctx.typed(ctx.arg(1), TString, true)
	if !ok {
		return
	}
	val := ctx.args[3]
	var cur []byte
	if e != nil {
		cur = e.str
	}
	if len(val) == 0 {
		ctx.w.Int(int64(len(cur)))
		return
	}
	// Copy-on-write: never mutate bytes that a background save may share.
	nb := make([]byte, max(len(cur), int(off)+len(val)))
	copy(nb, cur)
	copy(nb[off:], val)
	if e == nil {
		e = s.newEntry(ctx.arg(1), TString)
		e.str = nb
		s.db.add(e)
	} else {
		e.str = nb
	}
	ctx.dirty(1)
	ctx.w.Int(int64(len(nb)))
}
