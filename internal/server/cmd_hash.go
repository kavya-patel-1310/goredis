package server

import (
	"math"
	"strconv"

	"goredis/internal/ds"
)

func (ctx *Ctx) hashForWrite(key string) (*entry, bool) {
	e, ok := ctx.typed(key, THash, true)
	if !ok {
		return nil, false
	}
	if e == nil {
		e = ctx.s.newEntry(key, THash)
		e.hash = ds.NewHash()
		ctx.s.db.add(e)
	}
	return e, true
}

// HSET key field value [field value ...]  (also HMSET)
func cmdHSet(ctx *Ctx) {
	if ctx.nargs()%2 != 0 {
		ctx.error("ERR wrong number of arguments for '" + ctx.cmd.Name + "' command")
		return
	}
	e, ok := ctx.hashForWrite(ctx.arg(1))
	if !ok {
		return
	}
	added := 0
	for i := 2; i < ctx.nargs(); i += 2 {
		if e.hash.Set(ctx.arg(i), ctx.args[i+1]) {
			added++
		}
	}
	ctx.dirty(ctx.nargs()/2 - 1)
	if ctx.cmd.Name == "hmset" {
		ctx.w.OK()
	} else {
		ctx.w.Int(int64(added))
	}
}

func cmdHSetNX(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), THash, true)
	if !ok {
		return
	}
	if e != nil {
		if _, exists := e.hash.M[ctx.arg(2)]; exists {
			ctx.w.Int(0)
			return
		}
	}
	e, _ = ctx.hashForWrite(ctx.arg(1))
	e.hash.Set(ctx.arg(2), ctx.args[3])
	ctx.dirty(1)
	ctx.w.Int(1)
}

func cmdHGet(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), THash, false)
	if !ok {
		return
	}
	if e != nil {
		if v, ok := e.hash.M[ctx.arg(2)]; ok {
			ctx.w.Bulk(v)
			return
		}
	}
	ctx.w.Null()
}

func cmdHMGet(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), THash, false)
	if !ok {
		return
	}
	ctx.w.Array(ctx.nargs() - 2)
	for i := 2; i < ctx.nargs(); i++ {
		if e != nil {
			if v, ok := e.hash.M[ctx.arg(i)]; ok {
				ctx.w.Bulk(v)
				continue
			}
		}
		ctx.w.Null()
	}
}

func cmdHDel(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), THash, true)
	if !ok {
		return
	}
	n := 0
	if e != nil {
		for i := 2; i < ctx.nargs(); i++ {
			if e.hash.Del(ctx.arg(i)) {
				n++
			}
		}
		if len(e.hash.M) == 0 {
			ctx.s.deleteEntry(e)
		}
	}
	ctx.dirty(n)
	ctx.w.Int(int64(n))
}

func cmdHLen(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), THash, false)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Int(0)
		return
	}
	ctx.w.Int(int64(len(e.hash.M)))
}

func cmdHExists(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), THash, false)
	if !ok {
		return
	}
	if e != nil {
		if _, ok := e.hash.M[ctx.arg(2)]; ok {
			ctx.w.Int(1)
			return
		}
	}
	ctx.w.Int(0)
}

func cmdHStrlen(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), THash, false)
	if !ok {
		return
	}
	if e != nil {
		ctx.w.Int(int64(len(e.hash.M[ctx.arg(2)])))
		return
	}
	ctx.w.Int(0)
}

func hashDump(ctx *Ctx, keys, vals bool) {
	e, ok := ctx.typed(ctx.arg(1), THash, false)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Array(0)
		return
	}
	n := len(e.hash.M)
	if keys && vals {
		n *= 2
	}
	ctx.w.Array(n)
	for k, v := range e.hash.M {
		if keys {
			ctx.w.BulkString(k)
		}
		if vals {
			ctx.w.Bulk(v)
		}
	}
}

func cmdHGetAll(ctx *Ctx) { hashDump(ctx, true, true) }
func cmdHKeys(ctx *Ctx)   { hashDump(ctx, true, false) }
func cmdHVals(ctx *Ctx)   { hashDump(ctx, false, true) }

func cmdHIncrBy(ctx *Ctx) {
	d, ok := ctx.intArg(3)
	if !ok {
		return
	}
	e, ok := ctx.typed(ctx.arg(1), THash, true)
	if !ok {
		return
	}
	var cur int64
	if e != nil {
		if v, exists := e.hash.M[ctx.arg(2)]; exists {
			n, err := strconv.ParseInt(string(v), 10, 64)
			if err != nil {
				ctx.error("ERR hash value is not an integer")
				return
			}
			cur = n
		}
	}
	if (d > 0 && cur > math.MaxInt64-d) || (d < 0 && cur < math.MinInt64-d) {
		ctx.error(errOverflow)
		return
	}
	e, _ = ctx.hashForWrite(ctx.arg(1))
	e.hash.Set(ctx.arg(2), strconv.AppendInt(nil, cur+d, 10))
	ctx.dirty(1)
	ctx.w.Int(cur + d)
}

func cmdHIncrByFloat(ctx *Ctx) {
	d, ok := ctx.floatArg(3)
	if !ok {
		return
	}
	e, ok := ctx.typed(ctx.arg(1), THash, true)
	if !ok {
		return
	}
	cur := 0.0
	if e != nil {
		if v, exists := e.hash.M[ctx.arg(2)]; exists {
			f, ok := parseFloat(string(v))
			if !ok {
				ctx.error("ERR hash value is not a float")
				return
			}
			cur = f
		}
	}
	n := cur + d
	if math.IsInf(n, 0) || math.IsNaN(n) {
		ctx.error("ERR increment would produce NaN or Infinity")
		return
	}
	e, _ = ctx.hashForWrite(ctx.arg(1))
	val := []byte(strconv.FormatFloat(n, 'f', -1, 64))
	e.hash.Set(ctx.arg(2), val)
	ctx.dirty(1)
	ctx.propagateAs([]byte("HSET"), ctx.args[1], ctx.args[2], val)
	ctx.w.Bulk(val)
}
