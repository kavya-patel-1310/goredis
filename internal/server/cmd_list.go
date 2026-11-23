package server

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"goredis/internal/ds"
)

func pushGeneric(ctx *Ctx, left, onlyIfExists bool) {
	s := ctx.s
	key := ctx.arg(1)
	e, ok := ctx.typed(key, TList, true)
	if !ok {
		return
	}
	if e == nil {
		if onlyIfExists {
			ctx.w.Int(0)
			return
		}
		e = s.newEntry(key, TList)
		e.list = ds.NewDeque()
		s.db.add(e)
	}
	for i := 2; i < ctx.nargs(); i++ {
		if left {
			e.list.PushFront(ctx.args[i])
		} else {
			e.list.PushBack(ctx.args[i])
		}
	}
	ctx.dirty(ctx.nargs() - 2)
	s.signalKeyReady(key)
	ctx.w.Int(int64(e.list.Len()))
}

func cmdLPush(ctx *Ctx)  { pushGeneric(ctx, true, false) }
func cmdRPush(ctx *Ctx)  { pushGeneric(ctx, false, false) }
func cmdLPushX(ctx *Ctx) { pushGeneric(ctx, true, true) }
func cmdRPushX(ctx *Ctx) { pushGeneric(ctx, false, true) }

func popGeneric(ctx *Ctx, left bool) {
	s := ctx.s
	count, hasCount := int64(1), ctx.nargs() == 3
	if hasCount {
		var ok bool
		if count, ok = ctx.intArg(2); !ok {
			return
		}
		if count < 0 {
			ctx.error("ERR value is out of range, must be positive")
			return
		}
	}
	e, ok := ctx.typed(ctx.arg(1), TList, true)
	if !ok {
		return
	}
	if e == nil {
		if hasCount {
			ctx.w.NullArray()
		} else {
			ctx.w.Null()
		}
		return
	}
	n := min(int(count), e.list.Len())
	if hasCount {
		ctx.w.Array(n)
	}
	for i := 0; i < n; i++ {
		var v []byte
		if left {
			v, _ = e.list.PopFront()
		} else {
			v, _ = e.list.PopBack()
		}
		ctx.w.Bulk(v)
	}
	if n > 0 {
		ctx.dirty(n)
	}
	if e.list.Len() == 0 {
		s.deleteEntry(e)
	}
}

func cmdLPop(ctx *Ctx) { popGeneric(ctx, true) }
func cmdRPop(ctx *Ctx) { popGeneric(ctx, false) }

// BLPOP/BRPOP key [key ...] timeout
func blockingPop(ctx *Ctx, left bool) {
	s := ctx.s
	t, ok := parseFloat(ctx.arg(ctx.nargs() - 1))
	if !ok || t < 0 {
		ctx.error("ERR timeout is not a float or out of range")
		return
	}
	keys := make([]string, 0, ctx.nargs()-2)
	for i := 1; i < ctx.nargs()-1; i++ {
		key := ctx.arg(i)
		e, ok := ctx.typed(key, TList, true)
		if !ok {
			return
		}
		if e != nil {
			var v []byte
			cmd := "LPOP"
			if left {
				v, _ = e.list.PopFront()
			} else {
				v, _ = e.list.PopBack()
				cmd = "RPOP"
			}
			ctx.w.Array(2)
			ctx.w.BulkString(key)
			ctx.w.Bulk(v)
			if e.list.Len() == 0 {
				s.deleteEntry(e)
			}
			ctx.dirty(1)
			ctx.propagateAs([]byte(cmd), ctx.args[i])
			return
		}
		keys = append(keys, key)
	}
	// Nothing to pop: inside MULTI or from internal clients we never block.
	if ctx.c.inExec || ctx.c.noBlock || s.loading {
		ctx.w.NullArray()
		return
	}
	s.block(ctx.c, keys, left, time.Duration(t*float64(time.Second)))
}

func cmdBLPop(ctx *Ctx) { blockingPop(ctx, true) }
func cmdBRPop(ctx *Ctx) { blockingPop(ctx, false) }

func cmdLLen(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TList, false)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Int(0)
		return
	}
	ctx.w.Int(int64(e.list.Len()))
}

// normRange converts Redis inclusive start/stop (negative = from end) into
// clamped indexes; ok=false means empty.
func normRange(start, stop int64, n int) (int, int, bool) {
	ln := int64(n)
	if start < 0 {
		start += ln
	}
	if stop < 0 {
		stop += ln
	}
	if start < 0 {
		start = 0
	}
	if start > stop || start >= ln {
		return 0, 0, false
	}
	if stop >= ln {
		stop = ln - 1
	}
	return int(start), int(stop), true
}

func cmdLRange(ctx *Ctx) {
	start, ok := ctx.intArg(2)
	if !ok {
		return
	}
	stop, ok := ctx.intArg(3)
	if !ok {
		return
	}
	e, ok := ctx.typed(ctx.arg(1), TList, false)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Array(0)
		return
	}
	a, b, ok := normRange(start, stop, e.list.Len())
	if !ok {
		ctx.w.Array(0)
		return
	}
	ctx.w.Array(b - a + 1)
	for i := a; i <= b; i++ {
		ctx.w.Bulk(e.list.At(i))
	}
}

func cmdLIndex(ctx *Ctx) {
	idx, ok := ctx.intArg(2)
	if !ok {
		return
	}
	e, ok := ctx.typed(ctx.arg(1), TList, false)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Null()
		return
	}
	n := int64(e.list.Len())
	if idx < 0 {
		idx += n
	}
	if idx < 0 || idx >= n {
		ctx.w.Null()
		return
	}
	ctx.w.Bulk(e.list.At(int(idx)))
}

func cmdLSet(ctx *Ctx) {
	idx, ok := ctx.intArg(2)
	if !ok {
		return
	}
	e, ok := ctx.typed(ctx.arg(1), TList, true)
	if !ok {
		return
	}
	if e == nil {
		ctx.error(errNoKey)
		return
	}
	n := int64(e.list.Len())
	if idx < 0 {
		idx += n
	}
	if idx < 0 || idx >= n {
		ctx.error(errIndexRange)
		return
	}
	e.list.Set(int(idx), ctx.args[3])
	ctx.dirty(1)
	ctx.w.OK()
}

// LREM key count element
func cmdLRem(ctx *Ctx) {
	count, ok := ctx.intArg(2)
	if !ok {
		return
	}
	e, ok := ctx.typed(ctx.arg(1), TList, true)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Int(0)
		return
	}
	target := ctx.args[3]
	n := e.list.Len()
	remove := make(map[int]bool)
	if count >= 0 {
		for i := 0; i < n && (count == 0 || int64(len(remove)) < count); i++ {
			if bytes.Equal(e.list.At(i), target) {
				remove[i] = true
			}
		}
	} else {
		for i := n - 1; i >= 0 && int64(len(remove)) < -count; i-- {
			if bytes.Equal(e.list.At(i), target) {
				remove[i] = true
			}
		}
	}
	if len(remove) > 0 {
		e.list.Filter(func(i int, _ []byte) bool { return !remove[i] })
		ctx.dirty(len(remove))
		if e.list.Len() == 0 {
			ctx.s.deleteEntry(e)
		}
	}
	ctx.w.Int(int64(len(remove)))
}

func cmdLTrim(ctx *Ctx) {
	start, ok := ctx.intArg(2)
	if !ok {
		return
	}
	stop, ok := ctx.intArg(3)
	if !ok {
		return
	}
	e, ok := ctx.typed(ctx.arg(1), TList, true)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.OK()
		return
	}
	a, b, ok := normRange(start, stop, e.list.Len())
	if !ok {
		ctx.s.deleteEntry(e)
	} else {
		e.list.Filter(func(i int, _ []byte) bool { return i >= a && i <= b })
	}
	ctx.dirty(1)
	ctx.w.OK()
}

// LINSERT key BEFORE|AFTER pivot element
func cmdLInsert(ctx *Ctx) {
	where := strings.ToUpper(ctx.arg(2))
	if where != "BEFORE" && where != "AFTER" {
		ctx.syntax()
		return
	}
	e, ok := ctx.typed(ctx.arg(1), TList, true)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Int(0)
		return
	}
	pivot, val := ctx.args[3], ctx.args[4]
	pos := -1
	for i := 0; i < e.list.Len(); i++ {
		if bytes.Equal(e.list.At(i), pivot) {
			pos = i
			break
		}
	}
	if pos < 0 {
		ctx.w.Int(-1)
		return
	}
	if where == "AFTER" {
		pos++
	}
	nd := ds.NewDeque()
	for i := 0; i < e.list.Len(); i++ {
		if i == pos {
			nd.PushBack(val)
		}
		nd.PushBack(e.list.At(i))
	}
	if pos == e.list.Len() {
		nd.PushBack(val)
	}
	e.list = nd
	ctx.dirty(1)
	ctx.w.Int(int64(nd.Len()))
}

// LMOVE source destination LEFT|RIGHT LEFT|RIGHT  (and RPOPLPUSH)
func lmoveGeneric(ctx *Ctx, srcLeft, dstLeft bool) {
	s := ctx.s
	src, ok := ctx.typed(ctx.arg(1), TList, true)
	if !ok {
		return
	}
	if src == nil {
		ctx.w.Null()
		return
	}
	dst, ok := ctx.typed(ctx.arg(2), TList, true)
	if !ok {
		return
	}
	var v []byte
	if srcLeft {
		v, _ = src.list.PopFront()
	} else {
		v, _ = src.list.PopBack()
	}
	if dst == nil {
		if src.list.Len() == 0 && src.key == ctx.arg(2) {
			dst = src
		} else {
			dst = s.newEntry(ctx.arg(2), TList)
			dst.list = ds.NewDeque()
			s.db.add(dst)
		}
	}
	if dstLeft {
		dst.list.PushFront(v)
	} else {
		dst.list.PushBack(v)
	}
	if src.list.Len() == 0 {
		s.deleteEntry(src)
	}
	ctx.dirty(1)
	s.signalKeyReady(ctx.arg(2))
	ctx.w.Bulk(v)
}

func cmdRPopLPush(ctx *Ctx) { lmoveGeneric(ctx, false, true) }

func cmdLMove(ctx *Ctx) {
	side := func(s string) (bool, bool) {
		switch strings.ToUpper(s) {
		case "LEFT":
			return true, true
		case "RIGHT":
			return false, true
		}
		return false, false
	}
	a, ok1 := side(ctx.arg(3))
	b, ok2 := side(ctx.arg(4))
	if !ok1 || !ok2 {
		ctx.syntax()
		return
	}
	lmoveGeneric(ctx, a, b)
}

// LPOS key element  (basic form)
func cmdLPos(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TList, false)
	if !ok {
		return
	}
	if e != nil {
		for i := 0; i < e.list.Len(); i++ {
			if bytes.Equal(e.list.At(i), ctx.args[2]) {
				ctx.w.Int(int64(i))
				return
			}
		}
	}
	ctx.w.Null()
}

var _ = strconv.Itoa
