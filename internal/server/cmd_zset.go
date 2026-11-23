package server

import (
	"math"
	"strings"

	"goredis/internal/ds"
	"goredis/internal/resp"
)

// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
func cmdZAdd(ctx *Ctx) {
	s := ctx.s
	var nx, xx, gt, lt, ch, incr bool
	i := 2
loop:
	for ; i < ctx.nargs(); i++ {
		switch strings.ToUpper(ctx.arg(i)) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GT":
			gt = true
		case "LT":
			lt = true
		case "CH":
			ch = true
		case "INCR":
			incr = true
		default:
			break loop
		}
	}
	pairs := ctx.nargs() - i
	if pairs == 0 || pairs%2 != 0 {
		ctx.syntax()
		return
	}
	if nx && xx {
		ctx.error("ERR XX and NX options at the same time are not compatible")
		return
	}
	if (gt && lt) || (nx && (gt || lt)) {
		ctx.error("ERR GT, LT, and/or NX options at the same time are not compatible")
		return
	}
	if incr && pairs != 2 {
		ctx.error("ERR INCR option supports a single increment-element pair")
		return
	}
	scores := make([]float64, 0, pairs/2)
	for j := i; j < ctx.nargs(); j += 2 {
		f, ok := parseFloat(ctx.arg(j))
		if !ok {
			ctx.error(errNotFloat)
			return
		}
		scores = append(scores, f)
	}
	e, ok := ctx.typed(ctx.arg(1), TZSet, true)
	if !ok {
		return
	}
	if e == nil {
		if xx {
			if incr {
				ctx.w.Null()
			} else {
				ctx.w.Int(0)
			}
			return
		}
		e = s.newEntry(ctx.arg(1), TZSet)
		e.zset = ds.NewZSet()
		s.db.add(e)
	}
	added, changed := 0, 0
	var incrResult float64
	incrApplied := false
	for k, sc := range scores {
		m := ctx.arg(i + 2*k + 1)
		cur, exists := e.zset.Score(m)
		if (nx && exists) || (xx && !exists) {
			continue
		}
		if incr && exists {
			sc += cur
			if math.IsNaN(sc) {
				ctx.error("ERR resulting score is not a number (NaN)")
				return
			}
		}
		if exists && ((gt && sc <= cur) || (lt && sc >= cur)) {
			continue
		}
		if e.zset.Add(m, sc) {
			added++
		} else if cur != sc {
			changed++
		}
		incrResult, incrApplied = sc, true
	}
	if e.zset.Len() == 0 {
		s.deleteEntry(e)
	}
	ctx.dirty(added + changed)
	switch {
	case incr && incrApplied:
		ctx.w.BulkFloat(incrResult)
	case incr:
		ctx.w.Null()
	case ch:
		ctx.w.Int(int64(added + changed))
	default:
		ctx.w.Int(int64(added))
	}
}

func cmdZIncrBy(ctx *Ctx) {
	d, ok := ctx.floatArg(2)
	if !ok {
		return
	}
	e, ok := ctx.typed(ctx.arg(1), TZSet, true)
	if !ok {
		return
	}
	if e == nil {
		e = ctx.s.newEntry(ctx.arg(1), TZSet)
		e.zset = ds.NewZSet()
		ctx.s.db.add(e)
	}
	cur, _ := e.zset.Score(ctx.arg(3))
	n := cur + d
	if math.IsNaN(n) {
		ctx.error("ERR resulting score is not a number (NaN)")
		return
	}
	e.zset.Add(ctx.arg(3), n)
	ctx.dirty(1)
	ctx.w.BulkFloat(n)
}

func cmdZRem(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TZSet, true)
	if !ok {
		return
	}
	n := 0
	if e != nil {
		for i := 2; i < ctx.nargs(); i++ {
			if e.zset.Remove(ctx.arg(i)) {
				n++
			}
		}
		if e.zset.Len() == 0 {
			ctx.s.deleteEntry(e)
		}
	}
	ctx.dirty(n)
	ctx.w.Int(int64(n))
}

func cmdZScore(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TZSet, false)
	if !ok {
		return
	}
	if e != nil {
		if sc, ok := e.zset.Score(ctx.arg(2)); ok {
			ctx.w.BulkFloat(sc)
			return
		}
	}
	ctx.w.Null()
}

func cmdZMScore(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TZSet, false)
	if !ok {
		return
	}
	ctx.w.Array(ctx.nargs() - 2)
	for i := 2; i < ctx.nargs(); i++ {
		if e != nil {
			if sc, ok := e.zset.Score(ctx.arg(i)); ok {
				ctx.w.BulkFloat(sc)
				continue
			}
		}
		ctx.w.Null()
	}
}

func cmdZCard(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TZSet, false)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Int(0)
		return
	}
	ctx.w.Int(int64(e.zset.Len()))
}

func zrankGeneric(ctx *Ctx, rev bool) {
	e, ok := ctx.typed(ctx.arg(1), TZSet, false)
	if !ok {
		return
	}
	if e != nil {
		if r, ok := e.zset.Rank(ctx.arg(2), rev); ok {
			ctx.w.Int(int64(r))
			return
		}
	}
	ctx.w.Null()
}

func cmdZRank(ctx *Ctx)    { zrankGeneric(ctx, false) }
func cmdZRevRank(ctx *Ctx) { zrankGeneric(ctx, true) }

// parseScoreRange parses "1.5", "(1.5", "-inf", "+inf".
func parseScoreRange(minS, maxS string) (ds.ScoreRange, bool) {
	var r ds.ScoreRange
	var ok bool
	if strings.HasPrefix(minS, "(") {
		r.MinEx = true
		minS = minS[1:]
	}
	if strings.HasPrefix(maxS, "(") {
		r.MaxEx = true
		maxS = maxS[1:]
	}
	if r.Min, ok = parseFloat(minS); !ok {
		return r, false
	}
	if r.Max, ok = parseFloat(maxS); !ok {
		return r, false
	}
	return r, true
}

func writeZEntry(w *resp.Writer, m string, sc float64, withScores bool) {
	w.BulkString(m)
	if withScores {
		w.BulkFloat(sc)
	}
}

// zrangeGeneric serves ZRANGE (with BYSCORE/REV/LIMIT/WITHSCORES),
// ZREVRANGE, ZRANGEBYSCORE and ZREVRANGEBYSCORE.
func zrangeGeneric(ctx *Ctx, byScore, rev bool, argStart int) {
	withScores := false
	offset, count := 0, -1
	for i := argStart; i < ctx.nargs(); i++ {
		switch strings.ToUpper(ctx.arg(i)) {
		case "WITHSCORES":
			withScores = true
		case "BYSCORE":
			byScore = true
		case "REV":
			rev = true
		case "LIMIT":
			if i+2 >= ctx.nargs() {
				ctx.syntax()
				return
			}
			o, ok := ctx.intArg(i + 1)
			if !ok {
				return
			}
			c, ok := ctx.intArg(i + 2)
			if !ok {
				return
			}
			offset, count = int(o), int(c)
			i += 2
		default:
			ctx.syntax()
			return
		}
	}
	e, ok := ctx.typed(ctx.arg(1), TZSet, false)
	if !ok {
		return
	}
	var out []struct {
		m string
		s float64
	}
	collect := func(m string, s float64) {
		out = append(out, struct {
			m string
			s float64
		}{m, s})
	}
	if byScore {
		a, b := ctx.arg(2), ctx.arg(3)
		if rev {
			a, b = b, a
		}
		r, ok := parseScoreRange(a, b)
		if !ok {
			ctx.error("ERR min or max is not a float")
			return
		}
		if e != nil && offset >= 0 {
			e.zset.RangeByScore(r, rev, offset, count, collect)
		}
	} else {
		start, ok := ctx.intArg(2)
		if !ok {
			return
		}
		stop, ok := ctx.intArg(3)
		if !ok {
			return
		}
		if e != nil {
			if a, b, ok := normRange(start, stop, e.zset.Len()); ok {
				e.zset.RangeByRank(a, b, rev, collect)
			}
		}
	}
	n := len(out)
	if withScores {
		n *= 2
	}
	ctx.w.Array(n)
	for _, p := range out {
		writeZEntry(ctx.w, p.m, p.s, withScores)
	}
}

func cmdZRange(ctx *Ctx)           { zrangeGeneric(ctx, false, false, 4) }
func cmdZRevRange(ctx *Ctx)        { zrangeGeneric(ctx, false, true, 4) }
func cmdZRangeByScore(ctx *Ctx)    { zrangeGeneric(ctx, true, false, 4) }
func cmdZRevRangeByScore(ctx *Ctx) { zrangeGeneric(ctx, true, true, 4) } // key max min

func cmdZCount(ctx *Ctx) {
	r, ok := parseScoreRange(ctx.arg(2), ctx.arg(3))
	if !ok {
		ctx.error("ERR min or max is not a float")
		return
	}
	e, ok := ctx.typed(ctx.arg(1), TZSet, false)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Int(0)
		return
	}
	ctx.w.Int(int64(e.zset.Count(r)))
}

func zpopGeneric(ctx *Ctx, max bool) {
	count := int64(1)
	if ctx.nargs() == 3 {
		var ok bool
		if count, ok = ctx.intArg(2); !ok {
			return
		}
	}
	e, ok := ctx.typed(ctx.arg(1), TZSet, true)
	if !ok {
		return
	}
	if e == nil || count <= 0 {
		ctx.w.Array(0)
		return
	}
	n := min(int(count), e.zset.Len())
	var picked []string
	var scores []float64
	e.zset.RangeByRank(0, n-1, max, func(m string, s float64) {
		picked = append(picked, m)
		scores = append(scores, s)
	})
	ctx.w.Array(2 * n)
	prop := [][]byte{[]byte("ZREM"), ctx.args[1]}
	for i, m := range picked {
		e.zset.Remove(m)
		writeZEntry(ctx.w, m, scores[i], true)
		prop = append(prop, []byte(m))
	}
	if e.zset.Len() == 0 {
		ctx.s.deleteEntry(e)
	}
	ctx.dirty(n)
	ctx.propagateAs(prop...)
}

func cmdZPopMin(ctx *Ctx) { zpopGeneric(ctx, false) }
func cmdZPopMax(ctx *Ctx) { zpopGeneric(ctx, true) }

func cmdZRemRangeByRank(ctx *Ctx) {
	start, ok := ctx.intArg(2)
	if !ok {
		return
	}
	stop, ok := ctx.intArg(3)
	if !ok {
		return
	}
	e, ok := ctx.typed(ctx.arg(1), TZSet, true)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Int(0)
		return
	}
	var victims []string
	if a, b, ok := normRange(start, stop, e.zset.Len()); ok {
		e.zset.RangeByRank(a, b, false, func(m string, _ float64) { victims = append(victims, m) })
	}
	removeZMembers(ctx, e, victims)
}

func cmdZRemRangeByScore(ctx *Ctx) {
	r, ok := parseScoreRange(ctx.arg(2), ctx.arg(3))
	if !ok {
		ctx.error("ERR min or max is not a float")
		return
	}
	e, ok := ctx.typed(ctx.arg(1), TZSet, true)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Int(0)
		return
	}
	var victims []string
	e.zset.RangeByScore(r, false, 0, -1, func(m string, _ float64) { victims = append(victims, m) })
	removeZMembers(ctx, e, victims)
}

func removeZMembers(ctx *Ctx, e *entry, victims []string) {
	for _, m := range victims {
		e.zset.Remove(m)
	}
	if e.zset.Len() == 0 {
		ctx.s.deleteEntry(e)
	}
	ctx.dirty(len(victims))
	ctx.w.Int(int64(len(victims)))
}
