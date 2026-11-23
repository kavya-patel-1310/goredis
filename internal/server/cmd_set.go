package server

import (
	"math/rand/v2"
	"sort"

	"goredis/internal/ds"
)

func (ctx *Ctx) setForWrite(key string) (*entry, bool) {
	e, ok := ctx.typed(key, TSet, true)
	if !ok {
		return nil, false
	}
	if e == nil {
		e = ctx.s.newEntry(key, TSet)
		e.set = ds.NewSet()
		ctx.s.db.add(e)
	}
	return e, true
}

func cmdSAdd(ctx *Ctx) {
	e, ok := ctx.setForWrite(ctx.arg(1))
	if !ok {
		return
	}
	n := 0
	for i := 2; i < ctx.nargs(); i++ {
		if e.set.Add(ctx.arg(i)) {
			n++
		}
	}
	if e.set.Bytes == 0 && len(e.set.M) == 0 {
		ctx.s.deleteEntry(e)
	}
	ctx.dirty(n)
	ctx.w.Int(int64(n))
}

func cmdSRem(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TSet, true)
	if !ok {
		return
	}
	n := 0
	if e != nil {
		for i := 2; i < ctx.nargs(); i++ {
			if e.set.Remove(ctx.arg(i)) {
				n++
			}
		}
		if len(e.set.M) == 0 {
			ctx.s.deleteEntry(e)
		}
	}
	ctx.dirty(n)
	ctx.w.Int(int64(n))
}

func cmdSIsMember(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TSet, false)
	if !ok {
		return
	}
	if e != nil && e.set.Has(ctx.arg(2)) {
		ctx.w.Int(1)
		return
	}
	ctx.w.Int(0)
}

func cmdSMIsMember(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TSet, false)
	if !ok {
		return
	}
	ctx.w.Array(ctx.nargs() - 2)
	for i := 2; i < ctx.nargs(); i++ {
		if e != nil && e.set.Has(ctx.arg(i)) {
			ctx.w.Int(1)
		} else {
			ctx.w.Int(0)
		}
	}
}

func sortedMembers(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func cmdSMembers(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TSet, false)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Array(0)
		return
	}
	ctx.w.Array(len(e.set.M))
	for k := range e.set.M {
		ctx.w.BulkString(k)
	}
}

func cmdSCard(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TSet, false)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Int(0)
		return
	}
	ctx.w.Int(int64(len(e.set.M)))
}

// randomMembers picks up to n distinct random members.
func randomMembers(set *ds.Set, n int) []string {
	all := make([]string, 0, len(set.M))
	for k := range set.M {
		all = append(all, k)
	}
	rand.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	if n < len(all) {
		all = all[:n]
	}
	return all
}

// SPOP key [count] — propagated as SREM with the chosen members, because
// random choices must not be re-rolled when the AOF is replayed.
func cmdSPop(ctx *Ctx) {
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
	e, ok := ctx.typed(ctx.arg(1), TSet, true)
	if !ok {
		return
	}
	if e == nil {
		if hasCount {
			ctx.w.Array(0)
		} else {
			ctx.w.Null()
		}
		return
	}
	picked := randomMembers(e.set, int(count))
	prop := [][]byte{[]byte("SREM"), ctx.args[1]}
	for _, m := range picked {
		e.set.Remove(m)
		prop = append(prop, []byte(m))
	}
	if hasCount {
		ctx.w.BulkStrings(picked)
	} else if len(picked) > 0 {
		ctx.w.BulkString(picked[0])
	} else {
		ctx.w.Null()
	}
	if len(e.set.M) == 0 {
		ctx.s.deleteEntry(e)
	}
	if len(picked) > 0 {
		ctx.dirty(len(picked))
		ctx.propagateAs(prop...)
	}
}

func cmdSRandMember(ctx *Ctx) {
	e, ok := ctx.typed(ctx.arg(1), TSet, false)
	if !ok {
		return
	}
	if ctx.nargs() == 2 {
		if e == nil {
			ctx.w.Null()
			return
		}
		ctx.w.BulkString(randomMembers(e.set, 1)[0])
		return
	}
	n, ok := ctx.intArg(2)
	if !ok {
		return
	}
	if e == nil {
		ctx.w.Array(0)
		return
	}
	if n >= 0 {
		ctx.w.BulkStrings(randomMembers(e.set, int(n)))
		return
	}
	// Negative count: may repeat.
	all := randomMembers(e.set, len(e.set.M))
	ctx.w.Array(int(-n))
	for i := int64(0); i < -n; i++ {
		ctx.w.BulkString(all[rand.IntN(len(all))])
	}
}

// setOp computes SINTER / SUNION / SDIFF over keys[from:].
func setOp(ctx *Ctx, op string, from int) (map[string]struct{}, bool) {
	sets := make([]*ds.Set, 0, ctx.nargs()-from)
	for i := from; i < ctx.nargs(); i++ {
		e, ok := ctx.typed(ctx.arg(i), TSet, false)
		if !ok {
			return nil, false
		}
		if e == nil {
			sets = append(sets, nil)
		} else {
			sets = append(sets, e.set)
		}
	}
	res := make(map[string]struct{})
	switch op {
	case "inter":
		for _, s := range sets {
			if s == nil {
				return res, true
			}
		}
		sort.Slice(sets, func(i, j int) bool { return len(sets[i].M) < len(sets[j].M) })
	outer:
		for m := range sets[0].M {
			for _, s := range sets[1:] {
				if !s.Has(m) {
					continue outer
				}
			}
			res[m] = struct{}{}
		}
	case "union":
		for _, s := range sets {
			if s != nil {
				for m := range s.M {
					res[m] = struct{}{}
				}
			}
		}
	case "diff":
		if sets[0] == nil {
			return res, true
		}
	diff:
		for m := range sets[0].M {
			for _, s := range sets[1:] {
				if s != nil && s.Has(m) {
					continue diff
				}
			}
			res[m] = struct{}{}
		}
	}
	return res, true
}

func setOpReply(ctx *Ctx, op string) {
	if res, ok := setOp(ctx, op, 1); ok {
		ctx.w.BulkStrings(sortedMembers(res))
	}
}

func setOpStore(ctx *Ctx, op string) {
	res, ok := setOp(ctx, op, 2)
	if !ok {
		return
	}
	s := ctx.s
	if old := s.lookupWrite(ctx.arg(1)); old != nil {
		s.deleteEntry(old)
	}
	if len(res) > 0 {
		e := s.newEntry(ctx.arg(1), TSet)
		e.set = ds.NewSet()
		for m := range res {
			e.set.Add(m)
		}
		s.db.add(e)
	}
	ctx.dirty(1)
	ctx.w.Int(int64(len(res)))
}

func cmdSInter(ctx *Ctx)      { setOpReply(ctx, "inter") }
func cmdSUnion(ctx *Ctx)      { setOpReply(ctx, "union") }
func cmdSDiff(ctx *Ctx)       { setOpReply(ctx, "diff") }
func cmdSInterStore(ctx *Ctx) { setOpStore(ctx, "inter") }
func cmdSUnionStore(ctx *Ctx) { setOpStore(ctx, "union") }
func cmdSDiffStore(ctx *Ctx)  { setOpStore(ctx, "diff") }

func cmdSMove(ctx *Ctx) {
	src, ok := ctx.typed(ctx.arg(1), TSet, true)
	if !ok {
		return
	}
	if _, ok := ctx.typed(ctx.arg(2), TSet, true); !ok {
		return
	}
	m := ctx.arg(3)
	if src == nil || !src.set.Has(m) {
		ctx.w.Int(0)
		return
	}
	src.set.Remove(m)
	if len(src.set.M) == 0 {
		ctx.s.deleteEntry(src)
	}
	dst, _ := ctx.setForWrite(ctx.arg(2))
	dst.set.Add(m)
	ctx.dirty(1)
	ctx.w.Int(1)
}
