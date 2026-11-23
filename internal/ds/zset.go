package ds

// ZSet combines a hash map (O(1) ZSCORE) with a skiplist (ordered ranges,
// O(log n) rank) — the same dual-structure design Redis uses.
type ZSet struct {
	dict  map[string]float64
	sl    *SkipList
	Bytes int64
}

const zsetNodeOverhead = 48

func NewZSet() *ZSet { return &ZSet{dict: make(map[string]float64), sl: NewSkipList()} }

func (z *ZSet) Len() int { return len(z.dict) }

func (z *ZSet) Score(member string) (float64, bool) {
	s, ok := z.dict[member]
	return s, ok
}

// Add inserts or updates member. Returns true if the member is new.
func (z *ZSet) Add(member string, score float64) bool {
	if old, ok := z.dict[member]; ok {
		if old != score {
			z.sl.Delete(old, member)
			z.sl.Insert(score, member)
			z.dict[member] = score
		}
		return false
	}
	z.dict[member] = score
	z.sl.Insert(score, member)
	z.Bytes += int64(len(member)) + zsetNodeOverhead
	return true
}

func (z *ZSet) Remove(member string) bool {
	s, ok := z.dict[member]
	if !ok {
		return false
	}
	delete(z.dict, member)
	z.sl.Delete(s, member)
	z.Bytes -= int64(len(member)) + zsetNodeOverhead
	return true
}

// Rank returns the 0-based rank (ascending, or descending if rev).
func (z *ZSet) Rank(member string, rev bool) (int, bool) {
	s, ok := z.dict[member]
	if !ok {
		return 0, false
	}
	r := z.sl.Rank(s, member)
	if rev {
		return z.sl.Len() - r, true
	}
	return r - 1, true
}

// RangeByRank calls fn for elements with 0-based ranks [start, stop]
// (already normalised and clamped by the caller).
func (z *ZSet) RangeByRank(start, stop int, rev bool, fn func(member string, score float64)) {
	if start > stop || start >= z.Len() {
		return
	}
	n := stop - start + 1
	var x *ZNode
	if rev {
		x = z.sl.ByRank(z.sl.Len() - start)
	} else {
		x = z.sl.ByRank(start + 1)
	}
	for ; x != nil && n > 0; n-- {
		fn(x.Member, x.Score)
		if rev {
			x = x.Prev()
		} else {
			x = x.Next()
		}
	}
}

// RangeByScore iterates elements inside r, skipping offset and returning at
// most count (count < 0 means unlimited).
func (z *ZSet) RangeByScore(r ScoreRange, rev bool, offset, count int, fn func(member string, score float64)) {
	var x *ZNode
	if rev {
		x = z.sl.LastInRange(r)
	} else {
		x = z.sl.FirstInRange(r)
	}
	for x != nil && offset > 0 {
		offset--
		if rev {
			x = x.Prev()
		} else {
			x = x.Next()
		}
	}
	for x != nil && count != 0 {
		if rev && !r.GteMin(x.Score) || !rev && !r.LteMax(x.Score) {
			break
		}
		fn(x.Member, x.Score)
		count--
		if rev {
			x = x.Prev()
		} else {
			x = x.Next()
		}
	}
}

// Count returns the number of elements inside r in O(log n).
func (z *ZSet) Count(r ScoreRange) int {
	first := z.sl.FirstInRange(r)
	if first == nil {
		return 0
	}
	last := z.sl.LastInRange(r)
	return z.sl.Rank(last.Score, last.Member) - z.sl.Rank(first.Score, first.Member) + 1
}

// Each iterates in ascending order.
func (z *ZSet) Each(fn func(member string, score float64)) {
	for x := z.sl.First(); x != nil; x = x.Next() {
		fn(x.Member, x.Score)
	}
}

func (z *ZSet) Clone() *ZSet {
	nz := NewZSet()
	z.Each(func(m string, s float64) { nz.Add(m, s) })
	return nz
}
