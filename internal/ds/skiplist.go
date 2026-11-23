package ds

import "math/rand/v2"

// This is a port of the Redis sorted-set skiplist (t_zset.c). Every forward
// link stores a "span" (number of nodes it skips) so that rank queries
// (ZRANK, ZRANGE by index) run in O(log n) instead of O(n).

const (
	maxLevel = 32
	levelP   = 0.25
)

type zLevel struct {
	forward *ZNode
	span    int
}

// ZNode is a skiplist node.
type ZNode struct {
	Member   string
	Score    float64
	backward *ZNode
	level    []zLevel
}

// Next returns the following node (ascending order) or nil.
func (n *ZNode) Next() *ZNode { return n.level[0].forward }

// Prev returns the previous node or nil.
func (n *ZNode) Prev() *ZNode { return n.backward }

type SkipList struct {
	header *ZNode
	tail   *ZNode
	length int
	level  int
}

func NewSkipList() *SkipList {
	return &SkipList{header: &ZNode{level: make([]zLevel, maxLevel)}, level: 1}
}

func randomLevel() int {
	l := 1
	for l < maxLevel && rand.Float64() < levelP {
		l++
	}
	return l
}

func less(n *ZNode, score float64, member string) bool {
	return n.Score < score || (n.Score == score && n.Member < member)
}

func (sl *SkipList) Len() int { return sl.length }

// Insert adds a node. The caller guarantees member is not already present.
func (sl *SkipList) Insert(score float64, member string) *ZNode {
	var update [maxLevel]*ZNode
	var rank [maxLevel]int
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		if i != sl.level-1 {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil && less(x.level[i].forward, score, member) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}
	lvl := randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			rank[i] = 0
			update[i] = sl.header
			update[i].level[i].span = sl.length
		}
		sl.level = lvl
	}
	x = &ZNode{Member: member, Score: score, level: make([]zLevel, lvl)}
	for i := 0; i < lvl; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = rank[0] - rank[i] + 1
	}
	for i := lvl; i < sl.level; i++ {
		update[i].level[i].span++
	}
	if update[0] != sl.header {
		x.backward = update[0]
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		sl.tail = x
	}
	sl.length++
	return x
}

func (sl *SkipList) deleteNode(x *ZNode, update *[maxLevel]*ZNode) {
	for i := 0; i < sl.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		sl.tail = x.backward
	}
	for sl.level > 1 && sl.header.level[sl.level-1].forward == nil {
		sl.level--
	}
	sl.length--
}

// Delete removes the node with the given score and member.
func (sl *SkipList) Delete(score float64, member string) bool {
	var update [maxLevel]*ZNode
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && less(x.level[i].forward, score, member) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	if x != nil && x.Score == score && x.Member == member {
		sl.deleteNode(x, &update)
		return true
	}
	return false
}

// Rank returns the 1-based rank of the element, or 0 if absent.
func (sl *SkipList) Rank(score float64, member string) int {
	rank := 0
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.Score < score ||
				(x.level[i].forward.Score == score && x.level[i].forward.Member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}
		if x != sl.header && x.Member == member && x.Score == score {
			return rank
		}
	}
	return 0
}

// ByRank returns the node at 1-based rank, or nil.
func (sl *SkipList) ByRank(rank int) *ZNode {
	traversed := 0
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && traversed+x.level[i].span <= rank {
			traversed += x.level[i].span
			x = x.level[i].forward
		}
		if traversed == rank {
			return x
		}
	}
	return nil
}

// First returns the lowest node; Last the highest.
func (sl *SkipList) First() *ZNode { return sl.header.level[0].forward }
func (sl *SkipList) Last() *ZNode  { return sl.tail }

// ScoreRange is a score interval; Ex flags make a bound exclusive.
type ScoreRange struct {
	Min, Max     float64
	MinEx, MaxEx bool
}

func (r ScoreRange) GteMin(v float64) bool {
	if r.MinEx {
		return v > r.Min
	}
	return v >= r.Min
}

func (r ScoreRange) LteMax(v float64) bool {
	if r.MaxEx {
		return v < r.Max
	}
	return v <= r.Max
}

func (sl *SkipList) isInRange(r ScoreRange) bool {
	if r.Min > r.Max || (r.Min == r.Max && (r.MinEx || r.MaxEx)) {
		return false
	}
	if sl.tail == nil || !r.GteMin(sl.tail.Score) {
		return false
	}
	f := sl.header.level[0].forward
	return f != nil && r.LteMax(f.Score)
}

// FirstInRange returns the first node with score inside r.
func (sl *SkipList) FirstInRange(r ScoreRange) *ZNode {
	if !sl.isInRange(r) {
		return nil
	}
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && !r.GteMin(x.level[i].forward.Score) {
			x = x.level[i].forward
		}
	}
	x = x.level[0].forward
	if x == nil || !r.LteMax(x.Score) {
		return nil
	}
	return x
}

// LastInRange returns the last node with score inside r.
func (sl *SkipList) LastInRange(r ScoreRange) *ZNode {
	if !sl.isInRange(r) {
		return nil
	}
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && r.LteMax(x.level[i].forward.Score) {
			x = x.level[i].forward
		}
	}
	if x == sl.header || !r.GteMin(x.Score) {
		return nil
	}
	return x
}
