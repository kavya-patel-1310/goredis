package ds

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"
)

func TestDeque(t *testing.T) {
	d := NewDeque()
	var ref []string
	r := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 20000; i++ {
		v := fmt.Sprint(i)
		switch r.IntN(4) {
		case 0:
			d.PushBack([]byte(v))
			ref = append(ref, v)
		case 1:
			d.PushFront([]byte(v))
			ref = append([]string{v}, ref...)
		case 2:
			got, ok := d.PopFront()
			if ok != (len(ref) > 0) {
				t.Fatal("PopFront ok mismatch")
			}
			if ok {
				if string(got) != ref[0] {
					t.Fatalf("PopFront %s want %s", got, ref[0])
				}
				ref = ref[1:]
			}
		case 3:
			got, ok := d.PopBack()
			if ok != (len(ref) > 0) {
				t.Fatal("PopBack ok mismatch")
			}
			if ok {
				if string(got) != ref[len(ref)-1] {
					t.Fatalf("PopBack %s want %s", got, ref[len(ref)-1])
				}
				ref = ref[:len(ref)-1]
			}
		}
		if d.Len() != len(ref) {
			t.Fatalf("len %d want %d", d.Len(), len(ref))
		}
	}
	for i, v := range ref {
		if string(d.At(i)) != v {
			t.Fatalf("At(%d)=%s want %s", i, d.At(i), v)
		}
	}
}

type pair struct {
	m string
	s float64
}

func sortedRef(m map[string]float64) []pair {
	var ps []pair
	for k, v := range m {
		ps = append(ps, pair{k, v})
	}
	sort.Slice(ps, func(i, j int) bool {
		if ps[i].s != ps[j].s {
			return ps[i].s < ps[j].s
		}
		return ps[i].m < ps[j].m
	})
	return ps
}

// TestZSetAgainstReference is a randomized property test: after every batch
// of random adds/updates/removes the skiplist must agree with a sorted slice
// on order, ranks, rank lookups and score-range counts.
func TestZSetAgainstReference(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 9))
	z := NewZSet()
	ref := map[string]float64{}
	for round := 0; round < 200; round++ {
		for i := 0; i < 50; i++ {
			m := fmt.Sprintf("m%d", r.IntN(300))
			if r.IntN(3) == 0 {
				z.Remove(m)
				delete(ref, m)
			} else {
				s := float64(r.IntN(100))
				z.Add(m, s)
				ref[m] = s
			}
		}
		exp := sortedRef(ref)
		if z.Len() != len(exp) || z.sl.Len() != len(exp) {
			t.Fatalf("len mismatch %d %d %d", z.Len(), z.sl.Len(), len(exp))
		}
		i := 0
		z.Each(func(m string, s float64) {
			if exp[i].m != m || exp[i].s != s {
				t.Fatalf("order mismatch at %d: %s/%v want %s/%v", i, m, s, exp[i].m, exp[i].s)
			}
			if rk, _ := z.Rank(m, false); rk != i {
				t.Fatalf("rank(%s)=%d want %d", m, rk, i)
			}
			if rk, _ := z.Rank(m, true); rk != len(exp)-1-i {
				t.Fatalf("revrank(%s)=%d want %d", m, rk, len(exp)-1-i)
			}
			if n := z.sl.ByRank(i + 1); n == nil || n.Member != m {
				t.Fatalf("ByRank(%d) wrong", i+1)
			}
			i++
		})
		lo, hi := float64(r.IntN(100)), float64(r.IntN(100))
		rg := ScoreRange{Min: lo, Max: hi, MinEx: r.IntN(2) == 0}
		want := 0
		for _, p := range exp {
			if rg.GteMin(p.s) && rg.LteMax(p.s) {
				want++
			}
		}
		if got := z.Count(rg); got != want {
			t.Fatalf("Count(%+v)=%d want %d", rg, got, want)
		}
		got := 0
		z.RangeByScore(rg, r.IntN(2) == 0, 0, -1, func(string, float64) { got++ })
		if got != want {
			t.Fatalf("RangeByScore(%+v) returned %d want %d", rg, got, want)
		}
	}
}

func BenchmarkZSetAdd(b *testing.B) {
	z := NewZSet()
	for i := 0; i < b.N; i++ {
		z.Add(fmt.Sprint(i), rand.Float64())
	}
}

func BenchmarkZSetRank(b *testing.B) {
	z := NewZSet()
	for i := 0; i < 1_000_000; i++ {
		z.Add(fmt.Sprint(i), float64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		z.Rank(fmt.Sprint(i%1_000_000), false)
	}
}
