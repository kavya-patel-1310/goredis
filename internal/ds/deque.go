// Package ds contains the data structures backing GoRedis value types.
package ds

// Deque is a growable ring buffer giving O(1) push/pop at both ends and
// O(1) random access — the backing store for Redis lists.
type Deque struct {
	buf   [][]byte
	head  int
	n     int
	Bytes int64 // payload bytes, for memory accounting
}

func NewDeque() *Deque { return &Deque{buf: make([][]byte, 8)} }

func (d *Deque) Len() int { return d.n }

func (d *Deque) grow() {
	nb := make([][]byte, len(d.buf)*2)
	for i := 0; i < d.n; i++ {
		nb[i] = d.buf[(d.head+i)%len(d.buf)]
	}
	d.buf, d.head = nb, 0
}

func (d *Deque) PushBack(v []byte) {
	if d.n == len(d.buf) {
		d.grow()
	}
	d.buf[(d.head+d.n)%len(d.buf)] = v
	d.n++
	d.Bytes += int64(len(v))
}

func (d *Deque) PushFront(v []byte) {
	if d.n == len(d.buf) {
		d.grow()
	}
	d.head = (d.head - 1 + len(d.buf)) % len(d.buf)
	d.buf[d.head] = v
	d.n++
	d.Bytes += int64(len(v))
}

func (d *Deque) PopFront() ([]byte, bool) {
	if d.n == 0 {
		return nil, false
	}
	v := d.buf[d.head]
	d.buf[d.head] = nil
	d.head = (d.head + 1) % len(d.buf)
	d.n--
	d.Bytes -= int64(len(v))
	d.maybeShrink()
	return v, true
}

func (d *Deque) PopBack() ([]byte, bool) {
	if d.n == 0 {
		return nil, false
	}
	i := (d.head + d.n - 1) % len(d.buf)
	v := d.buf[i]
	d.buf[i] = nil
	d.n--
	d.Bytes -= int64(len(v))
	d.maybeShrink()
	return v, true
}

func (d *Deque) maybeShrink() {
	if len(d.buf) > 64 && d.n < len(d.buf)/4 {
		nb := make([][]byte, len(d.buf)/2)
		for i := 0; i < d.n; i++ {
			nb[i] = d.buf[(d.head+i)%len(d.buf)]
		}
		d.buf, d.head = nb, 0
	}
}

// At returns the i-th element (0-based, must be in range).
func (d *Deque) At(i int) []byte { return d.buf[(d.head+i)%len(d.buf)] }

func (d *Deque) Set(i int, v []byte) {
	j := (d.head + i) % len(d.buf)
	d.Bytes += int64(len(v)) - int64(len(d.buf[j]))
	d.buf[j] = v
}

// Slice returns a copy of elements [start, stop] inclusive (already clamped).
func (d *Deque) Slice(start, stop int) [][]byte {
	if start > stop {
		return nil
	}
	out := make([][]byte, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		out = append(out, d.At(i))
	}
	return out
}

// Filter keeps elements for which keep returns true.
func (d *Deque) Filter(keep func(i int, v []byte) bool) {
	nd := NewDeque()
	for i := 0; i < d.n; i++ {
		if v := d.At(i); keep(i, v) {
			nd.PushBack(v)
		}
	}
	*d = *nd
}

// Clone returns a shallow copy (element byte slices are shared; GoRedis
// never mutates element bytes in place).
func (d *Deque) Clone() *Deque {
	nd := &Deque{buf: make([][]byte, max(8, d.n)), Bytes: d.Bytes, n: d.n}
	for i := 0; i < d.n; i++ {
		nd.buf[i] = d.At(i)
	}
	return nd
}
