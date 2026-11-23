package ds

const entryOverhead = 32

// Hash is a field→value map with byte accounting.
type Hash struct {
	M     map[string][]byte
	Bytes int64
}

func NewHash() *Hash { return &Hash{M: make(map[string][]byte)} }

// Set returns true if the field is new.
func (h *Hash) Set(field string, v []byte) bool {
	old, ok := h.M[field]
	if ok {
		h.Bytes += int64(len(v) - len(old))
	} else {
		h.Bytes += int64(len(field)+len(v)) + entryOverhead
	}
	h.M[field] = v
	return !ok
}

func (h *Hash) Del(field string) bool {
	old, ok := h.M[field]
	if ok {
		delete(h.M, field)
		h.Bytes -= int64(len(field)+len(old)) + entryOverhead
	}
	return ok
}

func (h *Hash) Clone() *Hash {
	nh := &Hash{M: make(map[string][]byte, len(h.M)), Bytes: h.Bytes}
	for k, v := range h.M {
		nh.M[k] = v
	}
	return nh
}

// Set is an unordered set of strings with byte accounting.
type Set struct {
	M     map[string]struct{}
	Bytes int64
}

func NewSet() *Set { return &Set{M: make(map[string]struct{})} }

func (s *Set) Add(m string) bool {
	if _, ok := s.M[m]; ok {
		return false
	}
	s.M[m] = struct{}{}
	s.Bytes += int64(len(m)) + entryOverhead
	return true
}

func (s *Set) Remove(m string) bool {
	if _, ok := s.M[m]; !ok {
		return false
	}
	delete(s.M, m)
	s.Bytes -= int64(len(m)) + entryOverhead
	return true
}

func (s *Set) Has(m string) bool { _, ok := s.M[m]; return ok }

func (s *Set) Clone() *Set {
	ns := &Set{M: make(map[string]struct{}, len(s.M)), Bytes: s.Bytes}
	for k := range s.M {
		ns.M[k] = struct{}{}
	}
	return ns
}
