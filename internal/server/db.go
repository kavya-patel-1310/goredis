package server

import (
	"math/rand/v2"

	"goredis/internal/ds"
)

// ValueType is the Redis data type stored at a key.
type ValueType uint8

const (
	TString ValueType = iota
	TList
	THash
	TSet
	TZSet
)

func (t ValueType) String() string {
	return [...]string{"string", "list", "hash", "set", "zset"}[t]
}

// entry is one key in the keyspace. Exactly one of the value fields is set,
// according to typ.
type entry struct {
	key  string
	typ  ValueType
	str  []byte
	list *ds.Deque
	hash *ds.Hash
	set  *ds.Set
	zset *ds.ZSet

	expireAt int64 // unix ms; 0 = persistent
	lru      int64 // unix ms of last access (approximated LRU)

	idx  int   // position in DB.keys
	eidx int   // position in DB.volatile, or -1
	acct int64 // size last accounted into DB.used
}

const keyOverhead = 64 // rough per-key bookkeeping cost (map slot + struct)

// size estimates the memory used by the entry. It is O(1): containers keep
// running byte counters that are updated on every mutation.
func (e *entry) size() int64 {
	n := int64(len(e.key)) + keyOverhead
	switch e.typ {
	case TString:
		n += int64(len(e.str))
	case TList:
		n += e.list.Bytes + int64(e.list.Len())*24
	case THash:
		n += e.hash.Bytes
	case TSet:
		n += e.set.Bytes
	case TZSet:
		n += e.zset.Bytes
	}
	return n
}

// isEmpty reports whether a collection became empty (Redis deletes such keys).
func (e *entry) isEmpty() bool {
	switch e.typ {
	case TList:
		return e.list.Len() == 0
	case THash:
		return len(e.hash.M) == 0
	case TSet:
		return len(e.set.M) == 0
	case TZSet:
		return e.zset.Len() == 0
	}
	return false
}

// DB is the keyspace.
//
// Besides the hash map, keys are kept in a dense slice. That gives:
//   - O(1) uniform random sampling for RANDOMKEY and approximated-LRU eviction;
//   - a SCAN cursor that never skips keys: iteration walks the slice from the
//     end towards the start, and deletion swaps the *last* element (already
//     visited) into the hole, so an unvisited key can never be moved behind
//     the cursor. Same guarantee Redis gets from reverse-binary cursors.
//
// Keys with a TTL are additionally tracked in `volatile` for O(1) sampling
// by the active-expiry cycle.
type DB struct {
	dict     map[string]*entry
	keys     []*entry
	volatile []*entry
	used     int64 // estimated bytes
	types    [5]int
}

func newDB() *DB { return &DB{dict: make(map[string]*entry)} }

func (db *DB) Len() int { return len(db.dict) }

// get returns the entry without expiry checks.
func (db *DB) get(key string) *entry { return db.dict[key] }

// add inserts a new entry (key must not exist).
func (db *DB) add(e *entry) {
	e.idx = len(db.keys)
	e.eidx = -1
	db.keys = append(db.keys, e)
	db.dict[e.key] = e
	if e.expireAt > 0 {
		db.trackVolatile(e)
	}
	e.acct = e.size()
	db.used += e.acct
	db.types[e.typ]++
}

// resync re-measures an entry after a mutation and updates DB.used.
func (db *DB) resync(e *entry) {
	s := e.size()
	db.used += s - e.acct
	e.acct = s
}

func (db *DB) remove(e *entry) {
	last := db.keys[len(db.keys)-1]
	db.keys[e.idx] = last
	last.idx = e.idx
	db.keys[len(db.keys)-1] = nil
	db.keys = db.keys[:len(db.keys)-1]
	delete(db.dict, e.key)
	if e.eidx >= 0 {
		db.untrackVolatile(e)
	}
	db.used -= e.acct
	db.types[e.typ]--
}

func (db *DB) trackVolatile(e *entry) {
	if e.eidx >= 0 {
		return
	}
	e.eidx = len(db.volatile)
	db.volatile = append(db.volatile, e)
}

func (db *DB) untrackVolatile(e *entry) {
	if e.eidx < 0 {
		return
	}
	last := db.volatile[len(db.volatile)-1]
	db.volatile[e.eidx] = last
	last.eidx = e.eidx
	db.volatile[len(db.volatile)-1] = nil
	db.volatile = db.volatile[:len(db.volatile)-1]
	e.eidx = -1
}

func (db *DB) setExpire(e *entry, at int64) {
	e.expireAt = at
	if at > 0 {
		db.trackVolatile(e)
	} else {
		db.untrackVolatile(e)
	}
}

func (db *DB) randomEntry() *entry {
	if len(db.keys) == 0 {
		return nil
	}
	return db.keys[rand.IntN(len(db.keys))]
}

func (db *DB) randomVolatile() *entry {
	if len(db.volatile) == 0 {
		return nil
	}
	return db.volatile[rand.IntN(len(db.volatile))]
}

// scan implements the SCAN cursor (see type comment). Cursor 0 starts a new
// iteration; returned cursor 0 means the iteration is complete.
func (db *DB) scan(cursor uint64, count int, fn func(e *entry)) uint64 {
	pos := len(db.keys) - 1
	if cursor != 0 {
		pos = int(cursor) - 1
		if pos > len(db.keys)-1 {
			pos = len(db.keys) - 1
		}
	}
	for ; pos >= 0 && count > 0; pos-- {
		fn(db.keys[pos])
		count--
	}
	if pos < 0 {
		return 0
	}
	return uint64(pos) + 1
}

// clone deep-copies the dataset for background persistence. Strings are
// shared because GoRedis never mutates string bytes in place (APPEND may
// grow the backing array, but an existing slice header never observes it).
func (db *DB) clone() []*entry {
	out := make([]*entry, 0, len(db.keys))
	for _, e := range db.keys {
		c := &entry{key: e.key, typ: e.typ, str: e.str, expireAt: e.expireAt}
		switch e.typ {
		case TList:
			c.list = e.list.Clone()
		case THash:
			c.hash = e.hash.Clone()
		case TSet:
			c.set = e.set.Clone()
		case TZSet:
			c.zset = e.zset.Clone()
		}
		out = append(out, c)
	}
	return out
}
