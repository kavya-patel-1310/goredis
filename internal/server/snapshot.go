package server

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"goredis/internal/ds"
)

// Snapshot file format (".grdb"), a compact binary point-in-time dump
// inspired by Redis' RDB:
//
//	"GOREDIS" | version(1)
//	{ [0xFC expireAt:int64 LE] type:byte key value }*
//	0xFF
//	crc32-IEEE of everything above (uint32 LE)
//
// Strings are uvarint-length-prefixed; collections are a uvarint count
// followed by their elements; zset scores are IEEE-754 float64 LE.
// Files are written to a temp file, fsync'ed, then atomically renamed.

const (
	snapMagic        = "GOREDIS"
	snapVersion      = 1
	opExpire    byte = 0xFC
	opEOF       byte = 0xFF
)

type snapWriter struct {
	w   *bufio.Writer
	crc hashWriter
	tmp [binary.MaxVarintLen64]byte
	err error
}

type hashWriter interface {
	io.Writer
	Sum32() uint32
}

func (sw *snapWriter) write(b []byte) {
	if sw.err != nil {
		return
	}
	sw.crc.Write(b)
	_, sw.err = sw.w.Write(b)
}

func (sw *snapWriter) byte(b byte) { sw.write([]byte{b}) }
func (sw *snapWriter) uvarint(n uint64) {
	sw.write(sw.tmp[:binary.PutUvarint(sw.tmp[:], n)])
}
func (sw *snapWriter) str(b []byte) { sw.uvarint(uint64(len(b))); sw.write(b) }
func (sw *snapWriter) u64(n uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], n)
	sw.write(b[:])
}

func writeSnapshot(path string, entries []*entry, now int64) error {
	tmp := fmt.Sprintf("%s.tmp-%d", path, os.Getpid())
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	sw := &snapWriter{w: bufio.NewWriterSize(f, 1<<20), crc: crc32.NewIEEE()}
	sw.write([]byte(snapMagic))
	sw.byte(snapVersion)
	for _, e := range entries {
		if e.expireAt > 0 && e.expireAt <= now {
			continue
		}
		if e.expireAt > 0 {
			sw.byte(opExpire)
			sw.u64(uint64(e.expireAt))
		}
		sw.byte(byte(e.typ))
		sw.str([]byte(e.key))
		switch e.typ {
		case TString:
			sw.str(e.str)
		case TList:
			sw.uvarint(uint64(e.list.Len()))
			for i := 0; i < e.list.Len(); i++ {
				sw.str(e.list.At(i))
			}
		case THash:
			sw.uvarint(uint64(len(e.hash.M)))
			for k, v := range e.hash.M {
				sw.str([]byte(k))
				sw.str(v)
			}
		case TSet:
			sw.uvarint(uint64(len(e.set.M)))
			for m := range e.set.M {
				sw.str([]byte(m))
			}
		case TZSet:
			sw.uvarint(uint64(e.zset.Len()))
			e.zset.Each(func(m string, sc float64) {
				sw.str([]byte(m))
				sw.u64(math.Float64bits(sc))
			})
		}
	}
	sw.byte(opEOF)
	if sw.err == nil {
		var c [4]byte
		binary.LittleEndian.PutUint32(c[:], sw.crc.Sum32())
		_, sw.err = sw.w.Write(c[:])
	}
	if sw.err == nil {
		sw.err = sw.w.Flush()
	}
	if sw.err == nil {
		sw.err = f.Sync()
	}
	if cerr := f.Close(); sw.err == nil {
		sw.err = cerr
	}
	if sw.err != nil {
		os.Remove(tmp)
		return sw.err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

var errCorrupt = errors.New("snapshot is corrupt")

func (s *Server) loadSnapshot(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() < int64(len(snapMagic))+1+1+4 {
		return errCorrupt
	}
	// Pass 1: verify checksum.
	h := crc32.NewIEEE()
	if _, err := io.Copy(h, io.LimitReader(f, st.Size()-4)); err != nil {
		return err
	}
	var want [4]byte
	if _, err := io.ReadFull(f, want[:]); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(want[:]) != h.Sum32() {
		return fmt.Errorf("%w: checksum mismatch", errCorrupt)
	}
	// Pass 2: decode.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(f, 1<<20)
	hdr := make([]byte, len(snapMagic)+1)
	if _, err := io.ReadFull(r, hdr); err != nil || string(hdr[:len(snapMagic)]) != snapMagic {
		return fmt.Errorf("%w: bad header", errCorrupt)
	}
	if hdr[len(snapMagic)] != snapVersion {
		return fmt.Errorf("unsupported snapshot version %d", hdr[len(snapMagic)])
	}
	readStr := func() ([]byte, error) {
		n, err := binary.ReadUvarint(r)
		if err != nil || n > 512<<20 {
			return nil, errCorrupt
		}
		b := make([]byte, n)
		_, err = io.ReadFull(r, b)
		return b, err
	}
	readU64 := func() (uint64, error) {
		var b [8]byte
		_, err := io.ReadFull(r, b[:])
		return binary.LittleEndian.Uint64(b[:]), err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	for {
		op, err := r.ReadByte()
		if err != nil {
			return errCorrupt
		}
		if op == opEOF {
			break
		}
		var expireAt int64
		if op == opExpire {
			v, err := readU64()
			if err != nil {
				return errCorrupt
			}
			expireAt = int64(v)
			if op, err = r.ReadByte(); err != nil {
				return errCorrupt
			}
		}
		key, err := readStr()
		if err != nil {
			return err
		}
		e := &entry{key: string(key), typ: ValueType(op), expireAt: expireAt, lru: now, eidx: -1}
		switch e.typ {
		case TString:
			if e.str, err = readStr(); err != nil {
				return err
			}
		case TList, THash, TSet, TZSet:
			n, err := binary.ReadUvarint(r)
			if err != nil {
				return errCorrupt
			}
			switch e.typ {
			case TList:
				e.list = ds.NewDeque()
			case THash:
				e.hash = ds.NewHash()
			case TSet:
				e.set = ds.NewSet()
			case TZSet:
				e.zset = ds.NewZSet()
			}
			for i := uint64(0); i < n; i++ {
				a, err := readStr()
				if err != nil {
					return err
				}
				switch e.typ {
				case TList:
					e.list.PushBack(a)
				case TSet:
					e.set.Add(string(a))
				case THash:
					v, err := readStr()
					if err != nil {
						return err
					}
					e.hash.Set(string(a), v)
				case TZSet:
					bits, err := readU64()
					if err != nil {
						return errCorrupt
					}
					e.zset.Add(string(a), math.Float64frombits(bits))
				}
			}
		default:
			return fmt.Errorf("%w: unknown type %d", errCorrupt, op)
		}
		if expireAt > 0 && expireAt <= now {
			continue // expired while the server was down
		}
		s.setKey(e)
	}
	return nil
}

// saveSnapshotLocked implements SAVE (blocking; s.mu held).
func (s *Server) saveSnapshotLocked() error {
	if s.bgSaveRunning {
		return errors.New("Background save already in progress")
	}
	start := time.Now()
	err := writeSnapshot(filepath.Join(s.cfg.Dir, s.cfg.SnapshotFilename), s.db.keys, time.Now().UnixMilli())
	s.lastSaveOK = err == nil
	if err != nil {
		s.logf("Error saving DB on disk: %v", err)
		return err
	}
	s.dirty = 0
	s.lastSave = time.Now()
	s.logf("DB saved on disk (%d keys, %v)", s.db.Len(), time.Since(start).Round(time.Millisecond))
	return nil
}

// bgSave writes a snapshot from a point-in-time copy without blocking
// clients for the duration of the disk write.
func (s *Server) bgSave() error {
	if s.bgSaveRunning {
		return errors.New("Background save already in progress")
	}
	snap := s.db.clone()
	dirtyAtStart := s.dirty
	s.bgSaveRunning = true
	start := time.Now()
	go func() {
		err := writeSnapshot(filepath.Join(s.cfg.Dir, s.cfg.SnapshotFilename), snap, time.Now().UnixMilli())
		s.mu.Lock()
		defer s.mu.Unlock()
		s.bgSaveRunning = false
		s.lastBgSaveDur = time.Since(start)
		s.lastSaveOK = err == nil
		if err != nil {
			s.logf("Background saving error: %v", err)
			return
		}
		s.dirty -= dirtyAtStart
		s.lastSave = time.Now()
		s.logf("Background saving terminated with success (%d keys, %v)", len(snap), s.lastBgSaveDur.Round(time.Millisecond))
	}()
	return nil
}

// maybeAutoSave triggers BGSAVE (snapshot rules) and BGREWRITEAOF (AOF
// grew to 2x its post-rewrite size and over 64MB). Called once a second.
func (s *Server) maybeAutoSave() {
	if s.cfg.SaveSeconds > 0 && !s.bgSaveRunning && s.dirty >= s.cfg.SaveChanges && s.dirty > 0 &&
		time.Since(s.lastSave) >= time.Duration(s.cfg.SaveSeconds)*time.Second {
		s.logf("%d changes in %d seconds. Saving...", s.dirty, s.cfg.SaveSeconds)
		s.bgSave()
	}
	if s.aof != nil && !s.rewriting {
		st := s.aof.stats()
		s.aof.mu.Lock()
		base := s.aof.baseSize
		s.aof.mu.Unlock()
		if st.size > 64<<20 && st.size > 2*base {
			s.logf("Starting automatic rewriting of AOF on %d%% growth", (st.size-base)*100/max(base, 1))
			s.bgRewriteAOF()
		}
	}
}
