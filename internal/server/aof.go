package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"goredis/internal/resp"
)

// aof is the append-only file. Write commands are encoded as RESP and
// appended to an in-memory buffer by the executor; the buffer reaches the
// OS before the client gets its reply (see Client.flushAll), and is
// fsync'ed according to the policy:
//
//	always   - fsync before replying (safest, slowest)
//	everysec - fsync once per second in the background (default; lose ≤1s on power loss)
//	no       - let the OS decide
type aof struct {
	mu         sync.Mutex
	f          *os.File
	path       string
	buf        []byte
	policy     string
	size       int64
	baseSize   int64 // size after the last rewrite (for auto-rewrite)
	needFsync  bool
	lastErr    error
	rewriting  bool
	rewriteBuf []byte
}

func openAOF(path, policy string) (*aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &aof{f: f, path: path, policy: policy, size: st.Size(), baseSize: st.Size()}, nil
}

func (a *aof) feed(args [][]byte) {
	a.mu.Lock()
	start := len(a.buf)
	a.buf = resp.AppendCommand(a.buf, args...)
	if a.rewriting {
		a.rewriteBuf = append(a.rewriteBuf, a.buf[start:]...)
	}
	a.mu.Unlock()
}

func (a *aof) setPolicy(p string) {
	a.mu.Lock()
	a.policy = p
	a.mu.Unlock()
}

// flush writes pending bytes to the OS (and fsyncs if policy=always).
func (a *aof) flush() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.flushLocked()
}

func (a *aof) flushLocked() {
	if len(a.buf) == 0 {
		return
	}
	n, err := a.f.Write(a.buf)
	a.size += int64(n)
	if err != nil {
		a.lastErr = err
		// Keep unwritten bytes for a retry.
		a.buf = append(a.buf[:0], a.buf[n:]...)
		return
	}
	a.buf = a.buf[:0]
	a.lastErr = nil
	if a.policy == "always" {
		if err := a.f.Sync(); err != nil {
			a.lastErr = err
		}
	} else {
		a.needFsync = true
	}
}

type aofStats struct {
	size    int64
	pending int
	policy  string
	lastErr error
}

func (a *aof) stats() aofStats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return aofStats{size: a.size, pending: len(a.buf), policy: a.policy, lastErr: a.lastErr}
}

func (a *aof) close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.flushLocked()
	if err := a.f.Sync(); err != nil {
		return err
	}
	return a.f.Close()
}

// aofFsyncLoop flushes stray buffered writes and performs the everysec
// fsync off the hot path.
func (s *Server) aofFsyncLoop() {
	defer s.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.quit:
			return
		case <-t.C:
		}
		a := s.aof
		a.mu.Lock()
		a.flushLocked()
		f, sync := a.f, a.needFsync && a.policy == "everysec"
		a.needFsync = false
		a.mu.Unlock()
		if sync {
			if err := f.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
				s.logf("AOF fsync error: %v", err)
			}
		}
	}
}

// replayAOF loads the dataset by executing every command in the file
// through the normal command path with a pseudo-client. A truncated last
// command (e.g. after a crash mid-write) is trimmed, like Redis'
// aof-load-truncated.
func (s *Server) replayAOF(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := resp.NewReaderSize(f, 1<<20)
	c := s.newInternalClient("aof-loader")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loading = true
	defer func() { s.loading = false }()
	n := 0
	good := int64(0)
	for {
		args, err := r.ReadCommand()
		if err == io.EOF {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			s.logf("!!! AOF truncated at offset %d (partial last command). Trimming the file.", good)
			f.Close()
			if err := os.Truncate(path, good); err != nil {
				return n, err
			}
			break
		}
		if err != nil {
			return n, fmt.Errorf("bad AOF format at offset %d: %w (fix the file or remove the tail)", good, err)
		}
		s.processCommand(c, args)
		c.w.Reset()
		good = r.Offset()
		n++
	}
	if c.inMulti {
		s.logf("!!! AOF ends inside an unterminated MULTI; discarding %d queued commands", len(c.multiQueue))
	}
	s.dirty = 0
	return n, nil
}

// bgRewriteAOF compacts the AOF in the background: a point-in-time copy of
// the dataset is written as the minimal set of commands to rebuild it,
// while new writes are captured in a rewrite buffer and appended before
// the new file atomically replaces the old one.
func (s *Server) bgRewriteAOF() error {
	if s.aof == nil {
		return errors.New("append only file is disabled (start with --appendonly)")
	}
	if s.rewriting {
		return errors.New("Background append only file rewriting already in progress")
	}
	snap := s.db.clone()
	now := s.now
	s.aof.mu.Lock()
	s.aof.rewriting = true
	s.aof.rewriteBuf = nil
	s.aof.mu.Unlock()
	s.rewriting = true
	start := time.Now()
	go func() {
		tmp := s.aof.path + ".rewrite.tmp"
		err := writeAOFSnapshot(tmp, snap, now)
		s.mu.Lock()
		defer s.mu.Unlock()
		if err == nil {
			err = s.aof.finishRewrite(tmp)
		}
		if err != nil {
			s.aof.mu.Lock()
			s.aof.rewriting, s.aof.rewriteBuf = false, nil
			s.aof.mu.Unlock()
			os.Remove(tmp)
			s.logf("Background AOF rewrite failed: %v", err)
		} else {
			s.logf("Background AOF rewrite finished successfully in %v (%d keys, %s)",
				time.Since(start).Round(time.Millisecond), len(snap), HumanBytes(s.aof.size))
		}
		s.rewriting = false
	}()
	return nil
}

func (a *aof) finishRewrite(tmp string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(a.rewriteBuf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := os.Rename(tmp, a.path); err != nil {
		f.Close()
		return err
	}
	syncDir(filepath.Dir(a.path))
	a.f.Close()
	a.f = f
	// Pending bytes are either reflected in the snapshot (fed before the
	// rewrite started) or already contained in rewriteBuf.
	a.buf = a.buf[:0]
	st, _ := f.Stat()
	a.size = st.Size()
	a.baseSize = a.size
	a.rewriting, a.rewriteBuf = false, nil
	return nil
}

func writeAOFSnapshot(path string, entries []*entry, now int64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	var buf []byte
	emit := func(args ...[]byte) error {
		buf = resp.AppendCommand(buf[:0], args...)
		_, err := w.Write(buf)
		return err
	}
	const batch = 64
	for _, e := range entries {
		if e.expireAt > 0 && e.expireAt <= now {
			continue
		}
		k := []byte(e.key)
		var err error
		switch e.typ {
		case TString:
			err = emit([]byte("SET"), k, e.str)
		case TList:
			for i := 0; i < e.list.Len() && err == nil; i += batch {
				args := [][]byte{[]byte("RPUSH"), k}
				for j := i; j < min(i+batch, e.list.Len()); j++ {
					args = append(args, e.list.At(j))
				}
				err = emit(args...)
			}
		case THash:
			args := [][]byte{[]byte("HSET"), k}
			for f, v := range e.hash.M {
				args = append(args, []byte(f), v)
				if len(args) >= 2+2*batch {
					if err = emit(args...); err != nil {
						break
					}
					args = args[:2]
				}
			}
			if err == nil && len(args) > 2 {
				err = emit(args...)
			}
		case TSet:
			args := [][]byte{[]byte("SADD"), k}
			for m := range e.set.M {
				args = append(args, []byte(m))
				if len(args) >= 2+batch {
					if err = emit(args...); err != nil {
						break
					}
					args = args[:2]
				}
			}
			if err == nil && len(args) > 2 {
				err = emit(args...)
			}
		case TZSet:
			args := [][]byte{[]byte("ZADD"), k}
			e.zset.Each(func(m string, sc float64) {
				args = append(args, []byte(resp.FormatFloat(sc)), []byte(m))
				if len(args) >= 2+2*batch && err == nil {
					err = emit(args...)
					args = args[:2]
				}
			})
			if err == nil && len(args) > 2 {
				err = emit(args...)
			}
		}
		if err == nil && e.expireAt > 0 {
			err = emit([]byte("PEXPIREAT"), k, []byte(strconv.FormatInt(e.expireAt, 10)))
		}
		if err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
