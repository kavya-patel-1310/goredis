package server

import (
	"bytes"
	"runtime"
	"sort"
	"strings"
	"time"

	"goredis/internal/glob"
	"goredis/internal/resp"
)

// This file is the in-process API used by the web dashboard. Everything
// goes through the same executor lock as network clients.

type CmdStat struct {
	Name        string  `json:"name"`
	Calls       int64   `json:"calls"`
	Usec        int64   `json:"usec"`
	UsecPerCall float64 `json:"usecPerCall"`
	Failed      int64   `json:"failed"`
}

type StatsSnapshot struct {
	Version          string         `json:"version"`
	Addr             string         `json:"addr"`
	UptimeSec        int64          `json:"uptimeSec"`
	Clients          int64          `json:"clients"`
	BlockedClients   int            `json:"blockedClients"`
	Keys             int            `json:"keys"`
	Expires          int            `json:"expires"`
	UsedMemory       int64          `json:"usedMemory"`
	MaxMemory        int64          `json:"maxMemory"`
	Policy           string         `json:"policy"`
	OpsPerSec        int64          `json:"opsPerSec"`
	TotalCommands    int64          `json:"totalCommands"`
	TotalConnections int64          `json:"totalConnections"`
	Hits             int64          `json:"hits"`
	Misses           int64          `json:"misses"`
	Expired          int64          `json:"expired"`
	Evicted          int64          `json:"evicted"`
	NetIn            int64          `json:"netIn"`
	NetOut           int64          `json:"netOut"`
	PubSubChannels   int            `json:"pubsubChannels"`
	PubSubPatterns   int            `json:"pubsubPatterns"`
	PubSubMessages   int64          `json:"pubsubMessages"`
	AOFEnabled       bool           `json:"aofEnabled"`
	AOFSize          int64          `json:"aofSize"`
	AOFPolicy        string         `json:"aofPolicy"`
	AOFRewriting     bool           `json:"aofRewriting"`
	LastSave         int64          `json:"lastSave"`
	ChangesSinceSave int64          `json:"changesSinceSave"`
	BgSaveRunning    bool           `json:"bgSaveRunning"`
	GoHeap           uint64         `json:"goHeap"`
	Goroutines       int            `json:"goroutines"`
	GCCycles         uint32         `json:"gcCycles"`
	TypeCounts       map[string]int `json:"typeCounts"`
	Commands         []CmdStat      `json:"commands"`
	History          []Sample       `json:"history"`
}

func (s *Server) Stats(withHistory bool) StatsSnapshot {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	st := StatsSnapshot{
		Version:          Version,
		UptimeSec:        int64(time.Since(s.startTime).Seconds()),
		Clients:          s.numClients.Load(),
		OpsPerSec:        s.stats.opsSec.Load(),
		TotalCommands:    s.stats.totalCommands.Load(),
		TotalConnections: s.stats.totalConnections.Load(),
		Hits:             s.stats.hits.Load(),
		Misses:           s.stats.misses.Load(),
		Expired:          s.stats.expired.Load(),
		Evicted:          s.stats.evicted.Load(),
		NetIn:            s.stats.netInput.Load(),
		NetOut:           s.stats.netOutput.Load(),
		PubSubMessages:   s.stats.pubsubMessages.Load(),
		GoHeap:           ms.HeapAlloc,
		GCCycles:         ms.NumGC,
		Goroutines:       runtime.NumGoroutine(),
		TypeCounts:       map[string]int{},
	}
	s.mu.Lock()
	if a := s.Addr(); a != nil {
		st.Addr = a.String()
	}
	for _, c := range s.clients {
		if c.blocked != nil {
			st.BlockedClients++
		}
	}
	st.Keys = s.db.Len()
	st.Expires = len(s.db.volatile)
	st.UsedMemory = s.db.used
	st.MaxMemory = s.cfg.MaxMemory
	st.Policy = s.cfg.MaxMemoryPolicy
	st.PubSubChannels = len(s.pubsub.channels)
	st.PubSubPatterns = len(s.pubsub.patterns)
	st.AOFEnabled = s.aof != nil
	st.AOFRewriting = s.rewriting
	st.LastSave = s.lastSave.Unix()
	st.ChangesSinceSave = s.dirty
	st.BgSaveRunning = s.bgSaveRunning
	for t, n := range s.db.types {
		if n > 0 {
			st.TypeCounts[ValueType(t).String()] = n
		}
	}
	for _, c := range s.commands {
		if calls := c.calls.Load(); calls > 0 {
			us := c.usec.Load()
			st.Commands = append(st.Commands, CmdStat{c.Name, calls, us, float64(us) / float64(calls), c.fails.Load()})
		}
	}
	s.mu.Unlock()
	if st.AOFEnabled {
		as := s.aof.stats()
		st.AOFSize, st.AOFPolicy = as.size, as.policy
	}
	sort.Slice(st.Commands, func(i, j int) bool { return st.Commands[i].Calls > st.Commands[j].Calls })
	if withHistory {
		s.stats.histMu.Lock()
		st.History = append([]Sample(nil), s.stats.history...)
		s.stats.histMu.Unlock()
	}
	return st
}

// Exec runs one command for the web console and returns the decoded reply.
func (s *Server) Exec(args [][]byte) resp.Value {
	if len(args) == 0 {
		return resp.Value{Type: '-', Str: "ERR empty command"}
	}
	s.mu.Lock()
	c := s.consoleClient()
	s.processCommand(c, args)
	out := append([]byte(nil), c.w.Buf...)
	c.w.Reset()
	c.quit.Store(false)
	wrote := c.wroteAOF
	c.wroteAOF = false
	s.mu.Unlock()
	if wrote && s.aof != nil {
		s.aof.flush()
	}
	v, err := resp.NewReader(bytes.NewReader(out)).ReadValue()
	if err != nil {
		return resp.Value{Type: '-', Str: "ERR " + err.Error()}
	}
	return v
}

func (s *Server) consoleClient() *Client {
	for _, c := range s.clients {
		if c.internal && c.name == "web-console" {
			return c
		}
	}
	c := s.newInternalClient("web-console")
	s.clients[c.id] = c
	return c
}

// KeyInfo is a row in the dashboard key browser.
type KeyInfo struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	TTLms int64  `json:"ttl"`
	Size  int64  `json:"size"`
	Len   int    `json:"len"`
}

func (s *Server) keyInfo(e *entry) KeyInfo {
	ki := KeyInfo{Key: e.key, Type: e.typ.String(), TTLms: -1, Size: e.size(), Len: entryLen(e)}
	if e.expireAt > 0 {
		ki.TTLms = e.expireAt - time.Now().UnixMilli()
	}
	return ki
}

func entryLen(e *entry) int {
	switch e.typ {
	case TString:
		return len(e.str)
	case TList:
		return e.list.Len()
	case THash:
		return len(e.hash.M)
	case TSet:
		return len(e.set.M)
	case TZSet:
		return e.zset.Len()
	}
	return 0
}

// ScanKeys pages through keys for the browser. It keeps scanning until it
// has `count` matches or has examined 20x count keys.
func (s *Server) ScanKeys(cursor uint64, pattern, typ string, count int) (uint64, []KeyInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	var out []KeyInfo
	budget := max(count*20, 20000)
	for {
		cursor = s.db.scan(cursor, min(budget, 1000), func(e *entry) {
			budget--
			if e.expireAt > 0 && e.expireAt <= now {
				return
			}
			if pattern != "" && pattern != "*" && !glob.Match(pattern, e.key) {
				return
			}
			if typ != "" && e.typ.String() != typ {
				return
			}
			out = append(out, s.keyInfo(e))
		})
		if cursor == 0 || len(out) >= count || budget <= 0 {
			break
		}
	}
	return cursor, out
}

type ZMember struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

type KeyDetail struct {
	KeyInfo
	Encoding  string `json:"encoding"`
	IdleSec   int64  `json:"idleSec"`
	Value     any    `json:"value"`
	Truncated bool   `json:"truncated"`
}

// KeyDetail returns a key's value (collections truncated to 1000 items).
func (s *Server) KeyDetail(key string) (KeyDetail, bool) {
	const limit = 1000
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.db.get(key)
	if e == nil || (e.expireAt > 0 && e.expireAt <= time.Now().UnixMilli()) {
		return KeyDetail{}, false
	}
	d := KeyDetail{KeyInfo: s.keyInfo(e), IdleSec: (time.Now().UnixMilli() - e.lru) / 1000}
	d.Truncated = d.Len > limit
	switch e.typ {
	case TString:
		d.Encoding = "raw"
		d.Value = string(e.str)
	case TList:
		d.Encoding = "ringbuffer"
		var vs []string
		for i := 0; i < min(e.list.Len(), limit); i++ {
			vs = append(vs, string(e.list.At(i)))
		}
		d.Value = vs
	case THash:
		d.Encoding = "hashtable"
		keys := make([]string, 0, len(e.hash.M))
		for k := range e.hash.M {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var pairs [][2]string
		for _, k := range keys[:min(len(keys), limit)] {
			pairs = append(pairs, [2]string{k, string(e.hash.M[k])})
		}
		d.Value = pairs
	case TSet:
		d.Encoding = "hashtable"
		ms := sortedMembers(e.set.M)
		d.Value = ms[:min(len(ms), limit)]
	case TZSet:
		d.Encoding = "skiplist"
		var zs []ZMember
		e.zset.RangeByRank(0, min(e.zset.Len(), limit)-1, false, func(m string, sc float64) {
			zs = append(zs, ZMember{m, sc})
		})
		d.Value = zs
	}
	return d, true
}

// SubscribeMonitor streams every executed command (like MONITOR).
func (s *Server) SubscribeMonitor() (<-chan string, func()) {
	ch := make(chan string, 1024)
	s.mu.Lock()
	id := s.nextSinkID
	s.nextSinkID++
	s.monSinks[id] = ch
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		delete(s.monSinks, id)
		s.mu.Unlock()
	}
}

// PubSubMessage is delivered to dashboard subscribers.
type PubSubMessage struct {
	Pattern string `json:"pattern,omitempty"`
	Channel string `json:"channel"`
	Message string `json:"message"`
	Time    int64  `json:"time"`
}

type sink struct{ ch chan PubSubMessage }

func (k *sink) deliver(pattern, channel string, msg []byte) {
	select {
	case k.ch <- PubSubMessage{pattern, channel, string(msg), time.Now().UnixMilli()}:
	default:
	}
}

// SubscribePubSub subscribes an internal sink to channels/patterns
// (entries containing glob characters are treated as patterns).
func (s *Server) SubscribePubSub(targets []string) (<-chan PubSubMessage, func()) {
	k := &sink{ch: make(chan PubSubMessage, 256)}
	s.mu.Lock()
	for _, t := range targets {
		if strings.ContainsAny(t, "*?[") {
			add(s.pubsub.patterns, t, k)
		} else {
			add(s.pubsub.channels, t, k)
		}
	}
	s.mu.Unlock()
	return k.ch, func() {
		s.mu.Lock()
		for _, t := range targets {
			remove(s.pubsub.patterns, t, k)
			remove(s.pubsub.channels, t, k)
		}
		s.mu.Unlock()
	}
}

// Slowlog returns the slow log, newest first.
func (s *Server) Slowlog() []slowlogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]slowlogEntry(nil), s.slowlog.entries...)
}

type ClientInfo struct {
	ID      int64  `json:"id"`
	Addr    string `json:"addr"`
	Name    string `json:"name"`
	AgeSec  int64  `json:"ageSec"`
	IdleSec int64  `json:"idleSec"`
	LastCmd string `json:"lastCmd"`
	Flags   string `json:"flags"`
}

func (s *Server) Clients() []ClientInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var out []ClientInfo
	for _, c := range s.clients {
		if c.internal {
			continue
		}
		flags := "normal"
		switch {
		case c.blocked != nil:
			flags = "blocked"
		case c.subCount() > 0:
			flags = "pubsub"
		case c.inMulti:
			flags = "multi"
		case c.monitor:
			flags = "monitor"
		}
		out = append(out, ClientInfo{c.id, c.addr, c.name, int64(now.Sub(c.createdAt).Seconds()),
			(now.UnixMilli() - c.lastInteraction.Load()) / 1000, c.lastCmd, flags})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Config returns CONFIG GET * as a map.
func (s *Server) ConfigMap() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := map[string]string{}
	for k, p := range configParams {
		if k == "requirepass" {
			continue
		}
		m[k] = p.get(s)
	}
	return m
}

// RequirePass returns the current password (dashboard auth).
func (s *Server) RequirePass() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg.RequirePass
}
