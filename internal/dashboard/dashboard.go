// Package dashboard serves the GoRedis web UI, its JSON/SSE API and a
// Prometheus /metrics endpoint. The UI is a single embedded HTML file with
// no external dependencies, so the binary is fully self-contained.
package dashboard

import (
	"crypto/subtle"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"goredis/internal/resp"
	"goredis/internal/server"
)

//go:embed static
var staticFS embed.FS

type handler struct{ s *server.Server }

// Handler returns the dashboard HTTP handler.
func Handler(s *server.Server) http.Handler {
	h := &handler{s}
	mux := http.NewServeMux()
	sub, _ := fs.Sub(staticFS, "static")
	mux.Handle("GET /", http.FileServerFS(sub))
	mux.HandleFunc("GET /api/stats", h.auth(h.stats))
	mux.HandleFunc("GET /api/stream", h.auth(h.stream))
	mux.HandleFunc("GET /api/keys", h.auth(h.keys))
	mux.HandleFunc("GET /api/key", h.auth(h.key))
	mux.HandleFunc("POST /api/exec", h.auth(h.exec))
	mux.HandleFunc("GET /api/monitor", h.auth(h.monitor))
	mux.HandleFunc("GET /api/pubsub", h.auth(h.pubsub))
	mux.HandleFunc("GET /api/slowlog", h.auth(h.slowlog))
	mux.HandleFunc("GET /api/clients", h.auth(h.clients))
	mux.HandleFunc("GET /api/config", h.auth(h.config))
	mux.HandleFunc("GET /metrics", h.auth(h.metrics))
	return securityHeaders(mux)
}

// ListenAndServe starts the dashboard server.
func ListenAndServe(addr string, s *server.Server) error {
	srv := &http.Server{Addr: addr, Handler: Handler(s), ReadHeaderTimeout: 10 * time.Second}
	return srv.ListenAndServe()
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

// auth enforces requirepass (header X-GoRedis-Token, or ?token= for SSE,
// since EventSource can't set headers).
func (h *handler) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if pass := h.s.RequirePass(); pass != "" {
			tok := r.Header.Get("X-GoRedis-Token")
			if tok == "" {
				tok = r.URL.Query().Get("token")
			}
			if subtle.ConstantTimeCompare([]byte(tok), []byte(pass)) != 1 {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
				return
			}
		}
		next(w, r)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func (h *handler) stats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, h.s.Stats(true))
}

// sse prepares a Server-Sent Events response.
func sse(w http.ResponseWriter) (http.Flusher, bool) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return nil, false
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	fmt.Fprint(w, "retry: 2000\n\n")
	fl.Flush()
	return fl, true
}

func sendEvent(w http.ResponseWriter, fl http.Flusher, event string, v any) error {
	b, _ := json.Marshal(v)
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b); err != nil {
		return err
	}
	fl.Flush()
	return nil
}

func (h *handler) stream(w http.ResponseWriter, r *http.Request) {
	fl, ok := sse(w)
	if !ok {
		return
	}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		st := h.s.Stats(true)
		if n := len(st.History); n > 0 {
			st.History = st.History[n-1:]
		}
		if sendEvent(w, fl, "stats", st) != nil {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-t.C:
		}
	}
}

func (h *handler) keys(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	cursor, _ := strconv.ParseUint(q.Get("cursor"), 10, 64)
	count, _ := strconv.Atoi(q.Get("count"))
	if count <= 0 || count > 1000 {
		count = 100
	}
	next, keys := h.s.ScanKeys(cursor, q.Get("pattern"), q.Get("type"), count)
	if keys == nil {
		keys = []server.KeyInfo{}
	}
	writeJSON(w, 200, map[string]any{"cursor": strconv.FormatUint(next, 10), "keys": keys})
}

func (h *handler) key(w http.ResponseWriter, r *http.Request) {
	d, ok := h.s.KeyDetail(r.URL.Query().Get("key"))
	if !ok {
		writeJSON(w, 404, map[string]string{"error": "no such key"})
		return
	}
	writeJSON(w, 200, d)
}

func (h *handler) exec(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Command string   `json:"command"`
		Args    []string `json:"args"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	var args [][]byte
	if len(req.Args) > 0 {
		for _, a := range req.Args {
			args = append(args, []byte(a))
		}
	} else {
		var err error
		if args, err = resp.SplitArgs(strings.TrimSpace(req.Command)); err != nil {
			writeJSON(w, 200, map[string]any{"type": "error", "result": "ERR " + err.Error()})
			return
		}
	}
	start := time.Now()
	v := h.s.Exec(args)
	writeJSON(w, 200, map[string]any{
		"type":   typeName(v),
		"result": v.Interface(),
		"us":     time.Since(start).Microseconds(),
	})
}

func typeName(v resp.Value) string {
	switch v.Type {
	case '+':
		return "status"
	case '-':
		return "error"
	case ':':
		return "integer"
	case '$':
		if v.Null {
			return "nil"
		}
		return "bulk"
	case '*':
		if v.Null {
			return "nil"
		}
		return "array"
	}
	return "unknown"
}

func (h *handler) monitor(w http.ResponseWriter, r *http.Request) {
	fl, ok := sse(w)
	if !ok {
		return
	}
	ch, cancel := h.s.SubscribeMonitor()
	defer cancel()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case line := <-ch:
			if sendEvent(w, fl, "cmd", line) != nil {
				return
			}
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		}
	}
}

func (h *handler) pubsub(w http.ResponseWriter, r *http.Request) {
	var targets []string
	for _, t := range strings.Split(r.URL.Query().Get("channels"), ",") {
		if t = strings.TrimSpace(t); t != "" {
			targets = append(targets, t)
		}
	}
	if len(targets) == 0 {
		writeJSON(w, 400, map[string]string{"error": "channels required"})
		return
	}
	fl, ok := sse(w)
	if !ok {
		return
	}
	ch, cancel := h.s.SubscribePubSub(targets)
	defer cancel()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case m := <-ch:
			if sendEvent(w, fl, "message", m) != nil {
				return
			}
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		}
	}
}

func (h *handler) slowlog(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, h.s.Slowlog())
}

func (h *handler) clients(w http.ResponseWriter, r *http.Request) {
	c := h.s.Clients()
	if c == nil {
		c = []server.ClientInfo{}
	}
	writeJSON(w, 200, c)
}

func (h *handler) config(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, h.s.ConfigMap())
}

// metrics exposes Prometheus text format.
func (h *handler) metrics(w http.ResponseWriter, r *http.Request) {
	st := h.s.Stats(false)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	g := func(name, help, typ string, v any) {
		fmt.Fprintf(w, "# HELP goredis_%s %s\n# TYPE goredis_%s %s\ngoredis_%s %v\n", name, help, name, typ, name, v)
	}
	g("uptime_seconds", "Server uptime.", "gauge", st.UptimeSec)
	g("connected_clients", "Connected clients.", "gauge", st.Clients)
	g("blocked_clients", "Clients blocked on BLPOP/BRPOP.", "gauge", st.BlockedClients)
	g("keys", "Number of keys.", "gauge", st.Keys)
	g("keys_with_expiry", "Keys with a TTL.", "gauge", st.Expires)
	g("used_memory_bytes", "Estimated dataset memory.", "gauge", st.UsedMemory)
	g("maxmemory_bytes", "Configured memory limit.", "gauge", st.MaxMemory)
	g("go_heap_bytes", "Go heap in use.", "gauge", st.GoHeap)
	g("commands_processed_total", "Commands processed.", "counter", st.TotalCommands)
	g("connections_received_total", "Connections accepted.", "counter", st.TotalConnections)
	g("keyspace_hits_total", "Successful key lookups.", "counter", st.Hits)
	g("keyspace_misses_total", "Failed key lookups.", "counter", st.Misses)
	g("expired_keys_total", "Keys removed by expiry.", "counter", st.Expired)
	g("evicted_keys_total", "Keys removed by maxmemory eviction.", "counter", st.Evicted)
	g("net_input_bytes_total", "Bytes read from clients.", "counter", st.NetIn)
	g("net_output_bytes_total", "Bytes written to clients.", "counter", st.NetOut)
	g("aof_size_bytes", "Append-only file size.", "gauge", st.AOFSize)
	g("changes_since_last_save", "Writes since last snapshot.", "gauge", st.ChangesSinceSave)
	fmt.Fprint(w, "# HELP goredis_command_calls_total Calls per command.\n# TYPE goredis_command_calls_total counter\n")
	for _, c := range st.Commands {
		fmt.Fprintf(w, "goredis_command_calls_total{cmd=%q} %d\n", c.Name, c.Calls)
	}
	fmt.Fprint(w, "# HELP goredis_command_duration_seconds_total Time spent per command.\n# TYPE goredis_command_duration_seconds_total counter\n")
	for _, c := range st.Commands {
		fmt.Fprintf(w, "goredis_command_duration_seconds_total{cmd=%q} %g\n", c.Name, float64(c.Usec)/1e6)
	}
}
