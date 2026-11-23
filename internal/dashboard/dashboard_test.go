package dashboard

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"goredis/internal/server"
)

func newTestServer(t *testing.T, pass string) (*server.Server, *httptest.Server) {
	t.Helper()
	cfg := server.DefaultConfig()
	cfg.Addr = "127.0.0.1:0"
	cfg.Dir = t.TempDir()
	cfg.SaveSeconds = 0
	cfg.RequirePass = pass
	cfg.Logger = log.New(io.Discard, "", 0)
	s, err := server.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", cfg.Addr)
	go s.Serve(ln)
	hs := httptest.NewServer(Handler(s))
	t.Cleanup(func() { hs.Close(); s.Close() })
	return s, hs
}

func post(t *testing.T, url, body string, hdr map[string]string) map[string]any {
	t.Helper()
	req, _ := http.NewRequest("POST", url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return out
}

func TestExecAndKeys(t *testing.T) {
	_, hs := newTestServer(t, "")
	r := post(t, hs.URL+"/api/exec", `{"command":"SET greeting \"hello world\""}`, nil)
	if r["result"] != "OK" || r["type"] != "status" {
		t.Fatalf("exec SET: %v", r)
	}
	r = post(t, hs.URL+"/api/exec", `{"args":["RPUSH","list","a","b"]}`, nil)
	if r["result"].(float64) != 2 {
		t.Fatalf("exec RPUSH: %v", r)
	}
	r = post(t, hs.URL+"/api/exec", `{"command":"LRANGE list 0 -1"}`, nil)
	if arr := r["result"].([]any); len(arr) != 2 || arr[0] != "a" {
		t.Fatalf("exec LRANGE: %v", r)
	}

	resp, _ := http.Get(hs.URL + "/api/keys?pattern=*")
	var keys struct {
		Cursor string           `json:"cursor"`
		Keys   []server.KeyInfo `json:"keys"`
	}
	json.NewDecoder(resp.Body).Decode(&keys)
	if len(keys.Keys) != 2 || keys.Cursor != "0" {
		t.Fatalf("keys: %+v", keys)
	}

	resp, _ = http.Get(hs.URL + "/api/key?key=greeting")
	var d map[string]any
	json.NewDecoder(resp.Body).Decode(&d)
	if d["value"] != "hello world" || d["type"] != "string" {
		t.Fatalf("key detail: %v", d)
	}
}

func TestStatsMetricsAndUI(t *testing.T) {
	_, hs := newTestServer(t, "")
	post(t, hs.URL+"/api/exec", `{"command":"SET a 1"}`, nil)

	resp, _ := http.Get(hs.URL + "/api/stats")
	var st server.StatsSnapshot
	json.NewDecoder(resp.Body).Decode(&st)
	if st.Keys != 1 || st.Version == "" {
		t.Fatalf("stats: %+v", st)
	}

	resp, _ = http.Get(hs.URL + "/metrics")
	body, _ := io.ReadAll(resp.Body)
	for _, want := range []string{"goredis_keys 1", `goredis_command_calls_total{cmd="set"} 1`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics missing %q", want)
		}
	}

	resp, _ = http.Get(hs.URL + "/")
	body, _ = io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "GoRedis Dashboard") {
		t.Fatal("UI not served")
	}
}

func TestAuthRequired(t *testing.T) {
	_, hs := newTestServer(t, "pw")
	resp, _ := http.Get(hs.URL + "/api/stats")
	if resp.StatusCode != 401 {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
	resp, _ = http.Get(hs.URL + "/api/stats?token=pw")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 with token, got %d", resp.StatusCode)
	}
	r := post(t, hs.URL+"/api/exec", `{"command":"PING"}`, map[string]string{"X-GoRedis-Token": "pw"})
	if r["result"] != "PONG" {
		t.Fatalf("exec with token: %v", r)
	}
	// The static UI itself is public (it prompts for the password).
	resp, _ = http.Get(hs.URL + "/")
	if resp.StatusCode != 200 {
		t.Fatalf("UI status %d", resp.StatusCode)
	}
}
