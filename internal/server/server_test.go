package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"goredis/internal/resp"
)

// ---------------------------------------------------------------------------
// Test harness: a real server on a random port and a tiny RESP client.

func startServer(t *testing.T, mod func(*Config)) (*Server, string) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Addr = "127.0.0.1:0"
	cfg.Dir = t.TempDir()
	cfg.SaveSeconds = 0
	cfg.Logger = log.New(io.Discard, "", 0)
	if mod != nil {
		mod(&cfg)
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(ln)
	t.Cleanup(func() { s.Close() })
	return s, ln.Addr().String()
}

type client struct {
	t    *testing.T
	conn net.Conn
	r    *resp.Reader
	w    *bufio.Writer
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &client{t: t, conn: c, r: resp.NewReader(c), w: bufio.NewWriter(c)}
}

func (c *client) send(args ...string) {
	c.w.Write(resp.AppendCommandStrings(nil, args...))
	c.w.Flush()
}

func (c *client) read() resp.Value {
	c.t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	v, err := c.r.ReadValue()
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	return v
}

func (c *client) do(args ...string) resp.Value {
	c.t.Helper()
	if len(args) > 0 {
		c.send(args...)
	}
	return c.read()
}

// str renders a reply compactly for assertions: "OK", "(nil)", "(error) X",
// "5", "[a b c]".
func str(v resp.Value) string {
	switch v.Type {
	case '+':
		return v.Str
	case '-':
		return "(error) " + v.Str
	case ':':
		return fmt.Sprint(v.Int)
	case '$':
		if v.Null {
			return "(nil)"
		}
		return v.Str
	case '*':
		if v.Null {
			return "(nil)"
		}
		parts := make([]string, len(v.Array))
		for i, e := range v.Array {
			parts[i] = str(e)
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
	return "?"
}

func (c *client) expect(want string, args ...string) {
	c.t.Helper()
	if got := str(c.do(args...)); got != want {
		c.t.Fatalf("%v\n  got:  %s\n  want: %s", args, got, want)
	}
}

func (c *client) expectPrefix(want string, args ...string) {
	c.t.Helper()
	if got := str(c.do(args...)); !strings.HasPrefix(got, want) {
		c.t.Fatalf("%v\n  got:  %s\n  want prefix: %s", args, got, want)
	}
}

// ---------------------------------------------------------------------------

func TestStrings(t *testing.T) {
	_, addr := startServer(t, nil)
	c := dial(t, addr)
	c.expect("PONG", "PING")
	c.expect("hi", "ECHO", "hi")
	c.expect("OK", "SET", "k", "v")
	c.expect("v", "GET", "k")
	c.expect("(nil)", "GET", "missing")
	c.expect("(nil)", "SET", "k", "x", "NX")
	c.expect("OK", "SET", "k", "x", "XX")
	c.expect("x", "SET", "k", "y", "GET")
	c.expect("(nil)", "SET", "new", "1", "XX")
	c.expect("1", "SETNX", "nx", "1")
	c.expect("0", "SETNX", "nx", "2")
	c.expect("OK", "MSET", "a", "1", "b", "2")
	c.expect("[1 2 (nil)]", "MGET", "a", "b", "zz")
	c.expect("0", "MSETNX", "a", "9", "c", "3")
	c.expect("2", "INCR", "a")
	c.expect("12", "INCRBY", "a", "10")
	c.expect("11", "DECR", "a")
	c.expect("1", "DECRBY", "a", "10")
	c.expect("3.5", "INCRBYFLOAT", "f", "3.5")
	c.expect("(error) ERR value is not an integer or out of range", "INCR", "k")
	c.expect("OK", "SET", "max", "9223372036854775807")
	c.expect("(error) ERR increment or decrement would overflow", "INCR", "max")
	c.expect("5", "APPEND", "s", "Hello")
	c.expect("11", "APPEND", "s", " World")
	c.expect("World", "GETRANGE", "s", "-5", "-1")
	c.expect("11", "STRLEN", "s")
	c.expect("11", "SETRANGE", "s", "6", "Redis")
	c.expect("Hello Redis", "GET", "s")
	c.expect("Hello Redis", "GETDEL", "s")
	c.expect("0", "EXISTS", "s")
	c.expect("(error) ERR syntax error", "SET", "k", "v", "BOGUS")
	c.expect("(error) ERR wrong number of arguments for 'get' command", "GET")
	c.expectPrefix("(error) ERR unknown command 'NOPE'", "NOPE", "x")
}

func TestExpiry(t *testing.T) {
	s, addr := startServer(t, nil)
	c := dial(t, addr)
	c.expect("OK", "SET", "k", "v", "PX", "100")
	if ttl := c.do("PTTL", "k").Int; ttl <= 0 || ttl > 100 {
		t.Fatalf("PTTL = %d", ttl)
	}
	c.expect("OK", "SET", "p", "v")
	c.expect("-1", "TTL", "p")
	c.expect("-2", "TTL", "nope")
	c.expect("1", "EXPIRE", "p", "100")
	c.expect("0", "EXPIRE", "p", "50", "GT")
	c.expect("1", "EXPIRE", "p", "50", "LT")
	c.expect("50", "TTL", "p")
	c.expect("1", "PERSIST", "p")
	c.expect("-1", "TTL", "p")
	time.Sleep(150 * time.Millisecond)
	c.expect("(nil)", "GET", "k") // lazy expiry

	// Active expiry: keys that are never touched again still get reclaimed.
	for i := 0; i < 500; i++ {
		c.send("SET", fmt.Sprintf("tmp:%d", i), "x", "PX", "50")
	}
	for i := 0; i < 500; i++ {
		c.read()
	}
	c.expect("501", "DBSIZE")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.do("DBSIZE").Int == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.expect("1", "DBSIZE")
	if s.stats.expired.Load() < 500 {
		t.Fatalf("expired stat = %d", s.stats.expired.Load())
	}
	c.expect("1", "EXPIRE", "p", "-1") // past deadline deletes
	c.expect("0", "EXISTS", "p")
}

func TestLists(t *testing.T) {
	_, addr := startServer(t, nil)
	c := dial(t, addr)
	c.expect("3", "RPUSH", "l", "a", "b", "c")
	c.expect("5", "LPUSH", "l", "y", "z")
	c.expect("[z y a b c]", "LRANGE", "l", "0", "-1")
	c.expect("[b c]", "LRANGE", "l", "-2", "100")
	c.expect("a", "LINDEX", "l", "2")
	c.expect("OK", "LSET", "l", "0", "Z")
	c.expect("Z", "LPOP", "l")
	c.expect("[c b]", "RPOP", "l", "2")
	c.expect("2", "LLEN", "l")
	c.expect("3", "LINSERT", "l", "BEFORE", "a", "x")
	c.expect("[y x a]", "LRANGE", "l", "0", "-1")
	c.expect("4", "RPUSH", "l", "x")
	c.expect("2", "LREM", "l", "0", "x")
	c.expect("OK", "LTRIM", "l", "0", "0")
	c.expect("[y]", "LRANGE", "l", "0", "-1")
	c.expect("y", "RPOPLPUSH", "l", "l2")
	c.expect("0", "EXISTS", "l") // empty lists are deleted
	c.expect("0", "LPUSHX", "nope", "a")
	c.expect("OK", "SET", "str", "v")
	c.expect("(error) "+errWrongType, "LPUSH", "str", "a")
}

func TestBlockingPop(t *testing.T) {
	_, addr := startServer(t, nil)
	a, b := dial(t, addr), dial(t, addr)

	// Served by a later push.
	a.send("BLPOP", "q1", "q2", "5")
	time.Sleep(50 * time.Millisecond)
	b.expect("1", "RPUSH", "q2", "job")
	a.expect("[q2 job]")

	// Immediate when data exists.
	b.expect("1", "RPUSH", "q1", "now")
	a.expect("[q1 now]", "BRPOP", "q1", "1")

	// Timeout.
	start := time.Now()
	a.expect("(nil)", "BLPOP", "empty", "0.1")
	if d := time.Since(start); d < 90*time.Millisecond || d > time.Second {
		t.Fatalf("timeout took %v", d)
	}

	// FIFO fairness between two blocked clients.
	c := dial(t, addr)
	a.send("BLPOP", "fair", "5")
	time.Sleep(30 * time.Millisecond)
	c.send("BLPOP", "fair", "5")
	time.Sleep(30 * time.Millisecond)
	b.expect("2", "RPUSH", "fair", "first", "second")
	a.expect("[fair first]")
	c.expect("[fair second]")

	// Inside MULTI, BLPOP never blocks.
	a.expect("OK", "MULTI")
	a.expect("QUEUED", "BLPOP", "nothing", "0")
	a.expect("[(nil)]", "EXEC")
}

func TestBlockedClientDisconnect(t *testing.T) {
	s, addr := startServer(t, nil)
	a := dial(t, addr)
	a.send("BLPOP", "q", "0")
	time.Sleep(50 * time.Millisecond)
	a.conn.Close()
	time.Sleep(100 * time.Millisecond)
	s.mu.Lock()
	n := len(s.blockingKeys["q"])
	s.mu.Unlock()
	if n != 0 {
		t.Fatalf("disconnected client still blocked (%d)", n)
	}
	// The element must not be lost to a dead client.
	b := dial(t, addr)
	b.expect("1", "RPUSH", "q", "x")
	b.expect("1", "LLEN", "q")
}

func TestHashes(t *testing.T) {
	_, addr := startServer(t, nil)
	c := dial(t, addr)
	c.expect("2", "HSET", "h", "a", "1", "b", "2")
	c.expect("0", "HSET", "h", "a", "10")
	c.expect("10", "HGET", "h", "a")
	c.expect("[10 (nil)]", "HMGET", "h", "a", "zz")
	c.expect("2", "HLEN", "h")
	c.expect("1", "HEXISTS", "h", "b")
	c.expect("12", "HINCRBY", "h", "b", "10")
	c.expect("1.5", "HINCRBYFLOAT", "h", "f", "1.5")
	c.expect("0", "HSETNX", "h", "a", "x")
	v := c.do("HGETALL", "h")
	if len(v.Array) != 6 {
		t.Fatalf("HGETALL len %d", len(v.Array))
	}
	c.expect("3", "HDEL", "h", "a", "b", "f", "nope")
	c.expect("0", "EXISTS", "h")
}

func TestSets(t *testing.T) {
	_, addr := startServer(t, nil)
	c := dial(t, addr)
	c.expect("3", "SADD", "s1", "a", "b", "c")
	c.expect("0", "SADD", "s1", "a")
	c.expect("2", "SADD", "s2", "b", "c")
	c.expect("1", "SADD", "s2", "d")
	c.expect("[b c]", "SINTER", "s1", "s2")
	c.expect("[a b c d]", "SUNION", "s1", "s2")
	c.expect("[a]", "SDIFF", "s1", "s2")
	c.expect("2", "SINTERSTORE", "dst", "s1", "s2")
	c.expect("1", "SISMEMBER", "dst", "b")
	c.expect("[1 0]", "SMISMEMBER", "dst", "b", "a")
	c.expect("1", "SMOVE", "s1", "s3", "a")
	c.expect("2", "SCARD", "s1")
	if v := c.do("SPOP", "s1", "5"); len(v.Array) != 2 {
		t.Fatalf("SPOP count: %s", str(v))
	}
	c.expect("0", "EXISTS", "s1")
}

func TestSortedSets(t *testing.T) {
	_, addr := startServer(t, nil)
	c := dial(t, addr)
	c.expect("4", "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d")
	c.expect("0", "ZADD", "z", "NX", "10", "a")
	c.expect("1", "ZADD", "z", "CH", "5", "a")
	c.expect("0", "ZADD", "z", "GT", "1", "a")
	c.expect("6", "ZADD", "z", "INCR", "1", "a")
	c.expect("[b c d a]", "ZRANGE", "z", "0", "-1")
	c.expect("[a 6 d 4]", "ZRANGE", "z", "0", "1", "REV", "WITHSCORES")
	c.expect("[c d]", "ZRANGEBYSCORE", "z", "(2", "4")
	c.expect("[d c]", "ZREVRANGEBYSCORE", "z", "4", "(2")
	c.expect("[c]", "ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "1", "1")
	c.expect("[d c]", "ZRANGE", "z", "4", "3", "BYSCORE", "REV")
	c.expect("3", "ZCOUNT", "z", "2", "4")
	c.expect("3", "ZRANK", "z", "a")
	c.expect("0", "ZREVRANK", "z", "a")
	c.expect("6", "ZSCORE", "z", "a")
	c.expect("8.5", "ZINCRBY", "z", "2.5", "a")
	c.expect("[b 2]", "ZPOPMIN", "z")
	c.expect("[a 8.5]", "ZPOPMAX", "z")
	c.expect("1", "ZREMRANGEBYSCORE", "z", "4", "4")
	c.expect("1", "ZCARD", "z")
	c.expect("(error) ERR value is not a valid float", "ZADD", "z", "abc", "x")
}

func TestKeysScanRename(t *testing.T) {
	_, addr := startServer(t, nil)
	c := dial(t, addr)
	for i := 0; i < 1000; i++ {
		c.send("SET", fmt.Sprintf("user:%d", i), "x")
	}
	for i := 0; i < 1000; i++ {
		c.read()
	}
	c.expect("OK", "SET", "other", "x")
	c.expect("1001", "DBSIZE")
	if n := len(c.do("KEYS", "user:*").Array); n != 1000 {
		t.Fatalf("KEYS user:* = %d", n)
	}
	// SCAN must return every key that exists for the whole iteration, even
	// while keys are being deleted concurrently.
	seen := map[string]bool{}
	cursor := "0"
	deleted := 0
	for {
		v := c.do("SCAN", cursor, "MATCH", "user:*", "COUNT", "37")
		cursor = v.Array[0].Str
		for _, k := range v.Array[1].Array {
			seen[k.Str] = true
		}
		if deleted < 200 { // delete keys we've already seen
			for k := range seen {
				if deleted >= 200 {
					break
				}
				if c.do("DEL", k).Int == 1 {
					deleted++
				}
			}
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != 1000 {
		t.Fatalf("SCAN saw %d of 1000 keys", len(seen))
	}
	c.expect("OK", "RENAME", "other", "renamed")
	c.expect("x", "GET", "renamed")
	c.expect("(error) ERR no such key", "RENAME", "nope", "x")
	c.expect("string", "TYPE", "renamed")
	c.expect("none", "TYPE", "nope")
	c.expect("OK", "FLUSHALL")
	c.expect("0", "DBSIZE")
}

func TestTransactions(t *testing.T) {
	_, addr := startServer(t, nil)
	a, b := dial(t, addr), dial(t, addr)
	a.expect("OK", "MULTI")
	a.expect("QUEUED", "INCR", "n")
	a.expect("QUEUED", "INCR", "n")
	a.expect("QUEUED", "LPUSH", "n", "x") // runtime error, doesn't abort
	a.expect("[1 2 (error) "+errWrongType+"]", "EXEC")

	// Syntax error while queueing aborts the whole transaction.
	a.expect("OK", "MULTI")
	a.expect("(error) ERR wrong number of arguments for 'get' command", "GET")
	a.expect("QUEUED", "INCR", "n")
	a.expectPrefix("(error) EXECABORT", "EXEC")
	a.expect("2", "GET", "n")

	// WATCH: optimistic locking.
	a.expect("OK", "WATCH", "n")
	b.expect("3", "INCR", "n")
	a.expect("OK", "MULTI")
	a.expect("QUEUED", "INCR", "n")
	a.expect("(nil)", "EXEC")
	a.expect("OK", "WATCH", "n")
	a.expect("OK", "MULTI")
	a.expect("QUEUED", "INCR", "n")
	a.expect("[4]", "EXEC")
	a.expect("OK", "MULTI")
	a.expect("OK", "DISCARD")
	a.expect("(error) ERR EXEC without MULTI", "EXEC")
}

func TestPubSub(t *testing.T) {
	_, addr := startServer(t, nil)
	sub, pub := dial(t, addr), dial(t, addr)
	sub.expect("[subscribe news 1]", "SUBSCRIBE", "news")
	sub.send("PSUBSCRIBE", "chat:*")
	sub.expect("[psubscribe chat:* 2]")
	sub.expectPrefix("(error) ERR Can't execute 'get'", "GET", "x")
	pub.expect("1", "PUBLISH", "news", "hello")
	sub.expect("[message news hello]")
	pub.expect("1", "PUBLISH", "chat:42", "hi")
	sub.expect("[pmessage chat:* chat:42 hi]")
	pub.expect("0", "PUBLISH", "other", "x")
	pub.expect("[news]", "PUBSUB", "CHANNELS")
	pub.expect("[news 1]", "PUBSUB", "NUMSUB", "news")
	sub.expect("[unsubscribe news 1]", "UNSUBSCRIBE")
	sub.expect("[pong ]", "PING")
}

func TestPipelining(t *testing.T) {
	_, addr := startServer(t, nil)
	c := dial(t, addr)
	var buf []byte
	const n = 10000
	for i := 0; i < n; i++ {
		buf = resp.AppendCommandStrings(buf, "INCR", "ctr")
	}
	c.conn.Write(buf)
	for i := 1; i <= n; i++ {
		if v := c.read(); v.Int != int64(i) {
			t.Fatalf("reply %d = %d", i, v.Int)
		}
	}
}

func TestInlineProtocol(t *testing.T) {
	_, addr := startServer(t, nil)
	c := dial(t, addr)
	c.conn.Write([]byte("SET greeting \"hello world\"\r\nGET greeting\r\n"))
	if str(c.read()) != "OK" || str(c.read()) != "hello world" {
		t.Fatal("inline protocol failed")
	}
}

func TestConcurrentIncr(t *testing.T) {
	_, addr := startServer(t, nil)
	var wg sync.WaitGroup
	const workers, per = 20, 500
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := dial(t, addr)
			for i := 0; i < per; i++ {
				c.send("INCR", "shared")
			}
			for i := 0; i < per; i++ {
				c.read()
			}
		}()
	}
	wg.Wait()
	dial(t, addr).expect(fmt.Sprint(workers*per), "GET", "shared")
}

func TestAuth(t *testing.T) {
	_, addr := startServer(t, func(c *Config) { c.RequirePass = "s3cret" })
	c := dial(t, addr)
	c.expect("(error) NOAUTH Authentication required.", "GET", "x")
	c.expectPrefix("(error) WRONGPASS", "AUTH", "nope")
	c.expect("OK", "AUTH", "s3cret")
	c.expect("(nil)", "GET", "x")
}

func TestEvictionLRU(t *testing.T) {
	s, addr := startServer(t, func(c *Config) {
		c.MaxMemory = 200 * 1024
		c.MaxMemoryPolicy = "allkeys-lru"
		c.MaxMemorySamples = 10
	})
	c := dial(t, addr)
	val := strings.Repeat("x", 1000)
	c.expect("OK", "SET", "hot", val)
	for i := 0; i < 2000; i++ {
		c.do("GET", "hot") // keep it recently used
		c.do("SET", fmt.Sprintf("cold:%d", i), val)
	}
	s.mu.Lock()
	used, max := s.db.used, s.cfg.MaxMemory
	s.mu.Unlock()
	if used > max+2000 {
		t.Fatalf("used %d exceeds maxmemory %d", used, max)
	}
	if s.stats.evicted.Load() == 0 {
		t.Fatal("nothing evicted")
	}
	c.expect(val, "GET", "hot")

	c.expect("OK", "CONFIG", "SET", "maxmemory-policy", "noeviction")
	c.expectPrefix("(error) OOM", "SET", "more", val)
	c.expect("1", "DEL", "hot") // deletes are still allowed
}

func TestMemoryAccounting(t *testing.T) {
	s, addr := startServer(t, nil)
	c := dial(t, addr)
	for i := 0; i < 100; i++ {
		c.do("RPUSH", "l", strings.Repeat("a", 100))
		c.do("HSET", "h", fmt.Sprint(i), "v")
		c.do("ZADD", "z", fmt.Sprint(i), fmt.Sprint(i))
		c.do("SET", fmt.Sprint("s", i), "v", "EX", "100")
	}
	s.mu.Lock()
	var sum int64
	for _, e := range s.db.keys {
		sum += e.size()
	}
	used := s.db.used
	s.mu.Unlock()
	if sum != used {
		t.Fatalf("accounted %d != actual %d", used, sum)
	}
	c.do("FLUSHALL")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db.used != 0 {
		t.Fatalf("used after flush = %d", s.db.used)
	}
}

func verify(t *testing.T, c *client) {
	t.Helper()
	c.expect("hello", "GET", "str")
	if ttl := c.do("TTL", "ttl").Int; ttl < 990 || ttl > 1000 {
		t.Fatalf("TTL after restart = %d", ttl)
	}
	c.expect("[a b c]", "LRANGE", "list", "0", "-1")
	c.expect("v", "HGET", "hash", "f")
	c.expect("1", "SCARD", "set")
	c.expect("[one 1.5 two 2]", "ZRANGE", "zset", "0", "-1", "WITHSCORES")
	c.expect("(nil)", "GET", "gone")
	c.expect("10.5", "GET", "float")
}

func TestAOFPersistence(t *testing.T) {
	dir := t.TempDir()
	mod := func(c *Config) { c.Dir = dir; c.AppendOnly = true; c.AppendFsync = "always" }
	s, addr := startServer(t, mod)
	c := dial(t, addr)
	c.expect("OK", "SET", "str", "hello")
	c.expect("OK", "SET", "ttl", "x", "EX", "1000")
	c.expect("3", "RPUSH", "list", "a", "b", "c")
	c.expect("2", "HSET", "hash", "f", "v", "g", "w")
	c.expect("3", "SADD", "set", "m1", "m2", "m3")
	c.do("SPOP", "set", "2")
	c.expect("2", "ZADD", "zset", "1.5", "one", "2", "two")
	c.expect("OK", "SET", "gone", "x", "PX", "1")
	c.expect("10.5", "INCRBYFLOAT", "float", "10.5")
	c.expect("OK", "MULTI")
	c.expect("QUEUED", "INCR", "txn")
	c.expect("QUEUED", "INCR", "txn")
	c.expect("[1 2]", "EXEC")
	s.Close()

	data, _ := os.ReadFile(filepath.Join(dir, "appendonly.aof"))
	if strings.Contains(string(data), "INCRBYFLOAT") || strings.Contains(string(data), "SPOP") {
		t.Fatal("non-deterministic commands must be rewritten before propagation")
	}
	if !strings.Contains(string(data), "PXAT") || !strings.Contains(string(data), "MULTI") {
		t.Fatal("expected absolute expiry and MULTI/EXEC in AOF")
	}

	_, addr = startServer(t, mod)
	c = dial(t, addr)
	verify(t, c)
	c.expect("2", "GET", "txn")
}

func TestAOFTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	good := resp.AppendCommandStrings(nil, "SET", "a", "1")
	good = resp.AppendCommandStrings(good, "SET", "b", "2")
	partial := []byte("*3\r\n$3\r\nSET\r\n$1\r\nc")
	os.WriteFile(filepath.Join(dir, "appendonly.aof"), append(good, partial...), 0o644)
	_, addr := startServer(t, func(c *Config) { c.Dir = dir; c.AppendOnly = true })
	c := dial(t, addr)
	c.expect("2", "DBSIZE")
	st, _ := os.Stat(filepath.Join(dir, "appendonly.aof"))
	if st.Size() != int64(len(good)) {
		t.Fatalf("AOF not trimmed: %d vs %d", st.Size(), len(good))
	}
}

func TestAOFRewrite(t *testing.T) {
	dir := t.TempDir()
	mod := func(c *Config) { c.Dir = dir; c.AppendOnly = true }
	s, addr := startServer(t, mod)
	c := dial(t, addr)
	for i := 0; i < 1000; i++ {
		c.do("INCR", "counter")
	}
	c.do("RPUSH", "list", "a", "b")
	c.do("ZADD", "zset", "1.5", "one")
	c.do("SET", "ttl", "x", "EX", "1000")
	path := filepath.Join(dir, "appendonly.aof")
	s.aof.flush()
	before, _ := os.Stat(path)
	c.expectPrefix("Background append only file rewriting started", "BGREWRITEAOF")
	c.do("SET", "during", "rewrite") // captured by the rewrite buffer
	for i := 0; i < 100; i++ {
		time.Sleep(20 * time.Millisecond)
		s.mu.Lock()
		done := !s.rewriting
		s.mu.Unlock()
		if done {
			break
		}
	}
	c.do("SET", "after", "rewrite")
	s.Close()
	after, _ := os.Stat(path)
	if after.Size() >= before.Size() {
		t.Fatalf("rewrite did not shrink AOF: %d -> %d", before.Size(), after.Size())
	}
	_, addr = startServer(t, mod)
	c = dial(t, addr)
	c.expect("1000", "GET", "counter")
	c.expect("[a b]", "LRANGE", "list", "0", "-1")
	c.expect("1.5", "ZSCORE", "zset", "one")
	c.expect("rewrite", "GET", "during")
	c.expect("rewrite", "GET", "after")
	if ttl := c.do("TTL", "ttl").Int; ttl < 990 {
		t.Fatalf("ttl lost: %d", ttl)
	}
}

func TestSnapshotPersistence(t *testing.T) {
	dir := t.TempDir()
	mod := func(c *Config) { c.Dir = dir }
	s, addr := startServer(t, mod)
	c := dial(t, addr)
	c.expect("OK", "SET", "str", "hello")
	c.expect("OK", "SET", "ttl", "x", "EX", "1000")
	c.expect("3", "RPUSH", "list", "a", "b", "c")
	c.expect("2", "HSET", "hash", "f", "v", "g", "w")
	c.expect("1", "SADD", "set", "m1")
	c.expect("2", "ZADD", "zset", "1.5", "one", "2", "two")
	c.expect("OK", "SET", "gone", "x", "PX", "1")
	c.expect("10.5", "INCRBYFLOAT", "float", "10.5")
	c.expect("Background saving started", "BGSAVE")
	time.Sleep(200 * time.Millisecond)
	c.expect("OK", "SAVE")
	s.cfg.SaveSeconds = 0
	s.Close()

	// Corruption is detected via CRC.
	path := filepath.Join(dir, "dump.grdb")
	good, _ := os.ReadFile(path)
	bad := append([]byte(nil), good...)
	bad[len(bad)/2] ^= 0xFF
	os.WriteFile(path, bad, 0o644)
	cfg := DefaultConfig()
	cfg.Dir, cfg.Logger = dir, log.New(io.Discard, "", 0)
	if _, err := New(cfg); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("expected checksum error, got %v", err)
	}
	os.WriteFile(path, good, 0o644)

	_, addr = startServer(t, mod)
	c = dial(t, addr)
	verify(t, c)
}

func TestInfoAndConfig(t *testing.T) {
	_, addr := startServer(t, nil)
	c := dial(t, addr)
	c.do("SET", "a", "b")
	info := c.do("INFO", "all").Str
	for _, s := range []string{"# Server", "# Memory", "# Stats", "cmdstat_set:calls=1", "db0:keys=1"} {
		if !strings.Contains(info, s) {
			t.Errorf("INFO missing %q", s)
		}
	}
	c.expect("OK", "CONFIG", "SET", "maxmemory", "10mb")
	c.expect("[maxmemory 10485760]", "CONFIG", "GET", "maxmemory")
	c.expect("OK", "CONFIG", "SET", "slowlog-log-slower-than", "0")
	c.do("PING")
	if n := c.do("SLOWLOG", "LEN").Int; n == 0 {
		t.Fatal("slowlog empty")
	}
	names := []string{}
	for _, cmd := range c.do("COMMAND").Array {
		names = append(names, cmd.Array[0].Str)
	}
	sort.Strings(names)
	if len(names) < 100 {
		t.Fatalf("only %d commands registered", len(names))
	}
}

func TestMonitor(t *testing.T) {
	_, addr := startServer(t, nil)
	m, c := dial(t, addr), dial(t, addr)
	m.expect("OK", "MONITOR")
	c.do("SET", "watched", "1")
	line := m.read().Str
	if !strings.Contains(line, `"SET" "watched" "1"`) {
		t.Fatalf("monitor line: %q", line)
	}
}

func TestDashboardAPI(t *testing.T) {
	s, addr := startServer(t, nil)
	c := dial(t, addr)
	c.do("RPUSH", "l", "a", "b")
	c.do("ZADD", "z", "1", "m")
	if v := s.Exec([][]byte{[]byte("LLEN"), []byte("l")}); v.Int != 2 {
		t.Fatalf("Exec LLEN = %v", v)
	}
	_, keys := s.ScanKeys(0, "*", "", 100)
	if len(keys) != 2 {
		t.Fatalf("ScanKeys = %d", len(keys))
	}
	d, ok := s.KeyDetail("z")
	if !ok || d.Type != "zset" || len(d.Value.([]ZMember)) != 1 {
		t.Fatalf("KeyDetail = %+v", d)
	}
	ch, cancel := s.SubscribePubSub([]string{"n*"})
	defer cancel()
	c.expect("1", "PUBLISH", "news", "hi")
	select {
	case m := <-ch:
		if m.Message != "hi" || m.Pattern != "n*" {
			t.Fatalf("sink got %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatal("no message delivered to sink")
	}
	st := s.Stats(true)
	if st.Keys != 2 || st.TypeCounts["list"] != 1 {
		t.Fatalf("stats: %+v", st)
	}
}
