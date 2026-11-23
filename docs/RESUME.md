# Putting GoRedis on your resume

## Project entry (pick one version)

**Version A — systems / backend roles (recommended)**

> **GoRedis — Redis-compatible in-memory database** · Go, TCP, RESP, Docker, GitHub Actions · [github.com/kavya-patel-1310/goredis](https://github.com/kavya-patel-1310/goredis)
> - Built a Redis-compatible key-value server from scratch in Go (0 dependencies) implementing **135 commands** across strings, lists, hashes, sets and sorted sets; works unchanged with `redis-cli`, `redis-benchmark` and standard client libraries
> - Matched Redis 7 throughput on non-pipelined workloads (**~100–120K ops/s**) and reached **870K ops/s** pipelined by batching command execution under a single lock and coalescing replies into one syscall, guided by `pprof` profiling
> - Implemented durability with an **append-only log** (3 fsync policies, background compaction, crash-truncation recovery) and **CRC-verified binary snapshots**, rewriting non-deterministic commands so log replay is exact
> - Wrote a **skiplist with span-based ranking** for O(log n) sorted-set queries, **approximated-LRU eviction** and Redis' **adaptive TTL expiry**, plus transactions with optimistic locking (`WATCH`), pub/sub and blocking queues (`BLPOP`)
> - Shipped a real-time **web dashboard** (SSE, canvas charts, key browser, live console) and a Prometheus `/metrics` endpoint; covered by 38 test suites incl. property-based and fuzz tests, race-detector-clean in CI

**Version B — compact (3 bullets)**

> **GoRedis** — Redis-compatible database in Go · [link]
> - Implemented 135 Redis commands, RESP protocol, AOF + snapshot persistence, LRU eviction, TTL expiry, transactions, pub/sub and blocking queues with zero dependencies
> - Achieved parity with Redis 7 (~110K ops/s) on standard benchmarks; 870K ops/s pipelined after profiling-driven optimization
> - Built a live monitoring dashboard (SSE + canvas) and Prometheus metrics; 38 test suites incl. fuzzing and property tests in CI

**Version C — one line (for a "Projects" list)**

> GoRedis: Redis-compatible in-memory DB in Go — 135 commands, AOF/snapshot persistence, LRU eviction, pub/sub, live web dashboard; matches Redis 7 throughput.

Tips:
- Make sure the repo README renders on GitHub (screenshots are in `docs/screenshots`).
- Pin the repo on your GitHub profile. Recruiters click the link; the README + dashboard screenshot is what they see in the first 10 seconds.
- Re-run `scripts/bench.sh` on your own machine and quote *your* numbers — you need to be able to reproduce anything you put on the resume.

## Interview talking points

Have a 60-second story for each of these. They're the questions this project invites.

**"Why one mutex instead of fine-grained locking?"**
Redis' guarantees — atomic commands, isolated `MULTI/EXEC`, blocking-pop ordering — come for free with a serialized executor. The expensive parallel part (socket I/O and parsing) still runs on all cores in per-connection goroutines. Same split as Redis 6 I/O threads. The cost is that one slow command blocks everyone, which is also true of Redis.

**"How does your SCAN guarantee not to miss keys?"**
Keys are stored in a map plus a dense slice. The cursor walks the slice backwards; deletion swaps the last (already visited) element into the hole, so nothing unvisited ever moves behind the cursor. The slice also gives O(1) random sampling for eviction.

**"How does LRU eviction work without a linked list?"**
Approximated LRU, as in Redis: each key stores a last-access timestamp; when over the limit, sample N random keys and evict the oldest. No per-key pointer overhead, and with 5–10 samples it's close to true LRU.

**"What happens if the server crashes in the middle of writing the AOF?"**
On startup the replay detects an incomplete last command, truncates the file to the last complete one and continues. Replies are only sent after the command reaches the OS, so the client never got an OK for the lost write (with `appendfsync always`, after it reaches disk).

**"Why rewrite commands before logging them?"**
Replay must produce the same state. Relative TTLs become absolute timestamps, `SPOP` becomes `SREM` of the members actually chosen, float increments become `SET` of the result, evictions become `DEL`.

**"How do you rank in a sorted set in O(log n)?"**
Skiplist forward pointers store spans (how many nodes they skip). Sum the spans along the search path to get the rank. Paired with a hash map for O(1) score lookups.

**"Tell me about a bug you found."**
After adding pipelined batching, a test hung: an empty request (`*0\r\n`) was reported as "complete" by the batch-detection code, but the parser skips empty requests and then blocked reading the socket while replies for earlier commands were still unflushed — a client/server deadlock. Fixed by making the detector skip empty requests exactly like the parser, with a regression test.

**"How did you optimize performance?"**
Profiled under `redis-benchmark` with `pprof`. Found the clock (`time.Now`, 11% of CPU on that VM), per-command locking, and per-argument allocations. Batched pipelined commands under one lock acquisition, used one buffer for all arguments of a command, halved the clock reads. Also replies are flushed exactly when the reader would block, so a pipeline of 16 commands costs one `write(2)`.

**"What would you do next?"**
Replication (`PSYNC` with a backlog), copy-on-write snapshots to avoid the O(n) copy under the lock, RESP3, streams.
