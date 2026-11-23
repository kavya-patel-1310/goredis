
redis-benchmark: 500000 requests, 50 clients, 2 cores

### Pipeline = 1

| Command | GoRedis (req/s) | Redis 7.0.15 (req/s) | Ratio |
|---|---:|---:|---:|
| SET | 99681 | 105219 | 95% |
| GET | 119048 | 115902 | 103% |
| INCR | 115768 | 114995 | 101% |
| LPUSH | 115075 | 109004 | 106% |
| RPOP | 122971 | 113999 | 108% |
| SADD | 113843 | 126454 | 90% |
| HSET | 113662 | 150195 | 76% |
| ZADD | 106022 | 104844 | 101% |
| LPUSH (needed to benchmark LRANGE) | 130276 | 107666 | 121% |
| LRANGE_100 (first 100 elements) | 70353 | 61244 | 115% |

### Pipeline = 16

| Command | GoRedis (req/s) | Redis 7.0.15 (req/s) | Ratio |
|---|---:|---:|---:|
| SET | 560538 | 978474 | 57% |
| GET | 872600 | 1196172 | 73% |
| INCR | 773994 | 1190476 | 65% |
| LPUSH | 801282 | 1075269 | 75% |
| RPOP | 811688 | 978474 | 83% |
| SADD | 768049 | 1131222 | 68% |
| HSET | 775194 | 1333333 | 58% |
| ZADD | 681199 | 965251 | 71% |
| LPUSH (needed to benchmark LRANGE) | 732064 | 1024590 | 71% |
| LRANGE_100 (first 100 elements) | 207641 | 120890 | 172% |

