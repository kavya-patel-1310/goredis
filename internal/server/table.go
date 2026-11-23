package server

// buildCommandTable returns the command registry. Each row is:
// name, arity, flags, firstKey, lastKey, step, handler.
func buildCommandTable() map[string]*Command {
	w := fWrite | fDenyOOM
	r := fRead
	cmds := []*Command{
		// connection
		cmd("ping", -1, fFast|fPubSub, 0, 0, 0, cmdPing),
		cmd("echo", 2, fFast, 0, 0, 0, cmdEcho),
		cmd("quit", -1, fFast|fPubSub|fNoAuth, 0, 0, 0, cmdQuit),
		cmd("auth", -2, fFast|fNoAuth|fNoMulti, 0, 0, 0, cmdAuth),
		cmd("hello", -1, fFast|fNoAuth|fNoMulti, 0, 0, 0, cmdHello),
		cmd("select", 2, fFast, 0, 0, 0, cmdSelect),
		cmd("reset", 1, fFast|fNoAuth|fPubSub, 0, 0, 0, cmdReset),
		cmd("client", -2, fAdmin, 0, 0, 0, cmdClient),

		// keyspace
		cmd("del", -2, fWrite, 1, -1, 1, cmdDel),
		cmd("unlink", -2, fWrite, 1, -1, 1, cmdDel),
		cmd("exists", -2, r|fFast, 1, -1, 1, cmdExists),
		cmd("type", 2, r|fFast, 1, 1, 1, cmdType),
		cmd("expire", -3, fWrite|fFast, 1, 1, 1, cmdExpire),
		cmd("pexpire", -3, fWrite|fFast, 1, 1, 1, cmdPExpire),
		cmd("expireat", -3, fWrite|fFast, 1, 1, 1, cmdExpireAt),
		cmd("pexpireat", -3, fWrite|fFast, 1, 1, 1, cmdPExpireAt),
		cmd("ttl", 2, r|fFast, 1, 1, 1, cmdTTL),
		cmd("pttl", 2, r|fFast, 1, 1, 1, cmdPTTL),
		cmd("persist", 2, fWrite|fFast, 1, 1, 1, cmdPersist),
		cmd("keys", 2, r, 0, 0, 0, cmdKeys),
		cmd("scan", -2, r, 0, 0, 0, cmdScan),
		cmd("randomkey", 1, r, 0, 0, 0, cmdRandomKey),
		cmd("rename", 3, fWrite, 1, 2, 1, cmdRename),
		cmd("renamenx", 3, fWrite|fFast, 1, 2, 1, cmdRenameNX),
		cmd("dbsize", 1, r|fFast, 0, 0, 0, cmdDBSize),
		cmd("flushdb", -1, fWrite, 0, 0, 0, cmdFlushAll),
		cmd("flushall", -1, fWrite, 0, 0, 0, cmdFlushAll),
		cmd("memory", -2, r, 0, 0, 0, cmdMemoryUsage),
		cmd("object", -2, r, 0, 0, 0, cmdObject),

		// strings
		cmd("get", 2, r|fFast, 1, 1, 1, cmdGet),
		cmd("set", -3, w, 1, 1, 1, cmdSet),
		cmd("setnx", 3, w|fFast, 1, 1, 1, cmdSetNX),
		cmd("setex", 4, w, 1, 1, 1, cmdSetEX),
		cmd("psetex", 4, w, 1, 1, 1, cmdPSetEX),
		cmd("getset", 3, w|fFast, 1, 1, 1, cmdGetSet),
		cmd("getdel", 2, fWrite|fFast, 1, 1, 1, cmdGetDel),
		cmd("mget", -2, r|fFast, 1, -1, 1, cmdMGet),
		cmd("mset", -3, w, 1, -1, 2, cmdMSet),
		cmd("msetnx", -3, w, 1, -1, 2, cmdMSetNX),
		cmd("incr", 2, w|fFast, 1, 1, 1, cmdIncr),
		cmd("decr", 2, w|fFast, 1, 1, 1, cmdDecr),
		cmd("incrby", 3, w|fFast, 1, 1, 1, cmdIncrBy),
		cmd("decrby", 3, w|fFast, 1, 1, 1, cmdDecrBy),
		cmd("incrbyfloat", 3, w|fFast, 1, 1, 1, cmdIncrByFloat),
		cmd("append", 3, w|fFast, 1, 1, 1, cmdAppend),
		cmd("strlen", 2, r|fFast, 1, 1, 1, cmdStrlen),
		cmd("getrange", 4, r, 1, 1, 1, cmdGetRange),
		cmd("setrange", 4, w, 1, 1, 1, cmdSetRange),

		// lists
		cmd("lpush", -3, w|fFast, 1, 1, 1, cmdLPush),
		cmd("rpush", -3, w|fFast, 1, 1, 1, cmdRPush),
		cmd("lpushx", -3, w|fFast, 1, 1, 1, cmdLPushX),
		cmd("rpushx", -3, w|fFast, 1, 1, 1, cmdRPushX),
		cmd("lpop", -2, fWrite|fFast, 1, 1, 1, cmdLPop),
		cmd("rpop", -2, fWrite|fFast, 1, 1, 1, cmdRPop),
		cmd("blpop", -3, fWrite|fBlocking, 1, -2, 1, cmdBLPop),
		cmd("brpop", -3, fWrite|fBlocking, 1, -2, 1, cmdBRPop),
		cmd("llen", 2, r|fFast, 1, 1, 1, cmdLLen),
		cmd("lrange", 4, r, 1, 1, 1, cmdLRange),
		cmd("lindex", 3, r, 1, 1, 1, cmdLIndex),
		cmd("lset", 4, w, 1, 1, 1, cmdLSet),
		cmd("lrem", 4, fWrite, 1, 1, 1, cmdLRem),
		cmd("ltrim", 4, fWrite, 1, 1, 1, cmdLTrim),
		cmd("linsert", 5, w, 1, 1, 1, cmdLInsert),
		cmd("lpos", 3, r, 1, 1, 1, cmdLPos),
		cmd("rpoplpush", 3, w, 1, 2, 1, cmdRPopLPush),
		cmd("lmove", 5, w, 1, 2, 1, cmdLMove),

		// hashes
		cmd("hset", -4, w|fFast, 1, 1, 1, cmdHSet),
		cmd("hmset", -4, w|fFast, 1, 1, 1, cmdHSet),
		cmd("hsetnx", 4, w|fFast, 1, 1, 1, cmdHSetNX),
		cmd("hget", 3, r|fFast, 1, 1, 1, cmdHGet),
		cmd("hmget", -3, r|fFast, 1, 1, 1, cmdHMGet),
		cmd("hdel", -3, fWrite|fFast, 1, 1, 1, cmdHDel),
		cmd("hlen", 2, r|fFast, 1, 1, 1, cmdHLen),
		cmd("hexists", 3, r|fFast, 1, 1, 1, cmdHExists),
		cmd("hstrlen", 3, r|fFast, 1, 1, 1, cmdHStrlen),
		cmd("hgetall", 2, r, 1, 1, 1, cmdHGetAll),
		cmd("hkeys", 2, r, 1, 1, 1, cmdHKeys),
		cmd("hvals", 2, r, 1, 1, 1, cmdHVals),
		cmd("hincrby", 4, w|fFast, 1, 1, 1, cmdHIncrBy),
		cmd("hincrbyfloat", 4, w|fFast, 1, 1, 1, cmdHIncrByFloat),

		// sets
		cmd("sadd", -3, w|fFast, 1, 1, 1, cmdSAdd),
		cmd("srem", -3, fWrite|fFast, 1, 1, 1, cmdSRem),
		cmd("sismember", 3, r|fFast, 1, 1, 1, cmdSIsMember),
		cmd("smismember", -3, r|fFast, 1, 1, 1, cmdSMIsMember),
		cmd("smembers", 2, r, 1, 1, 1, cmdSMembers),
		cmd("scard", 2, r|fFast, 1, 1, 1, cmdSCard),
		cmd("spop", -2, fWrite|fFast, 1, 1, 1, cmdSPop),
		cmd("srandmember", -2, r, 1, 1, 1, cmdSRandMember),
		cmd("sinter", -2, r, 1, -1, 1, cmdSInter),
		cmd("sunion", -2, r, 1, -1, 1, cmdSUnion),
		cmd("sdiff", -2, r, 1, -1, 1, cmdSDiff),
		cmd("sinterstore", -3, w, 1, -1, 1, cmdSInterStore),
		cmd("sunionstore", -3, w, 1, -1, 1, cmdSUnionStore),
		cmd("sdiffstore", -3, w, 1, -1, 1, cmdSDiffStore),
		cmd("smove", 4, fWrite|fFast, 1, 2, 1, cmdSMove),

		// sorted sets
		cmd("zadd", -4, w|fFast, 1, 1, 1, cmdZAdd),
		cmd("zincrby", 4, w|fFast, 1, 1, 1, cmdZIncrBy),
		cmd("zrem", -3, fWrite|fFast, 1, 1, 1, cmdZRem),
		cmd("zscore", 3, r|fFast, 1, 1, 1, cmdZScore),
		cmd("zmscore", -3, r|fFast, 1, 1, 1, cmdZMScore),
		cmd("zcard", 2, r|fFast, 1, 1, 1, cmdZCard),
		cmd("zcount", 4, r|fFast, 1, 1, 1, cmdZCount),
		cmd("zrank", 3, r|fFast, 1, 1, 1, cmdZRank),
		cmd("zrevrank", 3, r|fFast, 1, 1, 1, cmdZRevRank),
		cmd("zrange", -4, r, 1, 1, 1, cmdZRange),
		cmd("zrevrange", -4, r, 1, 1, 1, cmdZRevRange),
		cmd("zrangebyscore", -4, r, 1, 1, 1, cmdZRangeByScore),
		cmd("zrevrangebyscore", -4, r, 1, 1, 1, cmdZRevRangeByScore),
		cmd("zpopmin", -2, fWrite|fFast, 1, 1, 1, cmdZPopMin),
		cmd("zpopmax", -2, fWrite|fFast, 1, 1, 1, cmdZPopMax),
		cmd("zremrangebyrank", 4, fWrite, 1, 1, 1, cmdZRemRangeByRank),
		cmd("zremrangebyscore", 4, fWrite, 1, 1, 1, cmdZRemRangeByScore),

		// pub/sub
		cmd("subscribe", -2, fPubSub|fNoMulti, 0, 0, 0, cmdSubscribe),
		cmd("psubscribe", -2, fPubSub|fNoMulti, 0, 0, 0, cmdPSubscribe),
		cmd("unsubscribe", -1, fPubSub|fNoMulti, 0, 0, 0, cmdUnsubscribe),
		cmd("punsubscribe", -1, fPubSub|fNoMulti, 0, 0, 0, cmdPUnsubscribe),
		cmd("publish", 3, fPubSub|fFast, 0, 0, 0, cmdPublish),
		cmd("pubsub", -2, fPubSub, 0, 0, 0, cmdPubSub),

		// transactions
		cmd("multi", 1, fFast|fNoMulti, 0, 0, 0, cmdMulti),
		cmd("exec", 1, fNoMulti, 0, 0, 0, cmdExec),
		cmd("discard", 1, fFast|fNoMulti, 0, 0, 0, cmdDiscard),
		cmd("watch", -2, fFast|fNoMulti, 1, -1, 1, cmdWatch),
		cmd("unwatch", 1, fFast|fNoMulti, 0, 0, 0, cmdUnwatch),

		// server
		cmd("info", -1, 0, 0, 0, 0, cmdInfo),
		cmd("config", -2, fAdmin, 0, 0, 0, cmdConfig),
		cmd("command", -1, 0, 0, 0, 0, cmdCommand),
		cmd("time", 1, fFast, 0, 0, 0, cmdTime),
		cmd("slowlog", -2, fAdmin, 0, 0, 0, cmdSlowlog),
		cmd("monitor", 1, fAdmin|fNoMulti, 0, 0, 0, cmdMonitor),
		cmd("save", 1, fAdmin|fNoMulti, 0, 0, 0, cmdSave),
		cmd("bgsave", -1, fAdmin, 0, 0, 0, cmdBgSave),
		cmd("bgrewriteaof", 1, fAdmin|fNoMulti, 0, 0, 0, cmdBgRewriteAOF),
		cmd("lastsave", 1, fFast, 0, 0, 0, cmdLastSave),
		cmd("shutdown", -1, fAdmin|fNoMulti, 0, 0, 0, cmdShutdown),
		cmd("debug", -2, fAdmin, 0, 0, 0, cmdDebug),
	}
	m := make(map[string]*Command, len(cmds))
	for _, c := range cmds {
		m[c.Name] = c
	}
	// DEBUG POPULATE writes keys directly and propagates them itself.
	return m
}

func cmd(name string, arity, flags, first, last, step int, h func(*Ctx)) *Command {
	return &Command{Name: name, Arity: arity, Flags: flags, FirstKey: first, LastKey: last, Step: step, Handler: h}
}
