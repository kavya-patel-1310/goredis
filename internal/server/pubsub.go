package server

import (
	"sort"
	"strings"

	"goredis/internal/glob"
	"goredis/internal/resp"
)

// subscriber receives published messages: a network client, or an internal
// sink such as the dashboard's live pub/sub viewer.
type subscriber interface {
	deliver(pattern, channel string, msg []byte)
}

type pubsub struct {
	channels map[string]map[subscriber]struct{}
	patterns map[string]map[subscriber]struct{}
}

func newPubSub() pubsub {
	return pubsub{
		channels: make(map[string]map[subscriber]struct{}),
		patterns: make(map[string]map[subscriber]struct{}),
	}
}

func add(m map[string]map[subscriber]struct{}, k string, s subscriber) bool {
	set := m[k]
	if set == nil {
		set = make(map[subscriber]struct{})
		m[k] = set
	}
	if _, ok := set[s]; ok {
		return false
	}
	set[s] = struct{}{}
	return true
}

func remove(m map[string]map[subscriber]struct{}, k string, s subscriber) {
	if set := m[k]; set != nil {
		delete(set, s)
		if len(set) == 0 {
			delete(m, k)
		}
	}
}

func (p *pubsub) publish(channel string, msg []byte) int {
	n := 0
	for s := range p.channels[channel] {
		s.deliver("", channel, msg)
		n++
	}
	for pat, subs := range p.patterns {
		if glob.Match(pat, channel) {
			for s := range subs {
				s.deliver(pat, channel, msg)
				n++
			}
		}
	}
	return n
}

func (p *pubsub) unsubscribeAll(c *Client) {
	for ch := range c.subs {
		remove(p.channels, ch, c)
	}
	for pat := range c.psubs {
		remove(p.patterns, pat, c)
	}
	c.subs, c.psubs = nil, nil
}

// deliver implements subscriber for network clients.
func (c *Client) deliver(pattern, channel string, msg []byte) {
	var w resp.Writer
	if pattern == "" {
		w.Array(3)
		w.BulkString("message")
	} else {
		w.Array(4)
		w.BulkString("pmessage")
		w.BulkString(pattern)
	}
	w.BulkString(channel)
	w.Bulk(msg)
	c.pushAsync(w.Buf)
}

func (c *Client) subCount() int { return len(c.subs) + len(c.psubs) }

func cmdSubscribe(ctx *Ctx) {
	c := ctx.c
	if c.internal {
		ctx.error("ERR SUBSCRIBE is not supported from this client; use the Pub/Sub tab")
		return
	}
	if c.subs == nil {
		c.subs = make(map[string]struct{})
	}
	for i := 1; i < ctx.nargs(); i++ {
		ch := ctx.arg(i)
		if add(ctx.s.pubsub.channels, ch, c) {
			c.subs[ch] = struct{}{}
		}
		ctx.w.Array(3)
		ctx.w.BulkString("subscribe")
		ctx.w.BulkString(ch)
		ctx.w.Int(int64(c.subCount()))
	}
}

func cmdPSubscribe(ctx *Ctx) {
	c := ctx.c
	if c.internal {
		ctx.error("ERR PSUBSCRIBE is not supported from this client; use the Pub/Sub tab")
		return
	}
	if c.psubs == nil {
		c.psubs = make(map[string]struct{})
	}
	for i := 1; i < ctx.nargs(); i++ {
		p := ctx.arg(i)
		if add(ctx.s.pubsub.patterns, p, c) {
			c.psubs[p] = struct{}{}
		}
		ctx.w.Array(3)
		ctx.w.BulkString("psubscribe")
		ctx.w.BulkString(p)
		ctx.w.Int(int64(c.subCount()))
	}
}

func unsubGeneric(ctx *Ctx, pattern bool) {
	c := ctx.c
	kind, set, table := "unsubscribe", c.subs, ctx.s.pubsub.channels
	if pattern {
		kind, set, table = "punsubscribe", c.psubs, ctx.s.pubsub.patterns
	}
	var targets []string
	if ctx.nargs() > 1 {
		for i := 1; i < ctx.nargs(); i++ {
			targets = append(targets, ctx.arg(i))
		}
	} else {
		for k := range set {
			targets = append(targets, k)
		}
		sort.Strings(targets)
	}
	if len(targets) == 0 {
		ctx.w.Array(3)
		ctx.w.BulkString(kind)
		ctx.w.Null()
		ctx.w.Int(int64(c.subCount()))
		return
	}
	for _, t := range targets {
		remove(table, t, c)
		delete(set, t)
		ctx.w.Array(3)
		ctx.w.BulkString(kind)
		ctx.w.BulkString(t)
		ctx.w.Int(int64(c.subCount()))
	}
}

func cmdUnsubscribe(ctx *Ctx)  { unsubGeneric(ctx, false) }
func cmdPUnsubscribe(ctx *Ctx) { unsubGeneric(ctx, true) }

func cmdPublish(ctx *Ctx) {
	n := ctx.s.pubsub.publish(ctx.arg(1), ctx.args[2])
	ctx.s.stats.pubsubMessages.Add(1)
	ctx.w.Int(int64(n))
}

func cmdPubSub(ctx *Ctx) {
	p := &ctx.s.pubsub
	switch strings.ToUpper(ctx.arg(1)) {
	case "CHANNELS":
		pat := "*"
		if ctx.nargs() > 2 {
			pat = ctx.arg(2)
		}
		var out []string
		for ch := range p.channels {
			if glob.Match(pat, ch) {
				out = append(out, ch)
			}
		}
		sort.Strings(out)
		ctx.w.BulkStrings(out)
	case "NUMSUB":
		ctx.w.Array(2 * (ctx.nargs() - 2))
		for i := 2; i < ctx.nargs(); i++ {
			ctx.w.BulkString(ctx.arg(i))
			ctx.w.Int(int64(len(p.channels[ctx.arg(i)])))
		}
	case "NUMPAT":
		ctx.w.Int(int64(len(p.patterns)))
	default:
		ctx.error("ERR unknown subcommand '" + ctx.arg(1) + "'. Try PUBSUB CHANNELS|NUMSUB|NUMPAT.")
	}
}

// ---------------------------------------------------------------------------
// Transactions

func cmdMulti(ctx *Ctx) {
	if ctx.c.inMulti {
		ctx.error("ERR MULTI calls can not be nested")
		return
	}
	ctx.c.inMulti = true
	ctx.c.multiQueue = nil
	ctx.c.multiErr = false
	ctx.w.OK()
}

func resetMulti(c *Client) {
	c.inMulti = false
	c.multiQueue = nil
	c.multiErr = false
}

func cmdExec(ctx *Ctx) {
	s, c, w := ctx.s, ctx.c, ctx.w
	if !c.inMulti {
		ctx.error("ERR EXEC without MULTI")
		return
	}
	queue := c.multiQueue
	if c.multiErr {
		resetMulti(c)
		s.unwatchAll(c)
		ctx.error("EXECABORT Transaction discarded because of previous errors.")
		return
	}
	if c.dirtyCAS {
		resetMulti(c)
		s.unwatchAll(c)
		w.NullArray()
		return
	}
	resetMulti(c)
	s.unwatchAll(c) // unwatch before executing, like Redis
	c.inExec, c.execProp = true, false
	w.Array(len(queue))
	for _, args := range queue {
		cmd := s.lookupCommand(args[0])
		s.call(c, cmd, args)
	}
	if c.execProp {
		s.propagateFrom(c, bs("EXEC"))
	}
	c.inExec, c.execProp = false, false
}

func cmdDiscard(ctx *Ctx) {
	if !ctx.c.inMulti {
		ctx.error("ERR DISCARD without MULTI")
		return
	}
	resetMulti(ctx.c)
	ctx.s.unwatchAll(ctx.c)
	ctx.w.OK()
}

func cmdWatch(ctx *Ctx) {
	s, c := ctx.s, ctx.c
	if c.inMulti {
		ctx.error("ERR WATCH inside MULTI is not allowed")
		return
	}
	for i := 1; i < ctx.nargs(); i++ {
		k := ctx.arg(i)
		m := s.watched[k]
		if m == nil {
			m = make(map[*Client]struct{})
			s.watched[k] = m
		}
		if _, ok := m[c]; !ok {
			m[c] = struct{}{}
			c.watched = append(c.watched, k)
		}
	}
	ctx.w.OK()
}

func cmdUnwatch(ctx *Ctx) {
	ctx.s.unwatchAll(ctx.c)
	ctx.w.OK()
}
