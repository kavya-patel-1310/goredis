// Command goredis runs the GoRedis server and its web dashboard.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"goredis/internal/dashboard"
	"goredis/internal/server"
)

const banner = `
   ____       ____          _ _
  / ___| ___ |  _ \ ___  __| (_)___
 | |  _ / _ \| |_) / _ \/ _' | / __|   GoRedis %s
 | |_| | (_) |  _ <  __/ (_| | \__ \   RESP2 · port %s · dashboard %s
  \____|\___/|_| \_\___|\__,_|_|___/   pid %d
`

func main() {
	cfg := server.DefaultConfig()
	var (
		port     = flag.Int("port", 6379, "TCP port for the RESP server")
		bind     = flag.String("bind", "0.0.0.0", "address to bind the RESP server")
		dashAddr = flag.String("dashboard", "127.0.0.1:8080", "dashboard listen address (empty to disable)")
		maxmem   = flag.String("maxmemory", "0", "memory limit, e.g. 100mb, 1gb (0 = unlimited)")
		save     = flag.String("save", "60 1000", `snapshot rule "<seconds> <changes>" ("" to disable)`)
		version  = flag.Bool("version", false, "print version and exit")
	)
	flag.StringVar(&cfg.Dir, "dir", cfg.Dir, "working directory for AOF and snapshot files")
	flag.BoolVar(&cfg.AppendOnly, "appendonly", false, "enable the append-only file")
	flag.StringVar(&cfg.AppendFsync, "appendfsync", cfg.AppendFsync, "AOF fsync policy: always|everysec|no")
	flag.StringVar(&cfg.MaxMemoryPolicy, "maxmemory-policy", cfg.MaxMemoryPolicy,
		"noeviction|allkeys-lru|volatile-lru|allkeys-random|volatile-random|volatile-ttl")
	flag.IntVar(&cfg.MaxMemorySamples, "maxmemory-samples", cfg.MaxMemorySamples, "keys sampled per eviction")
	flag.StringVar(&cfg.RequirePass, "requirepass", "", "password required for clients and the dashboard")
	flag.Int64Var(&cfg.SlowlogSlowerUs, "slowlog-log-slower-than", cfg.SlowlogSlowerUs, "slowlog threshold in microseconds")
	flag.IntVar(&cfg.MaxClients, "maxclients", cfg.MaxClients, "max simultaneous clients")
	flag.Parse()

	if *version {
		fmt.Println("goredis", server.Version)
		return
	}
	cfg.Addr = fmt.Sprintf("%s:%d", *bind, *port)
	var err error
	if cfg.MaxMemory, err = server.ParseMemory(*maxmem); err != nil {
		log.Fatalf("invalid -maxmemory: %v", err)
	}
	if !server.ValidPolicy(cfg.MaxMemoryPolicy) {
		log.Fatalf("invalid -maxmemory-policy %q", cfg.MaxMemoryPolicy)
	}
	if *save == "" {
		cfg.SaveSeconds = 0
	} else if _, err := fmt.Sscanf(*save, "%d %d", &cfg.SaveSeconds, &cfg.SaveChanges); err != nil {
		log.Fatalf("invalid -save %q", *save)
	}

	dashLabel := *dashAddr
	if dashLabel == "" {
		dashLabel = "off"
	} else {
		dashLabel = "http://" + dashLabel
	}
	fmt.Printf(banner, server.Version, fmt.Sprint(*port), dashLabel, os.Getpid())

	srv, err := server.New(cfg)
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
	if *dashAddr != "" {
		go func() {
			if err := dashboard.ListenAndServe(*dashAddr, srv); err != nil {
				log.Printf("dashboard: %v", err)
			}
		}()
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case s := <-sig:
			log.Printf("Received %v, scheduling shutdown...", s)
			srv.Close()
		case <-srv.Done():
		}
	}()

	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("fatal: %v", err)
	}
	<-srv.Done()
	srv.Close() // waits for the graceful shutdown to finish
}
