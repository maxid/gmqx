// Command cluster-node runs one broker node with the built-in TCP cluster
// enabled, so two or more processes on this machine form a cluster and a real MQTT
// client can verify that a publish on one node reaches a subscriber on another.
//
// Example: start two nodes with the same seed list and connect a client to each.
//
//	go run ./examples/cluster-node -node a -addr 127.0.0.1:1883 -cluster-addr 127.0.0.1:7901 -seeds 127.0.0.1:7901,127.0.0.1:7902
//	go run ./examples/cluster-node -node b -addr 127.0.0.1:1884 -cluster-addr 127.0.0.1:7902 -seeds 127.0.0.1:7901,127.0.0.1:7902
//
// Nodes dial only the seeds whose address sorts after their own cluster address,
// so a symmetric seed list yields exactly one connection per pair. Authentication
// is allow-all on purpose: this is a local verification harness, never a
// production deployment.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/maxid/gmqx"
)

func main() {
	node := flag.String("node", "a", "cluster node id; must be unique per process")
	addr := flag.String("addr", "127.0.0.1:1883", "MQTT TCP listen address")
	clusterAddr := flag.String("cluster-addr", "127.0.0.1:7901", "cluster listen address (explicit, never :0)")
	seeds := flag.String("seeds", "", "comma-separated cluster addresses of all nodes, including this one")
	reconnect := flag.Duration("reconnect", time.Second, "cluster reconnect interval")
	verbose := flag.Bool("v", false, "log at debug level")
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	var seedsList []string
	for _, s := range strings.Split(*seeds, ",") {
		if s = strings.TrimSpace(s); s != "" {
			seedsList = append(seedsList, s)
		}
	}

	cfg := gmqx.Config{
		Addr: *addr,
		// Local verification only: no credentials, no topic restrictions.
		Auth: gmqx.AllowAll{},
		ACL:  gmqx.AllowAll{},
		Cluster: gmqx.ClusterConfig{
			Enabled: true,
			Node:    *node,
		},
		ClusterBackend: gmqx.NewTCPCluster(gmqx.TCPClusterConfig{
			Addr:              *clusterAddr,
			Seeds:             seedsList,
			ReconnectInterval: *reconnect,
			Logger:            logger,
		}),
		Logger: logger,
	}
	broker, err := gmqx.New(cfg)
	if err != nil {
		log.Fatalf("gmqx: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := broker.Start(ctx); err != nil {
		log.Fatalf("gmqx: start: %v", err)
	}
	fmt.Printf("node %s\n  mqtt    %s\n  cluster %s\n  seeds   %v\n", *node, broker.Addr(), *clusterAddr, seedsList)

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := broker.Stop(shutdown); err != nil {
		log.Printf("gmqx: stop: %v", err)
	}
}
