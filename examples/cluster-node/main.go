// Command cluster-node runs one broker node with the built-in TCP cluster
// enabled, so two or more processes on this machine form a cluster and a real MQTT
// client can verify that a publish on one node reaches a subscriber on another.
//
// Start two nodes with the same seed list, then connect a client to each; -h prints
// the two commands. Every accepted connection makes this node publish a retained
// greeting on "<greet>_reply", which the cluster replicates to the peers — so a
// client that subscribes on another node still sees it. Nodes dial only the seeds whose address sorts after their own
// cluster address, so a symmetric seed list yields exactly one connection per pair.
// Authentication is allow-all on purpose: this is a local verification harness,
// never a production deployment.
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
	"sync"
	"syscall"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/hook"
)

func main() {
	node := flag.String("node", "a", "cluster node id; must be unique per process")
	addr := flag.String("addr", "127.0.0.1:1883", "MQTT TCP listen address")
	clusterAddr := flag.String("cluster-addr", "127.0.0.1:7901", "cluster listen address (explicit, never :0)")
	seeds := flag.String("seeds", "", "comma-separated cluster addresses of all nodes, including this one")
	reconnect := flag.Duration("reconnect", time.Second, "cluster reconnect interval")
	verbose := flag.Bool("v", false, "log at debug level")
	greetBase := flag.String("greet", "cluster/hello", `base topic of the greeting this node publishes on every accepted connection; the greeting goes to "<greet>_reply", empty disables it`)

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `cluster-node runs one broker node with the TCP cluster enabled. Start two
nodes in two terminals, then point an MQTT client at each: a publish on one node
reaches the subscriber on the other.

Examples:
  go run ./examples/cluster-node -node a -addr 127.0.0.1:1883 -cluster-addr 127.0.0.1:7901 -seeds 127.0.0.1:7901,127.0.0.1:7902
  go run ./examples/cluster-node -node b -addr 127.0.0.1:1884 -cluster-addr 127.0.0.1:7902 -seeds 127.0.0.1:7901,127.0.0.1:7902
  go run ./examples/cluster-node -node a -v   # same node, debug log

Stop a node with Ctrl-C; it drains and exits 0.

Flags:
`)
		flag.PrintDefaults()
	}
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	greet := &greeter{topic: *greetBase + "_reply"}

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
		Sink:   greet,
		Logger: logger,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker, err := gmqx.New(cfg)
	if err != nil {
		log.Fatalf("cluster-node: create broker: %v", err)
	}
	if err := broker.Start(ctx); err != nil {
		log.Fatalf("cluster-node: start broker: %v", err)
	}
	greet.bind(broker)
	fmt.Printf("node %s\n  mqtt    %s\n  cluster %s\n  seeds   %v\n", *node, broker.Addr(), *clusterAddr, seedsList)
	if *greetBase != "" {
		fmt.Printf("  greeting %s on every connect (retained, replicated to peers)\n", greet.topic)
	}

	<-ctx.Done()
	// Stop takes the cancelled ctx off the table: draining needs a live deadline.
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := broker.Stop(shutdown); err != nil {
		log.Fatalf("cluster-node: stop broker: %v", err)
	}
	fmt.Printf("[stop] node %s stopped\n", *node)
}

// greeter is the node's EventSink: every accepted connection makes this node publish
// a greeting of its own, and the cluster spreads it like any client publication.
//
// The greeting is retained, for two reasons: OnConnect runs before the client has
// subscribed to anything, so a plain publish would reach nobody, and a retained one
// is replicated to the peers — a client that subscribes on another node still finds
// it, which is what makes the cluster visible without a second client.
type greeter struct {
	mu     sync.Mutex
	broker *gmqx.Broker
	topic  string
}

func (g *greeter) bind(b *gmqx.Broker) {
	if g.topic == "" {
		return
	}
	g.mu.Lock()
	g.broker = b
	g.mu.Unlock()
}

func (g *greeter) OnConnect(ctx context.Context, info hook.ConnectInfo) {
	g.mu.Lock()
	b := g.broker
	g.mu.Unlock()
	if b == nil {
		return
	}
	if err := b.PublishMessage(ctx, gmqx.Publication{
		Topic:   g.topic,
		Payload: []byte("hi!"),
		Retain:  true,
	}); err != nil {
		slog.Warn("greeting failed", "client", info.ClientID, "err", err)
	}
}

func (*greeter) OnDisconnect(context.Context, string, error)     {}
func (*greeter) OnConnectionLost(context.Context, string, error) {}
func (*greeter) OnPing(context.Context, string)                  {}
func (*greeter) OnPublish(context.Context, hook.PublishEvent) hook.PublishResult {
	return hook.PublishResult{Disposition: hook.PublishAccepted}
}
