package gmqx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maxid/gnety"
)

// TCPClusterConfig configures the built-in TCP cluster backend.
//
// Every node listens on Addr and dials peers whose address sorts after its own,
// so configuring all peers as Seeds on every node yields exactly one connection
// per pair. Addr must be explicit (no ":0") and must differ from the MQTT
// listener address.
type TCPClusterConfig struct {
	Addr              string
	Seeds             []string
	ReconnectInterval time.Duration
}

// NewTCPCluster creates the built-in TCP cluster backend: JSON frames over
// gnety's length-prefixed TCP transport.
func NewTCPCluster(cfg TCPClusterConfig) Cluster {
	return &tcpCluster{cfg: cfg, peers: map[string]*clusterPeer{}}
}

type clusterPeer struct {
	send func(any) error
	stop func() error
}

type tcpCluster struct {
	cfg    TCPClusterConfig
	nodeID string
	ctx    context.Context
	cancel context.CancelFunc

	handler ClusterHandler
	server  *gnety.Server

	mu    sync.RWMutex
	peers map[string]*clusterPeer

	wg sync.WaitGroup
}

// Start implements Cluster.
func (c *tcpCluster) Start(ctx context.Context, nodeID string, handler ClusterHandler) error {
	if nodeID == "" {
		return errors.New("gmqx: cluster node id is required")
	}
	if c.cfg.Addr == "" {
		return errors.New("gmqx: cluster Addr is required")
	}
	if strings.HasSuffix(c.cfg.Addr, ":0") {
		return fmt.Errorf("gmqx: cluster Addr must be explicit, got %q", c.cfg.Addr)
	}
	c.nodeID, c.handler = nodeID, handler
	c.ctx, c.cancel = context.WithCancel(ctx)

	srv, err := gnety.New(gnety.Config{
		Network: "tcp",
		Addr:    c.cfg.Addr,
		Codec:   gnety.LengthFieldCodec{},
		Handler: gnety.HandlerFuncs{
			Open:    c.onOpen,
			Message: c.onMessage,
			Close:   c.onClose,
		},
	})
	if err != nil {
		return err
	}
	if err := srv.Start(c.ctx); err != nil {
		return fmt.Errorf("gmqx: cluster listen %s: %w", c.cfg.Addr, err)
	}
	c.server = srv

	for _, seed := range c.cfg.Seeds {
		seed = strings.TrimSpace(seed)
		// The node with the lower address dials; symmetric seed lists therefore
		// produce one connection per pair without duplicate delivery.
		if seed == "" || seed == c.cfg.Addr || seed < c.cfg.Addr {
			continue
		}
		c.wg.Add(1)
		go c.dialLoop(seed)
	}
	return nil
}

// Stop implements Cluster.
func (c *tcpCluster) Stop(ctx context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	c.mu.Lock()
	for _, p := range c.peers {
		_ = p.stop()
	}
	c.peers = map[string]*clusterPeer{}
	c.mu.Unlock()

	var err error
	if c.server != nil {
		err = c.server.Stop(ctx)
	}
	c.wg.Wait()
	return err
}

// Spread implements Cluster. Delivery is best-effort: a failed peer is dropped
// and its dial loop reconnects.
func (c *tcpCluster) Spread(_ context.Context, msg ClusterMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	type entry struct {
		key  string
		peer *clusterPeer
	}
	c.mu.RLock()
	entries := make([]entry, 0, len(c.peers))
	for k, p := range c.peers {
		entries = append(entries, entry{key: k, peer: p})
	}
	c.mu.RUnlock()

	for _, e := range entries {
		if err := e.peer.send(data); err != nil {
			_ = e.peer.stop()
			c.removePeer(e.key)
		}
	}
	return nil
}

func (c *tcpCluster) onOpen(s *gnety.Session) {
	c.mu.Lock()
	c.peers[fmt.Sprintf("in:%p", s)] = &clusterPeer{send: s.Send, stop: s.Close}
	c.mu.Unlock()
}

func (c *tcpCluster) onClose(s *gnety.Session, _ error) {
	c.removePeer(fmt.Sprintf("in:%p", s))
}

func (c *tcpCluster) onMessage(_ *gnety.Session, payload any) {
	if raw, ok := payload.([]byte); ok {
		c.dispatch(raw)
	}
}

func (c *tcpCluster) dispatch(raw []byte) {
	msg, ok := decodeClusterMessage(raw)
	if !ok || msg.NodeID == c.nodeID || c.handler == nil {
		return
	}
	switch msg.Type {
	case ClusterPublish:
		c.handler.OnRemotePublish(msg)
	case ClusterKick:
		c.handler.OnRemoteKick(msg)
	}
}

func (c *tcpCluster) dialLoop(seed string) {
	defer c.wg.Done()
	interval := c.cfg.ReconnectInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	for {
		if c.ctx.Err() != nil {
			return
		}
		client, err := gnety.Dial(c.ctx, gnety.ClientConfig{
			Network: "tcp",
			Addr:    seed,
			Codec:   gnety.LengthFieldCodec{},
			Timeout: 2 * time.Second,
		})
		if err != nil {
			if !sleepCtx(c.ctx, interval) {
				return
			}
			continue
		}
		key := "out:" + seed
		c.mu.Lock()
		c.peers[key] = &clusterPeer{send: client.Send, stop: client.Close}
		c.mu.Unlock()

		// The connection is bidirectional: remote spreads arrive on this client.
		for {
			payload, rerr := client.Receive(c.ctx)
			if rerr != nil {
				break
			}
			if raw, ok := payload.([]byte); ok {
				c.dispatch(raw)
			}
		}
		c.removePeer(key)
		_ = client.Close()
		if !sleepCtx(c.ctx, interval) {
			return
		}
	}
}

func (c *tcpCluster) removePeer(key string) {
	c.mu.Lock()
	delete(c.peers, key)
	c.mu.Unlock()
}

// peerCount reports the number of registered peers (used by tests).
func (c *tcpCluster) peerCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.peers)
}

func decodeClusterMessage(raw []byte) (ClusterMessage, bool) {
	var msg ClusterMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return ClusterMessage{}, false
	}
	if msg.Type != ClusterPublish && msg.Type != ClusterKick {
		return ClusterMessage{}, false
	}
	return msg, true
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
