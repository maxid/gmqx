package gmqx

import (
	"context"
	"sync"
	"testing"
)

type fakeCluster struct {
	mu    sync.Mutex
	nodes map[string]ClusterHandler
}

func newFakeCluster() *fakeCluster { return &fakeCluster{nodes: make(map[string]ClusterHandler)} }

func (f *fakeCluster) Start(_ context.Context, nodeID string, h ClusterHandler) error {
	f.mu.Lock()
	f.nodes[nodeID] = h
	f.mu.Unlock()
	return nil
}
func (f *fakeCluster) Stop(context.Context) error {
	return nil
}
func (f *fakeCluster) Spread(_ context.Context, msg ClusterMessage) error {
	f.mu.Lock()
	nodes := make(map[string]ClusterHandler, len(f.nodes))
	for id, h := range f.nodes {
		nodes[id] = h
	}
	f.mu.Unlock()
	for id, h := range nodes {
		if id == msg.NodeID {
			continue
		}
		if msg.Type == ClusterPublish {
			h.OnRemotePublish(msg)
		}
	}
	return nil
}

func TestClusterCrossNodePublish(t *testing.T) {
	cluster := newFakeCluster()
	a := startBroker(t, Config{Addr: "127.0.0.1:0", NodeID: "a", ClusterBackend: cluster})
	b := startBroker(t, Config{Addr: "127.0.0.1:0", NodeID: "b", ClusterBackend: cluster})

	sub := dialTestClient(t, b.Addr(), "sub-b")
	body := []byte{0, 1}
	body = appendString(body, "cluster/topic")
	body = append(body, 0)
	sub.writePacket(0x82, body)
	if h, p, err := sub.readPacket(); err != nil || h>>4 != 9 || len(p) < 3 || p[2] != 0 {
		t.Fatalf("suback: h=%x p=%v err=%v", h, p, err)
	}

	pub := dialTestClient(t, a.Addr(), "pub-a")
	pubBody := appendString(nil, "cluster/topic")
	pubBody = append(pubBody, []byte("remote")...)
	pub.writePacket(0x30, pubBody)

	h, p, err := sub.readPacket()
	if err != nil {
		t.Fatal(err)
	}
	if h>>4 != 3 {
		t.Fatalf("expected PUBLISH, got type %d", h>>4)
	}
	topic, rest, err := readString(p)
	if err != nil || topic != "cluster/topic" || string(rest) != "remote" {
		t.Fatalf("topic=%q payload=%q err=%v", topic, rest, err)
	}

	_ = a.Stop(context.Background())
	_ = b.Stop(context.Background())
}
