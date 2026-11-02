package conn

// A Host for the cases that exercise the connection state machine on its own.
// The broker-level suite (gmqx/server) runs the same code through a real broker;
// these cases need a connection with no server behind it, so they implement the
// handful of Host methods the path under test calls and let the rest fail loudly
// if a case starts depending on them.

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/maxid/gnety/codec/mqtt"

	"github.com/maxid/gmqx/hook"
	"github.com/maxid/gmqx/routing"
	"github.com/maxid/gmqx/session"
)

// testHost is the smallest Host a case can run a connection against.
type testHost struct {
	limits  Limits
	metrics hook.MetricsManager
}

func (h *testHost) Namespace() string     { return "" }
func (h *testHost) Second() time.Duration { return time.Second }
func (h *testHost) Limits() Limits        { return h.limits }
func (h *testHost) Metrics() hook.MetricsManager {
	if h.metrics != nil {
		return h.metrics
	}
	return hook.NoopMetrics{}
}
func (h *testHost) Sink() hook.EventSink              { return nil }
func (h *testHost) Logger() *slog.Logger              { return nil }
func (h *testHost) LifecycleContext() context.Context { return context.Background() }

func (h *testHost) AuthExchange(string) hook.AuthExchange { return nil }
func (h *testHost) Authenticate(context.Context, hook.ConnectInfo) error {
	return ErrUnauthorized
}
func (h *testHost) Register(*Client) error { return nil }
func (h *testHost) Unregister(*Client)     {}
func (h *testHost) Publish(Origin, hook.PublishEvent, mqtt.Properties) hook.PublishResult {
	return hook.PublishResult{Disposition: hook.PublishAccepted}
}
func (h *testHost) Subscribe(*Client, string, session.Subscription) (byte, bool) {
	return 0, false
}
func (h *testHost) Unsubscribe(*Client, string)                        {}
func (h *testHost) SendRetained(*Client, string, byte, bool, []uint32) {}
func (h *testHost) ReceiveMaxFull(string)                              {}
func (h *testHost) EnqueueOffline(routing.Target, Message)             {}
func (h *testHost) ScheduleWill(*Client, time.Duration, hook.PublishEvent, mqtt.Properties) {
}

// recordingMetrics counts what the connection reports, so a case can assert the
// drop/overflow a path is expected to count instead of asserting nothing.
type recordingMetrics struct {
	hook.NoopMetrics
	mu        sync.Mutex
	overflows map[string]int
	dropped   map[string]int
}

func newRecordingMetrics() *recordingMetrics {
	return &recordingMetrics{overflows: make(map[string]int), dropped: make(map[string]int)}
}

func (m *recordingMetrics) RecordOverflow(sink string) {
	m.mu.Lock()
	m.overflows[sink]++
	m.mu.Unlock()
}

func (m *recordingMetrics) RecordDroppedMessage(reason string) {
	m.mu.Lock()
	m.dropped[reason]++
	m.mu.Unlock()
}

func (m *recordingMetrics) overflowsFor(sink string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.overflows[sink]
}
