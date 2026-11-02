package hook_test

import (
	"context"
	"testing"

	"github.com/maxid/gmqx/hook"
)

func TestSinkFuncsSkipsNilFields(t *testing.T) {
	sink := hook.SinkFuncs{}
	ctx := context.Background()

	// Every callback with a nil field must be a no-op, not a panic.
	sink.OnConnect(ctx, hook.ConnectInfo{ClientID: "c1"})
	sink.OnDisconnect(ctx, "c1", nil)
	sink.OnConnectionLost(ctx, "c1", nil)
	sink.OnPing(ctx, "c1")

	// A sink with no publish hook has no opinion: the publication is accepted.
	if result := sink.OnPublish(ctx, hook.PublishEvent{Topic: "t"}); !result.Accepted() {
		t.Fatalf("default verdict = %+v, want accepted", result)
	}
}

func TestSinkFuncsCallsDeclaredFieldsOnly(t *testing.T) {
	var events []string
	sink := hook.SinkFuncs{
		OnConnectFunc: func(_ context.Context, info hook.ConnectInfo) {
			events = append(events, "connect:"+info.ClientID)
		},
		OnPublishFunc: func(_ context.Context, ev hook.PublishEvent) hook.PublishResult {
			events = append(events, "publish:"+ev.Topic)
			return hook.PublishResult{Disposition: hook.PublishRejected, ReasonCode: hook.ReasonNotAuthorized}
		},
	}
	ctx := context.Background()
	sink.OnConnect(ctx, hook.ConnectInfo{ClientID: "c1"})
	sink.OnPing(ctx, "c1") // no field: skipped
	result := sink.OnPublish(ctx, hook.PublishEvent{Topic: "devices/1"})
	if result.Accepted() || result.ReasonCode != hook.ReasonNotAuthorized {
		t.Fatalf("verdict = %+v, want rejected with 0x87", result)
	}
	if len(events) != 2 || events[0] != "connect:c1" || events[1] != "publish:devices/1" {
		t.Fatalf("events = %v, want [connect:c1 publish:devices/1]", events)
	}
}

func TestInterceptFuncImplementsInterceptor(t *testing.T) {
	var interceptor hook.Interceptor = hook.InterceptFunc(func(_ context.Context, clientID, topic string, _ []byte, _ byte) bool {
		return clientID == "allowed"
	})
	ctx := context.Background()
	if !interceptor.OnPublish(ctx, "allowed", "t", nil, 0) {
		t.Fatal("interceptor dropped an allowed publish")
	}
	if interceptor.OnPublish(ctx, "denied", "t", nil, 0) {
		t.Fatal("interceptor kept a denied publish")
	}
}

func TestNoopMetricsAndSinkFuncsSatisfyTheContracts(t *testing.T) {
	var _ hook.MetricsManager = hook.NoopMetrics{}
	var _ hook.EventSink = hook.SinkFuncs{}
	var _ hook.Interceptor = hook.InterceptFunc(nil)
}

// embeddedMetrics is the embed-and-override pattern NoopMetrics exists for: only the
// counter that matters is implemented here.
type embeddedMetrics struct {
	hook.NoopMetrics
	dropped []string
}

func (m *embeddedMetrics) RecordDroppedMessage(reason string) { m.dropped = append(m.dropped, reason) }

func TestEmbeddingNoopMetricsAllowsPartialImplementation(t *testing.T) {
	var metrics hook.MetricsManager = &embeddedMetrics{}
	metrics.RecordDroppedMessage("acl_publish")
	metrics.IncrementConnections()
	if got := metrics.(*embeddedMetrics).dropped; len(got) != 1 || got[0] != "acl_publish" {
		t.Fatalf("dropped = %v, want [acl_publish]", got)
	}
}
