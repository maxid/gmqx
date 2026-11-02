package engine

import (
	"testing"

	"github.com/maxid/gmqx/hook"
)

// The extension contracts are defined in gmqx/hook and re-exported here as aliases. These
// assertions fail to compile the moment one of them becomes a distinct named type, which
// is exactly the compatibility promise this package makes: a consumer that imports gmqx
// and a consumer that imports gmqx/hook hold the same types.
var (
	_ hook.ConnectInfo    = ConnectInfo{}
	_ hook.AuthStep       = AuthStep{}
	_ hook.SessionInfo    = SessionInfo{}
	_ hook.ConnInfo       = ConnInfo{}
	_ hook.PublishEvent   = PublishEvent{}
	_ hook.PublishResult  = PublishResult{}
	_ hook.AuthManager    = (AuthManager)(nil)
	_ hook.AuthExchange   = (AuthExchange)(nil)
	_ hook.AclManager     = (AclManager)(nil)
	_ hook.EventSink      = (EventSink)(nil)
	_ hook.MetricsManager = (MetricsManager)(nil)
	_ hook.Interceptor    = (Interceptor)(nil)

	_ EventSink      = hook.SinkFuncs{}
	_ MetricsManager = hook.NoopMetrics{}
	_ Interceptor    = hook.InterceptFunc(nil)
)

func TestHookConstantsAreAliased(t *testing.T) {
	// Comparing the root constant with the hook one proves they are the same value, and
	// the typed assignments prove they kept their type.
	var status AuthStatus = AuthContinue
	if status != hook.AuthContinue {
		t.Fatalf("AuthContinue = %v, want %v", status, hook.AuthContinue)
	}
	var action AclAction = AclSubscribe
	if action != hook.AclSubscribe {
		t.Fatalf("AclSubscribe = %v, want %v", action, hook.AclSubscribe)
	}
	var disposition Disposition = PublishAccepted
	if disposition != hook.PublishAccepted {
		t.Fatalf("PublishAccepted = %v, want %v", disposition, hook.PublishAccepted)
	}
	if ReasonNotAuthorized != hook.ReasonNotAuthorized {
		t.Fatalf("ReasonNotAuthorized = %#x, want %#x", ReasonNotAuthorized, hook.ReasonNotAuthorized)
	}
}

func TestConnHelpersForwardToHook(t *testing.T) {
	info := ConnInfo{ClientID: "c1", ConnectionID: "conn-1", Namespace: "ns"}
	ctx := ContextWithConn(t.Context(), info)

	got, ok := ConnFromContext(ctx)
	if !ok || got != info {
		t.Fatalf("ConnFromContext = %+v/%v, want %+v/true", got, ok, info)
	}
	// A context built through hook must be readable through the root package, and the
	// other way round.
	viaHook := hook.ContextWithConn(t.Context(), info)
	if got, ok := ConnFromContext(viaHook); !ok || got != info {
		t.Fatalf("root read of a hook context = %+v/%v, want %+v/true", got, ok, info)
	}
	if got, ok := hook.ConnFromContext(ctx); !ok || got != info {
		t.Fatalf("hook read of a root context = %+v/%v, want %+v/true", got, ok, info)
	}
}
