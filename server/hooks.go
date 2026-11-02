package server

import (
	"context"

	"github.com/maxid/gnety/codec/mqtt"

	"github.com/maxid/gmqx/hook"
	"github.com/maxid/gmqx/routing"
)

// Publication is one accepted publication the broker routes. It is the payload
// of the routing SPI: QoS and Retain are the values the client published, Dup
// reports a retransmission, and FromClientID/FromConnectionID name the session
// that produced it (both are empty for an event that came from a peer node).
// Props is the MQTT 5 PUBLISH property block without its length prefix, nil for
// MQTT 3.1.1 traffic.
type Publication struct {
	Namespace        string
	Topic            string
	Payload          []byte
	QoS              byte
	Retain           bool
	Dup              bool
	FromClientID     string
	FromConnectionID string
	Props            []byte

	// props is the typed mirror of Props for publications the broker built
	// itself. SendLocal uses it directly instead of decoding Props once per
	// local target; a Publication built by a dispatcher only carries Props.
	props    mqtt.Properties
	propsSet bool
}

// Dispatcher is the single decision point for where an accepted publication
// goes. The broker authorizes, runs every Interceptor and calls the EventSink
// first; only a publication the sink accepted reaches Dispatch. Suppressing it
// here therefore suppresses delivery, not the acknowledgement the sink already
// granted.
//
// A Dispatcher must not re-implement topic matching. It routes through the
// broker's read-only seam instead:
//
//	targets := broker.LocalTargets(pub)        // sessions whose subscriptions match
//	for _, t := range targets {
//	    _ = broker.SendLocal(ctx, t, pub)      // one PUBLISH to one of them
//	}
//	return broker.SpreadPublication(ctx, pub)  // forward to the cluster backend
//
// A dispatcher that needs the seam implements DispatcherBinder; the broker binds
// itself before the first publication is routed. Retained state is not part of
// routing: the broker stores an accepted retained publication before Dispatch, so
// a dispatcher decides where live publications go, not what is retained.
//
// Config.Dispatcher == nil selects DefaultDispatcher, which is the behavior of a
// broker without the SPI: local delivery plus a cluster spread.
type Dispatcher interface {
	Dispatch(ctx context.Context, pub Publication) error
}

// DispatcherBinder is implemented by a Dispatcher that needs the broker's
// routing seam (LocalTargets, SendLocal, SpreadPublication). The broker calls
// Bind once, from New, before any connection can publish.
type DispatcherBinder interface {
	Bind(b *Broker)
}

// AllowAll permits every connection/subscription/publish.
type AllowAll struct{}

func (AllowAll) Authenticate(context.Context, ConnectInfo) error            { return nil }
func (AllowAll) RequiresOffload() bool                                      { return false }
func (AllowAll) Check(context.Context, SessionInfo, string, AclAction) bool { return true }

// DenyAll rejects every connection.
type DenyAll struct{}

func (DenyAll) Authenticate(context.Context, ConnectInfo) error { return ErrUnauthorized }
func (DenyAll) RequiresOffload() bool                           { return false }

// The extension contracts live in ./hook. Every name below is an alias, so a consumer
// keeps importing this package and still gets the hook type: one definition, two import
// paths. New code may import github.com/maxid/gmqx/hook directly — both spellings are the
// same type, so they mix freely.
type (
	// LocalTarget is one local session a publication matches. Its fields are
	// read-only reporting; pass the value back to Broker.SendLocal to write the
	// PUBLISH. A LocalTarget is only valid for the broker that produced it.
	//
	// The type is defined in gmqx/routing (the routing table is what produces it)
	// and re-exported here as an alias, so a Dispatcher keeps naming it the same way.
	LocalTarget = routing.Target

	ConnectInfo    = hook.ConnectInfo
	AuthManager    = hook.AuthManager
	AuthStatus     = hook.AuthStatus
	AuthStep       = hook.AuthStep
	AuthExchange   = hook.AuthExchange
	AclAction      = hook.AclAction
	SessionInfo    = hook.SessionInfo
	AclManager     = hook.AclManager
	ConnectionID   = hook.ConnectionID
	ConnInfo       = hook.ConnInfo
	Disposition    = hook.Disposition
	PublishResult  = hook.PublishResult
	PublishEvent   = hook.PublishEvent
	EventSink      = hook.EventSink
	MetricsManager = hook.MetricsManager
	NoopMetrics    = hook.NoopMetrics
	Interceptor    = hook.Interceptor
)

const (
	AuthContinue  = hook.AuthContinue
	AuthSucceeded = hook.AuthSucceeded
	AuthFailed    = hook.AuthFailed

	AclSubscribe = hook.AclSubscribe
	AclPublish   = hook.AclPublish

	PublishAccepted  = hook.PublishAccepted
	PublishRetryable = hook.PublishRetryable
	PublishRejected  = hook.PublishRejected

	ReasonUnspecified            = hook.ReasonUnspecified
	ReasonProtocolError          = hook.ReasonProtocolError
	ReasonNotAuthorized          = hook.ReasonNotAuthorized
	ReasonBadAuthMethod          = hook.ReasonBadAuthMethod
	ReasonSessionTakenOver       = hook.ReasonSessionTakenOver
	ReasonQuotaExceeded          = hook.ReasonQuotaExceeded
	ReasonConnectionRateExceeded = hook.ReasonConnectionRateExceeded
)

// ContextWithConn forwards to hook.ContextWithConn, so a consumer needs one import.
func ContextWithConn(ctx context.Context, info ConnInfo) context.Context {
	return hook.ContextWithConn(ctx, info)
}

// ConnFromContext forwards to hook.ConnFromContext.
func ConnFromContext(ctx context.Context) (ConnInfo, bool) { return hook.ConnFromContext(ctx) }
