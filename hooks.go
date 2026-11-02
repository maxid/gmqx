package gmqx

// The extension contracts live in ./hook; the engine that implements the host side
// lives in ./server. Every name below is an alias, so a consumer keeps importing
// this package and still gets the hook type: one definition, two import paths. New
// code may import github.com/maxid/gmqx/hook directly — both spellings are the same
// type, so they mix freely.

import (
	"context"

	"github.com/maxid/gmqx/hook"
	"github.com/maxid/gmqx/server"
)

type (
	ConnectInfo    = hook.ConnectInfo
	AuthManager    = hook.AuthManager
	AuthStatus     = hook.AuthStatus
	AuthStep       = hook.AuthStep
	AuthExchange   = hook.AuthExchange
	AclAction      = hook.AclAction
	SessionInfo    = hook.SessionInfo
	AclManager     = hook.AclManager
	ConnInfo       = hook.ConnInfo
	Disposition    = hook.Disposition
	PublishResult  = hook.PublishResult
	PublishEvent   = hook.PublishEvent
	EventSink      = hook.EventSink
	MetricsManager = hook.MetricsManager
	NoopMetrics    = hook.NoopMetrics
	Interceptor    = hook.Interceptor

	// AllowAll permits every connection/subscription/publish.
	AllowAll = server.AllowAll
	// DenyAll rejects every connection.
	DenyAll = server.DenyAll
)

const (
	// MaxAuthSteps is the number of AUTH challenges one exchange may run.
	MaxAuthSteps = server.MaxAuthSteps

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
