package hook

import "context"

// AclAction is a topic permission action.
type AclAction int

const (
	AclSubscribe AclAction = iota
	AclPublish
)

// SessionInfo identifies the session for ACL checks.
type SessionInfo struct {
	ClientID string
	Username string
}

// AclManager authorizes subscribe/publish.
type AclManager interface {
	Check(ctx context.Context, session SessionInfo, topic string, action AclAction) bool
	RequiresOffload() bool
}
