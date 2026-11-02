package cluster

// The optional capabilities a backend asks the handler it was handed at Start
// for. They are separate from ClusterHandler because they are separate roles:
// every handler receives events, but only a handler that knows the local session
// set can advertise a subscription summary, and only one that can receive a
// directed publish can be a target. A backend type-asserts the handler it was
// given and degrades when the assertion fails, so a third-party backend and a
// third-party handler stay compatible without either knowing the other.
//
// These interfaces live here rather than in the root package because the backends
// are what call them (docs/design/source-layout.md §4.1): the root package adapts
// its broker to them, and a backend that moves into this package keeps the
// contract it already implemented.

// TargetHandler is implemented by a ClusterHandler that can receive a directed
// publish (ClusterTarget). It is an optional interface so the handler contract
// stays additive: a backend delivers a target event to a handler that implements
// it, and counts the event as undeliverable otherwise. The broker implements it.
type TargetHandler interface {
	OnRemoteTarget(msg ClusterMessage)
}

// SubscriptionProvider is implemented by a ClusterHandler that can describe the
// topic filters its local sessions are subscribed to. The built-in TCP cluster
// advertises that summary to its peers for directed routing. A handler without
// it never advertises a summary, so peers treat it as unknown and send every
// publish to it (availability over precision).
type SubscriptionProvider interface {
	// SubscriptionFilters returns every topic filter a live local session is
	// subscribed to, sorted and de-duplicated.
	SubscriptionFilters() []string
}

// SharedSubscriptionProvider is implemented by a ClusterHandler that can describe
// the shared subscription groups its local sessions hold a live member of. It is
// the shared half of the advertised summary: a peer needs it to elect one node per
// shared group, so a handler without it leaves shared subscriptions local to the
// node that produced the publication.
type SharedSubscriptionProvider interface {
	// SharedSubscriptionGroups returns the shared subscription group keys this
	// node holds at least one live member of.
	SharedSubscriptionGroups() []string
}
