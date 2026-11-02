package hook

// Host is the live broker surface a plugin attaches to.
//
// It is what gplugin.Registry[Host] hands to every plugin's Start, and it is
// deliberately the only thing a plugin needs from the broker: an application-side
// plugin (a batching sink, an ACL backed by a database, a codec that transcodes a
// payload) installs itself through these slots and removes itself in Stop, while
// the broker keeps serving. The slots are safe to change at run time: every call
// on the publish path reads one consistent snapshot.
//
// A plugin must not block in Start: it runs before the control call returns.
type Host interface {
	// SetAuthManager replaces the CONNECT authentication hook.
	SetAuthManager(AuthManager)
	// SetAclManager replaces the authorization hook.
	SetAclManager(AclManager)
	// SetEventSink replaces the lifecycle/publication sink.
	SetEventSink(EventSink)
	// SetMetricsManager replaces the metrics hook.
	SetMetricsManager(MetricsManager)
	// AddInterceptor appends an interceptor and returns the function that removes
	// it. Removing it twice is safe.
	AddInterceptor(Interceptor) (remove func())
	// AddTranscoder appends a payload transcoder and returns the function that
	// removes it.
	AddTranscoder(Transcoder) (remove func())
	// Codecs is the registry of payload encodings this process understands. A
	// plugin registers its codecs here and unregisters them in Stop.
	Codecs() *CodecSet
}
