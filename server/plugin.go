package server

import (
	"github.com/maxid/gplugin"

	"github.com/maxid/gmqx/hook"
)

// extensions is the mutable half of a broker's configuration: the hooks an
// application can swap while the broker runs. It is immutable once stored, so a
// publish reads one consistent snapshot instead of a field that another goroutine
// is rewriting; a setter clones the snapshot, changes it and swaps it.
//
// Everything that is not here stays configuration: a listener address, a limit or a
// cluster backend cannot be swapped after Start, and pretending otherwise would
// make the broker's behaviour depend on call order.
type extensions struct {
	auth         hook.AuthManager
	acl          hook.AclManager
	sink         hook.EventSink
	metrics      hook.MetricsManager
	interceptors []*interceptorSlot
	transcoders  []*transcoderSlot
}

// interceptorSlot and transcoderSlot wrap a hook so Add* can hand back a remove
// function: slots are compared by pointer, because a hook's dynamic type may be a
// function type and interfaces holding functions are not comparable.
type interceptorSlot struct{ fn hook.Interceptor }

type transcoderSlot struct{ fn hook.Transcoder }

// interceptorSlots wraps the configured interceptors into slots, so a
// configuration-time interceptor and one a plugin adds later behave the same.
func interceptorSlots(interceptors []Interceptor) []*interceptorSlot {
	if len(interceptors) == 0 {
		return nil
	}
	slots := make([]*interceptorSlot, 0, len(interceptors))
	for _, interceptor := range interceptors {
		slots = append(slots, &interceptorSlot{fn: interceptor})
	}
	return slots
}

// exts returns the current extension snapshot.
func (b *Broker) exts() *extensions { return b.ext.Load() }

// metrics returns the metrics hook of the current snapshot.
func (b *Broker) metrics() hook.MetricsManager { return b.exts().metrics }

// sink returns the event sink of the current snapshot.
func (b *Broker) sink() hook.EventSink { return b.exts().sink }

// acl returns the authorization hook of the current snapshot.
func (b *Broker) acl() hook.AclManager { return b.exts().acl }

// auth returns the authentication hook of the current snapshot.
func (b *Broker) auth() hook.AuthManager { return b.exts().auth }

// mutateExtensions applies change to a clone of the current snapshot and publishes
// it. Setters go through here so a half-written snapshot is never visible.
func (b *Broker) mutateExtensions(change func(*extensions)) {
	b.extMu.Lock()
	defer b.extMu.Unlock()
	current := b.ext.Load()
	next := *current
	next.interceptors = append([]*interceptorSlot(nil), current.interceptors...)
	next.transcoders = append([]*transcoderSlot(nil), current.transcoders...)
	change(&next)
	b.ext.Store(&next)
}

// SetMetricsManager replaces the metrics hook of a running broker. It implements
// hook.Host, so a plugin can install one; an application that swaps metrics while
// connections are live is expected to keep both generations working.
func (b *Broker) SetMetricsManager(metrics hook.MetricsManager) {
	b.mutateExtensions(func(e *extensions) { e.metrics = metrics })
}

// SetEventSink replaces the lifecycle/publication sink of a running broker.
func (b *Broker) SetEventSink(sink hook.EventSink) {
	b.mutateExtensions(func(e *extensions) { e.sink = sink })
}

// SetAclManager replaces the authorization hook of a running broker.
func (b *Broker) SetAclManager(acl hook.AclManager) {
	b.mutateExtensions(func(e *extensions) { e.acl = acl })
}

// SetAuthManager replaces the CONNECT authentication hook of a running broker.
func (b *Broker) SetAuthManager(auth hook.AuthManager) {
	b.mutateExtensions(func(e *extensions) { e.auth = auth })
}

// AddInterceptor appends an interceptor and returns the function that removes it.
// Interceptors run in registration order; the returned function is safe to call
// twice.
func (b *Broker) AddInterceptor(interceptor hook.Interceptor) (remove func()) {
	slot := &interceptorSlot{fn: interceptor}
	b.mutateExtensions(func(e *extensions) { e.interceptors = append(e.interceptors, slot) })
	removed := false
	return func() {
		if removed {
			return
		}
		removed = true
		b.mutateExtensions(func(e *extensions) {
			kept := e.interceptors[:0]
			for _, existing := range e.interceptors {
				if existing != slot {
					kept = append(kept, existing)
				}
			}
			e.interceptors = kept
		})
	}
}

// AddTranscoder appends a payload transcoder and returns the function that removes
// it. Transcoders run in registration order, after the sink accepted a publication
// and before it is retained or routed.
func (b *Broker) AddTranscoder(transcoder hook.Transcoder) (remove func()) {
	slot := &transcoderSlot{fn: transcoder}
	b.mutateExtensions(func(e *extensions) { e.transcoders = append(e.transcoders, slot) })
	removed := false
	return func() {
		if removed {
			return
		}
		removed = true
		b.mutateExtensions(func(e *extensions) {
			kept := e.transcoders[:0]
			for _, existing := range e.transcoders {
				if existing != slot {
					kept = append(kept, existing)
				}
			}
			e.transcoders = kept
		})
	}
}

// Codecs returns the payload encodings this broker offers to its plugins. The
// broker never picks a codec itself: an application-side plugin registers the
// encodings it understands and wires them into a transcoder or a dispatcher.
func (b *Broker) Codecs() *hook.CodecSet { return b.codecs }

// Plugins returns the registry of application plugins attached to this broker.
//
// It is created with the broker, so a plugin can be added before or after Start:
//
//	broker.Plugins().Add(jsonCodecPlugin)
//	if err := broker.Plugins().Start("json-codec"); err != nil { ... }
//	for _, status := range broker.Plugins().Snapshot() { ... }   // what runs, and why not
//
// The host a plugin receives is the broker itself (hook.Host), which is what makes
// the slots above reachable without importing the broker's internals.
func (b *Broker) Plugins() *gplugin.Registry[hook.Host] { return b.plugins }
