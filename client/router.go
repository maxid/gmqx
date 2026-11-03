package client

// Client-side dispatch: which handler receives an inbound PUBLISH.
//
// The MQTT broker does not tell a client which subscription a message belongs to —
// a PUBLISH carries a topic name, and the client matches it against the filters it
// asked for. So dispatch is a client concern, and it is the same concern every MQTT
// client solves: paho binds a handler to a filter (Subscribe's callback,
// AddRoute, DefaultPublishHandler), and gmqx does the same here.
//
// Routes are a table of their own, not part of the subscription state: a route
// survives a reconnect (the client restores the subscription, the handler never
// moved) and outlives an Unsubscribe, exactly as paho's routes do. A route is
// matched against the topic of the delivery with the same rule the broker uses
// (shared subscriptions match on the filter inside "$share/<group>/").

import (
	"strings"
	"sync"
)

// MessageHandler is called for one inbound publication. The payload is owned by the
// client and valid only during the call, so a handler that keeps it must copy it
// (see Message). Handlers run one at a time on the delivery goroutine, in
// registration order, and every route that matches a publication is called.
type MessageHandler func(*Message)

// route is one filter-to-handler binding.
type route struct {
	filter  string
	handler MessageHandler
}

// router is the route table. It is safe for concurrent use: a handler may
// subscribe, unsubscribe or add a route from inside a callback.
type router struct {
	mu     sync.RWMutex
	routes []route
}

// add binds handler to filter. An empty filter or a nil handler is ignored: a
// Subscribe that carries neither is a subscription without a route, which is
// legitimate (the fallback handler or nobody answers it).
func (r *router) add(filter string, handler MessageHandler) {
	filter = unshareFilter(filter)
	if filter == "" || handler == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes = append(r.routes, route{filter: filter, handler: handler})
}

// match returns the handlers whose filter matches topic, in registration order.
// A publication that matches two routes calls both: the broker sends one PUBLISH
// per client, and the overlap is resolved here, the same way paho resolves it.
func (r *router) match(topic string) []MessageHandler {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []MessageHandler
	for _, rt := range r.routes {
		if topicMatch(rt.filter, topic) {
			out = append(out, rt.handler)
		}
	}
	return out
}

// len reports how many routes are registered.
func (r *router) len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.routes)
}

// unshareFilter strips the "$share/<group>/" prefix: a shared subscription receives
// publications whose topic is the filter after the group, so that is what a route
// has to match on. paho does the same when it registers its handler.
func unshareFilter(filter string) string {
	if !strings.HasPrefix(filter, "$share/") {
		return filter
	}
	rest := strings.TrimPrefix(filter, "$share/")
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return filter
	}
	return rest[slash+1:]
}

// topicMatch implements the MQTT filter rule on the client side: '#' is the
// multi-level wildcard (it must be the last level), '+' is one level, and a topic
// starting with '$' is not matched by a filter starting with a wildcard.
func topicMatch(filter, topic string) bool {
	if filter == "" {
		return false
	}
	fl, tl := splitLevels(filter), splitLevels(topic)
	if len(tl) > 0 && strings.HasPrefix(tl[0], "$") && (fl[0] == "#" || fl[0] == "+") {
		return false
	}
	for i := 0; i < len(fl); i++ {
		switch fl[i] {
		case "#":
			return true
		case "+":
			if i >= len(tl) {
				return false
			}
		default:
			if i >= len(tl) || fl[i] != tl[i] {
				return false
			}
		}
	}
	return len(fl) == len(tl)
}

// splitLevels splits a topic or filter on '/', keeping empty levels: "a//b" has
// three levels, and "/a" starts with an empty one.
func splitLevels(s string) []string {
	out := make([]string, 0, strings.Count(s, "/")+1)
	for {
		i := strings.IndexByte(s, '/')
		if i < 0 {
			return append(out, s)
		}
		out = append(out, s[:i])
		s = s[i+1:]
	}
}
