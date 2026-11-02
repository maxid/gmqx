package gmqx

import (
	"strings"

	ns "github.com/maxid/gmqx/internal/namespace"
	topicmatch "github.com/maxid/gmqx/internal/topic"
)

func validTopic(topic string) bool {
	if topic == "" {
		return false
	}
	return !strings.ContainsAny(topic, "+#")
}

// validFilter reports whether an MQTT topic filter is well formed: not empty,
// "+" occupies a whole level, and "#" occupies the whole last level.
func validFilter(filter string) bool {
	if filter == "" {
		return false
	}
	levels := strings.Split(filter, "/")
	for i, level := range levels {
		switch level {
		case "#":
			if i != len(levels)-1 {
				return false
			}
		case "+":
			// A single-level wildcard.
		default:
			if strings.ContainsAny(level, "+#") {
				return false
			}
		}
	}
	return true
}

// sharedSubscriptionPrefix introduces a shared subscription filter
// ("$share/<shareName>/<filter>", MQTT 5 §4.8.2).
const sharedSubscriptionPrefix = "$share/"

// isSharedSubscription reports whether filter has the shape of a shared
// subscription filter at all: it starts with "$share/". A filter that merely
// starts with "$share" ("$share" alone, "$sharex/g/t") is an ordinary filter
// and stays a literal one.
func isSharedSubscription(filter string) bool {
	return strings.HasPrefix(filter, sharedSubscriptionPrefix)
}

// sharedFilter is a parsed shared subscription filter: the ShareName and the
// Topic Filter the ShareName groups.
type sharedFilter struct {
	shareName string
	// filter is the Topic Filter after the ShareName, with the same syntax and
	// semantics as a non-shared one (§4.8.2): it is what publications match and
	// what a retained replay would use.
	filter string
}

// parseSharedFilter parses a shared subscription filter.
//
// isShared reports whether filter claims to be a shared subscription at all
// (isSharedSubscription); ok reports whether it is well formed. The two are
// separate because a malformed "$share/..." must be refused per filter (§4.8.2,
// MQTT-4.8.2-1 and -2: a ShareName of at least one character without "/", "+"
// or "#", followed by "/" and a valid Topic Filter), while a filter that only
// looks like one is an ordinary filter.
func parseSharedFilter(filter string) (parsed sharedFilter, isShared, ok bool) {
	if !isSharedSubscription(filter) {
		return sharedFilter{}, false, false
	}
	rest := filter[len(sharedSubscriptionPrefix):]
	shareName, topicFilter, found := strings.Cut(rest, "/")
	if !found || shareName == "" || strings.ContainsAny(shareName, "/+#") {
		return sharedFilter{}, true, false
	}
	if !validFilter(topicFilter) {
		return sharedFilter{}, true, false
	}
	return sharedFilter{shareName: shareName, filter: topicFilter}, true, true
}

// matches reports whether the parsed shared filter matches topic.
func (s sharedFilter) matches(topic string) bool { return matchTopic(s.filter, topic) }

// matchTopic reports whether an MQTT topic filter matches a topic. The rule lives
// in ./internal/topic because the cluster backends route by it too and may not
// import this package.
func matchTopic(filter, topic string) bool { return topicmatch.Match(filter, topic) }

// The key-space rules live in ./internal/namespace; these wrappers keep the root package's
// call sites and tests unchanged.
func namespaceKey(namespace, name string) string { return ns.Key(namespace, name) }

func keyInNamespace(namespace, key string) bool { return ns.InNamespace(namespace, key) }
