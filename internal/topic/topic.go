// Package topic holds the MQTT topic-filter matching rule.
//
// It is a package of its own because two sides of the broker must agree on it and
// neither may import the other: the routing index matches a publication against
// the filters of its local sessions, and the cluster backends decide whether a
// peer's advertised subscription summary matches a publication they are about to
// route. A divergence between the two would mis-route messages that one side
// believes the other delivers, so the rule is written once here.
package topic

import "strings"

// Match reports whether an MQTT topic filter matches a topic.
//
// The levels are walked with string slicing instead of strings.Split: routing runs
// this for every wildcard filter of every publication, and the split allocated two
// slices per call. The rules are the MQTT ones: "#" matches the rest of the topic
// including the empty rest (so "a/#" matches "a"), "+" matches exactly one level,
// and a topic whose first level starts with '$' is not matched by a filter that
// starts with a wildcard (so "#" does not see the "$SYS" tree, while "$SYS/#"
// does).
func Match(filter, topic string) bool {
	rest, restTopic := filter, topic
	first := true
	for {
		f, nextFilter, filterMore := nextLevel(rest)
		t, nextTopic, topicMore := nextLevel(restTopic)
		if first {
			if strings.HasPrefix(t, "$") && (f == "#" || f == "+") {
				return false
			}
			first = false
		}
		if f == "#" {
			return true
		}
		if f != "+" && f != t {
			return false
		}
		if !filterMore {
			return !topicMore
		}
		if !topicMore {
			// The topic ran out one level before the filter: only a trailing "#"
			// of the filter can still match the level the topic stopped at.
			return nextFilter == "#"
		}
		rest, restTopic = nextFilter, nextTopic
	}
}

// nextLevel splits the first topic level off s: the level, the rest of the string
// and whether a level follows.
func nextLevel(s string) (level, rest string, more bool) {
	if i := strings.IndexByte(s, '/'); i >= 0 {
		return s[:i], s[i+1:], true
	}
	return s, "", false
}

// ValidTopic reports whether topic is a well-formed MQTT topic name: non-empty and
// free of the wildcard characters.
func ValidTopic(topic string) bool {
	if topic == "" {
		return false
	}
	return !strings.ContainsAny(topic, "+#")
}

// ValidFilter reports whether filter is a well-formed MQTT topic filter: not
// empty, "+" occupies a whole level, and "#" occupies the whole last level.
func ValidFilter(filter string) bool {
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

// SharedSubscriptionPrefix introduces a shared subscription filter
// ("$share/<shareName>/<filter>", MQTT 5 §4.8.2).
const SharedSubscriptionPrefix = "$share/"

// IsSharedSubscription reports whether filter has the shape of a shared
// subscription filter at all: it starts with "$share/". A filter that merely
// starts with "$share" ("$share" alone, "$sharex/g/t") is an ordinary filter and
// stays a literal one.
func IsSharedSubscription(filter string) bool {
	return strings.HasPrefix(filter, SharedSubscriptionPrefix)
}

// SharedFilter is a parsed shared subscription filter: the ShareName and the Topic
// Filter the ShareName groups.
type SharedFilter struct {
	ShareName string
	// Filter is the Topic Filter after the ShareName, with the same syntax and
	// semantics as a non-shared one (§4.8.2): it is what publications match and
	// what a retained replay would use.
	Filter string
}

// ParseSharedFilter parses a shared subscription filter.
//
// isShared reports whether filter claims to be a shared subscription at all
// (IsSharedSubscription); ok reports whether it is well formed. The two are
// separate because a malformed "$share/..." must be refused per filter (§4.8.2,
// MQTT-4.8.2-1 and -2: a ShareName of at least one character without "/", "+" or
// "#", followed by "/" and a valid Topic Filter), while a filter that only looks
// like one is an ordinary filter.
func ParseSharedFilter(filter string) (parsed SharedFilter, isShared, ok bool) {
	if !IsSharedSubscription(filter) {
		return SharedFilter{}, false, false
	}
	rest := filter[len(SharedSubscriptionPrefix):]
	shareName, topicFilter, found := strings.Cut(rest, "/")
	if !found || shareName == "" || strings.ContainsAny(shareName, "/+#") {
		return SharedFilter{}, true, false
	}
	if !ValidFilter(topicFilter) {
		return SharedFilter{}, true, false
	}
	return SharedFilter{ShareName: shareName, Filter: topicFilter}, true, true
}

// Matches reports whether the parsed shared filter matches topic.
func (s SharedFilter) Matches(topic string) bool { return Match(s.Filter, topic) }
