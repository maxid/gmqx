package gmqx

import "strings"

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
// ("$share/<group>/<filter>", MQTT 5 §4.8.2).
const sharedSubscriptionPrefix = "$share/"

// isSharedSubscription reports whether filter is a shared subscription filter.
// This broker does not implement shared subscriptions — its CONNACK advertises
// Shared Subscription Available 0 — so the caller refuses such a filter instead
// of storing it as a literal that can never match: it is not a topic name a
// publisher could use, so the subscription would be accepted and then stay
// silent forever.
func isSharedSubscription(filter string) bool {
	return strings.HasPrefix(filter, sharedSubscriptionPrefix)
}

// matchTopic reports whether an MQTT topic filter matches a topic.
//
// The levels are walked with string slicing instead of strings.Split: routing runs
// this for every wildcard filter of every publication, and the split allocated two
// slices per call. The rules are the MQTT ones: "#" matches the rest of the topic
// including the empty rest (so "a/#" matches "a"), "+" matches exactly one level,
// and a topic whose first level starts with '$' is not matched by a filter that
// starts with a wildcard (so "#" does not see the "$SYS" tree, while "$SYS/#"
// does).
func matchTopic(filter, topic string) bool {
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
