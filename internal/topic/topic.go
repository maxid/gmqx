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
