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

// matchTopic reports whether an MQTT topic filter matches a topic.
func matchTopic(filter, topic string) bool {
	fs := strings.Split(filter, "/")
	ts := strings.Split(topic, "/")
	// A topic name that starts with '$' is reserved and is not matched by a
	// filter that starts with a wildcard.
	if len(ts) > 0 && strings.HasPrefix(ts[0], "$") && (fs[0] == "#" || fs[0] == "+") {
		return false
	}
	for i, f := range fs {
		if f == "#" {
			return true
		}
		if i >= len(ts) {
			return false
		}
		if f == "+" {
			continue
		}
		if f != ts[i] {
			return false
		}
	}
	return len(fs) == len(ts)
}
