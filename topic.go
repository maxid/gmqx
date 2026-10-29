package gmqx

import "strings"

func validTopic(topic string) bool {
	if topic == "" {
		return false
	}
	return !strings.ContainsAny(topic, "+#")
}

// matchTopic reports whether an MQTT topic filter matches a topic.
func matchTopic(filter, topic string) bool {
	fs := strings.Split(filter, "/")
	ts := strings.Split(topic, "/")
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
