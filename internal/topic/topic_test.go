package topic

import "testing"

// TestMatchLevels pins the level rules every caller relies on: the routing index
// (literal and wildcard lookups) and the cluster backends, which match a peer's
// advertised subscription summary against a publication. It covers the cases a
// string-split implementation gets right by accident: "#" matches the level the
// topic stopped at ("a/#" matches "a"), "+" never spans a level, and the first
// level of a '$' topic is only matched by a literal first level.
func TestMatchLevels(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"a", "a", true},
		{"a", "b", false},
		{"a/b", "a/b", true},
		{"a/b", "a/b/c", false},
		{"a/b", "a", false},
		{"a/#", "a", true},
		{"a/#", "a/", true},
		{"a/#", "a/b/c", true},
		{"a/#", "b", false},
		{"a/+/#", "a/b", true},
		{"a/+", "a", false},
		{"a/+", "a/b", true},
		{"a/+", "a/b/c", false},
		{"+", "a", true},
		{"+", "a/b", false},
		{"+", "/", false},
		{"+/+", "/", true},
		{"#", "a/b", true},
		{"#", "", true},
		{"#", "$SYS/uptime", false},
		{"+/uptime", "$SYS/uptime", false},
		{"$SYS/#", "$SYS/uptime", true},
		{"$SYS/+", "$SYS", false},
		{"", "", true},
		{"", "a", false},
		{"a", "", false},
		{"a//b", "a//b", true},
		{"a/+/b", "a//b", true},
	}
	for _, tc := range cases {
		if got := Match(tc.filter, tc.topic); got != tc.want {
			t.Errorf("Match(%q, %q) = %v, want %v", tc.filter, tc.topic, got, tc.want)
		}
	}
}
