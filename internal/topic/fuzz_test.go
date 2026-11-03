package topic

import (
	"strings"
	"testing"
)

// FuzzMatch feeds arbitrary filters and topics to the matcher.
//
// The property it pins is the one the broker relies on everywhere: the matcher
// never panics and never allocates without bound on input a peer controls. A
// subscription filter arrives in a SUBSCRIBE (a peer), a topic in every PUBLISH
// and in every cluster event — so a panic here is a remote crash, and the routing
// index calls it on the publish path. Correctness is pinned by the table tests;
// this one is about surviving what they do not enumerate.
func FuzzMatch(f *testing.F) {
	seeds := [][2]string{
		{"a/b/c", "a/b/c"},
		{"a/+/c", "a/b/c"},
		{"a/#", "a"},
		{"#", "a/b"},
		{"#", "$SYS/uptime"},
		{"$SYS/#", "$SYS/uptime"},
		{"+/+/+", "a/b/c"},
		{"a//b", "a//b"},
		{"", ""},
		{"#", ""},
		{"a/+/#", "a/b/c/d"},
		{"$share/g/a/#", "a/b"},
	}
	for _, s := range seeds {
		f.Add(s[0], s[1])
	}

	f.Fuzz(func(t *testing.T, filter, topic string) {
		matched := Match(filter, topic)
		// Determinism: the same pair always answers the same way (a router that
		// disagrees with itself would deliver a message twice or not at all).
		if again := Match(filter, topic); again != matched {
			t.Fatalf("Match(%q, %q) is not deterministic", filter, topic)
		}
		// A wildcard-free filter is a topic name: it matches that topic and nothing
		// else. The broker validates first, so the target only claims this for
		// filters and topics that are already well-formed.
		if !strings.ContainsAny(filter, "+#") && ValidFilter(filter) && ValidTopic(topic) {
			if want := filter == topic; matched != want {
				t.Fatalf("Match(%q, %q) = %v, want %v (literal filter)", filter, topic, matched, want)
			}
		}
		// "#" sees every topic tree except the ones starting with '$'.
		if filter == "#" && !strings.HasPrefix(topic, "$") && topic != "" {
			if !matched {
				t.Fatalf("Match(%q, %q) = false, want true", filter, topic)
			}
		}
	})
}

// FuzzValidFilter checks the answer ValidFilter gives against the structural rules:
// a filter it accepts must be matchable, one it rejects must not be used.
func FuzzValidFilter(f *testing.F) {
	for _, seed := range []string{"a", "a/b", "a/#", "+/b", "#", "", "a/#/b", "+", "a/+/#", "$share/g/a"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, filter string) {
		if !ValidFilter(filter) {
			return
		}
		// An accepted filter never panics the matcher on any topic.
		_ = Match(filter, "a/b/c")
		_ = Match(filter, "$SYS/x")
	})
}
