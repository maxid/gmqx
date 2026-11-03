package client

import (
	"sync"
	"testing"
)

func TestTopicMatchClientSide(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
		{"a/+", "a/b", true},
		{"a/+", "a/b/c", false},
		{"a/#", "a", true},
		{"a/#", "a/b/c", true},
		{"#", "a/b", true},
		{"#", "$SYS/uptime", false},
		{"$SYS/#", "$SYS/uptime", true},
		{"+/b", "/b", true},
		{"", "", false},
	}
	for _, tc := range cases {
		if got := topicMatch(tc.filter, tc.topic); got != tc.want {
			t.Errorf("topicMatch(%q, %q) = %v, want %v", tc.filter, tc.topic, got, tc.want)
		}
	}
}

func TestRouterMatchesEveryRoute(t *testing.T) {
	var r router
	var mu sync.Mutex
	var called []string
	record := func(name string) MessageHandler {
		return func(*Message) {
			mu.Lock()
			called = append(called, name)
			mu.Unlock()
		}
	}
	r.add("a/#", record("wild"))
	r.add("a/+", record("plus"))
	r.add("b/#", record("other"))

	handlers := r.match("a/x")
	if len(handlers) != 2 {
		t.Fatalf("match = %d handlers, want 2 (one per matching route)", len(handlers))
	}
	for _, h := range handlers {
		h(&Message{Topic: "a/x"})
	}
	// Registration order, and the non-matching route stays out.
	mu.Lock()
	defer mu.Unlock()
	if len(called) != 2 || called[0] != "wild" || called[1] != "plus" {
		t.Fatalf("called = %v, want [wild plus]", called)
	}
	if len(r.match("b/x")) != 1 || len(r.match("c/x")) != 0 {
		t.Fatalf("route table is not per filter: %d, %d", len(r.match("b/x")), len(r.match("c/x")))
	}
}

func TestRouterIgnoresEmptyRoutes(t *testing.T) {
	var r router
	r.add("", func(*Message) {})
	r.add("a", nil)
	if r.len() != 0 {
		t.Fatalf("len = %d, want 0: an empty filter or a nil handler is not a route", r.len())
	}
}

// A shared subscription receives publications whose topic is the filter inside the
// group, so the route has to match on that.
func TestRouterStripsSharedPrefix(t *testing.T) {
	if got := unshareFilter("$share/group/a/#"); got != "a/#" {
		t.Fatalf("unshareFilter = %q", got)
	}
	if got := unshareFilter("$share/broken"); got != "$share/broken" {
		t.Fatalf("unshareFilter(%q) = %q, want it left alone", "$share/broken", got)
	}
	if got := unshareFilter("plain/#"); got != "plain/#" {
		t.Fatalf("unshareFilter = %q", got)
	}
	var r router
	r.add("$share/g/a/#", func(*Message) {})
	if len(r.match("a/x")) != 1 {
		t.Fatal("a shared route does not match the inner filter")
	}
}
