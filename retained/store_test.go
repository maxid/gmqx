package retained

import (
	"testing"
	"time"
)

func TestStorePutGetDelete(t *testing.T) {
	s := NewTable()
	if _, ok := s.Get("k"); ok {
		t.Fatal("an empty store returned a message")
	}
	m := Message{Namespace: "ns", Topic: "t", Payload: []byte("v"), QoS: 1}
	if !s.Put("k", m, 0) {
		t.Fatal("Put with no cap refused the first message")
	}
	got, ok := s.Get("k")
	if !ok || got.Topic != "t" || string(got.Payload) != "v" || got.QoS != 1 {
		t.Fatalf("Get = %+v, %v", got, ok)
	}
	s.Delete("k")
	s.Delete("k") // deleting a missing key is a no-op
	if s.Len() != 0 {
		t.Fatalf("Len after Delete = %d, want 0", s.Len())
	}
}

func TestStoreCapAcceptsReplacement(t *testing.T) {
	s := NewTable()
	s.Put("a", Message{Topic: "a", Payload: []byte("1")}, 2)
	s.Put("b", Message{Topic: "b", Payload: []byte("2")}, 2)
	if s.Put("c", Message{Topic: "c"}, 2) {
		t.Fatal("Put accepted a new key over the cap")
	}
	// Replacing a topic the store already holds does not grow it, so a full store
	// still accepts a new value for an existing topic.
	if !s.Put("a", Message{Topic: "a", Payload: []byte("3")}, 2) {
		t.Fatal("Put refused a replacement at the cap")
	}
	if got, _ := s.Get("a"); string(got.Payload) != "3" {
		t.Fatalf("replaced payload = %q, want 3", got.Payload)
	}
	if s.Len() != 2 {
		t.Fatalf("Len = %d, want 2", s.Len())
	}
	// A zero max means no cap.
	if !s.Put("c", Message{Topic: "c"}, 0) {
		t.Fatal("Put with max 0 refused a new key")
	}
	if s.Len() != 3 {
		t.Fatalf("Len = %d, want 3", s.Len())
	}
}

func TestStoreRangeRemovesOnFalse(t *testing.T) {
	s := NewTable()
	now := time.Now()
	s.Put("live", Message{Topic: "live", ExpiresAt: now.Add(time.Minute)}, 0)
	s.Put("stale", Message{Topic: "stale", ExpiresAt: now.Add(-time.Minute)}, 0)

	var seen []string
	s.Range(func(key string, m Message) bool {
		seen = append(seen, m.Topic)
		return !m.ExpiresAt.Before(now)
	})
	if len(seen) != 2 {
		t.Fatalf("Range visited %v, want both messages", seen)
	}
	if _, ok := s.Get("stale"); ok {
		t.Fatal("Range kept a message whose callback returned false")
	}
	if _, ok := s.Get("live"); !ok {
		t.Fatal("Range removed a message whose callback returned true")
	}
}
