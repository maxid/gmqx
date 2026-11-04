package hook_test

import (
	"strings"
	"testing"

	"github.com/maxid/gmqx/hook"
)

func TestUserPropertyRoundTrip(t *testing.T) {
	properties, err := hook.WithUserProperty(nil, "traceparent", "00-abc-def-01")
	if err != nil {
		t.Fatal(err)
	}
	values, err := hook.UserProperties(properties, "traceparent")
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0] != "00-abc-def-01" {
		t.Fatalf("values = %v", values)
	}

	properties, err = hook.WithUserProperty(properties, "traceparent", "second")
	if err != nil {
		t.Fatal(err)
	}
	values, _ = hook.UserProperties(properties, "traceparent")
	if len(values) != 2 || values[1] != "second" {
		t.Fatalf("values = %v", values)
	}
}

func TestUserPropertyPreservesExisting(t *testing.T) {
	properties, err := hook.WithUserProperty(nil, "a", "1")
	if err != nil {
		t.Fatal(err)
	}
	properties, err = hook.WithUserProperty(properties, "b", "2")
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"a": "1", "b": "2"} {
		values, err := hook.UserProperties(properties, key)
		if err != nil || len(values) != 1 || values[0] != want {
			t.Fatalf("%s = %v err=%v", key, values, err)
		}
	}
}

func TestUserPropertyRejectsMalformed(t *testing.T) {
	if _, err := hook.WithUserProperty([]byte{0xff, 0x00}, "k", "v"); err == nil {
		t.Fatal("malformed block accepted")
	}
	if _, err := hook.UserProperties([]byte{0xff, 0x00}, "k"); err == nil {
		t.Fatal("malformed block accepted")
	}
	if values, err := hook.UserProperties(nil, "missing"); err != nil || len(values) != 0 {
		t.Fatalf("empty block = %v err=%v", values, err)
	}
}

func TestUserPropertyConstrainsKeyAndValue(t *testing.T) {
	// The codec rejects invalid UTF-8 in a user property; the helper must
	// surface that instead of writing a malformed block.
	bad := string([]byte{0xff})
	if _, err := hook.WithUserProperty(nil, "k", bad); err == nil || !strings.Contains(err.Error(), "UTF-8") {
		t.Fatalf("err = %v", err)
	}
}
