//go:build interop_without_paho

package interop

// The fallback of the paho direction, built with the interop_without_paho tag.
//
// The third-party client is a compile-time dependency: when it cannot be
// resolved (no network, no module cache, a vendor-less offline checkout) the
// default build fails before any test can run. Building with
//
//	go test -tags interop_without_paho ./...
//
// drops the dependency entirely and reports the same cases as skipped with a
// reason, so the rest of the matrix stays runnable and the report says exactly
// what did not run. The default build (no tag) runs the real cases.

import "testing"

func skipWithoutPaho(t *testing.T) {
	t.Helper()
	t.Skip("the paho client is not part of this build (interop_without_paho tag): " +
		"the third-party client tests are skipped instead of failing to build without the module")
}

func TestPahoQoSRoundTrips(t *testing.T)     { skipWithoutPaho(t) }
func TestPahoRetainedMessage(t *testing.T)   { skipWithoutPaho(t) }
func TestPahoWill(t *testing.T)              { skipWithoutPaho(t) }
func TestPahoPersistentSession(t *testing.T) { skipWithoutPaho(t) }
func TestPahoLargePayload(t *testing.T)      { skipWithoutPaho(t) }
func TestPahoOversizedPublish(t *testing.T)  { skipWithoutPaho(t) }
