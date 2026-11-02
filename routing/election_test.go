package routing

// Shared-group election tests. The election is what makes a cluster deliver a
// shared publication exactly once: every origin must pick one node out of the
// candidates deterministically, and successive publications of one group must visit
// every candidate instead of pinning the group to the node its hash favours.

import (
	"reflect"
	"testing"
)

// TestElectionIsDeterministic pins the affinity: two origins that see the same
// candidate set elect the same node for the same group, whatever order the backend
// listed the candidates in, and duplicates in that list do not skew the choice.
func TestElectionIsDeterministic(t *testing.T) {
	const group = "$share/workers/jobs/+"
	candidates := []string{"node-b", "node-a", "node-c"}

	first := new(Election)
	chosen, ok := first.Choose(group, candidates)
	if !ok {
		t.Fatal("Choose reported no candidate for a non-empty set")
	}

	for i := 0; i < 20; i++ {
		// Every fresh election starts from the group's affinity, so the first choice
		// is the same node on every origin; duplicates are removed before the modulo.
		if got, ok := new(Election).Choose(group, []string{"node-c", "node-a", "node-b", "node-a"}); !ok || got != chosen {
			t.Fatalf("fresh election chose %q (found %v), want %q", got, ok, chosen)
		}
	}
}

// TestElectionRotatesThroughCandidates pins the seat: one group visits every
// candidate once per round instead of pinning the group to its hash's favourite.
func TestElectionRotatesThroughCandidates(t *testing.T) {
	e := new(Election)
	candidates := []string{"node-a", "node-b", "node-c"}

	seen := make(map[string]int, len(candidates))
	for i := 0; i < 3*len(candidates); i++ {
		chosen, ok := e.Choose("$share/g/t", candidates)
		if !ok {
			t.Fatal("Choose reported no candidate")
		}
		seen[chosen]++
	}
	for _, node := range candidates {
		if seen[node] != 3 {
			t.Fatalf("node %q was elected %d times over three rounds, want 3", node, seen[node])
		}
	}
}

// TestElectionKeepsGroupsApart pins that the rotation is per group: two groups
// that match the same publication must not disturb each other's parity (a single
// shared counter would make both of them advance one step per publication).
func TestElectionKeepsGroupsApart(t *testing.T) {
	e := new(Election)
	candidates := []string{"node-a", "node-b"}

	groupA := make([]string, 0, 4)
	groupB := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		a, _ := e.Choose("$share/a/t", candidates)
		b, _ := e.Choose("$share/b/t", candidates)
		groupA = append(groupA, a)
		groupB = append(groupB, b)
	}
	// Each group alternates on its own: consecutive choices differ in both traces.
	for _, trace := range [][]string{groupA, groupB} {
		for i := 1; i < len(trace); i++ {
			if trace[i] == trace[i-1] {
				t.Fatalf("trace %v repeats two nodes in a row; a per-group rotation must alternate", trace)
			}
		}
	}
}

// TestElectionWithoutCandidates pins the "no node holds a member" answer, which the
// broker turns into one counted drop rather than a guess.
func TestElectionWithoutCandidates(t *testing.T) {
	e := new(Election)
	for _, candidates := range [][]string{nil, {}} {
		if chosen, ok := e.Choose("$share/g/t", candidates); ok {
			t.Fatalf("Choose(%v) = %q, want no candidate", candidates, chosen)
		}
	}
}

// TestElectionResetKeepsChoosing pins the bounded rotation table: past its limit a
// fresh table still elects an existing candidate (the hash spreads the groups; only
// the rotation restarts).
func TestElectionResetKeepsChoosing(t *testing.T) {
	e := new(Election)
	candidates := []string{"node-a", "node-b"}
	for i := 0; i < maxRotationEntries+1; i++ {
		group := "$share/g/" + string(rune('a'+i%26)) + "/" + string(rune('a'+(i/26)%26))
		chosen, ok := e.Choose(group, candidates)
		if !ok || (chosen != "node-a" && chosen != "node-b") {
			t.Fatalf("Choose(%q) = %q (found %v), want one of the candidates", group, chosen, ok)
		}
	}
	if len(e.rotation) == 0 {
		t.Fatalf("the rotation table stayed empty after %d elections", maxRotationEntries+1)
	}
}

// TestUniqueSorted pins the candidate normalisation the election relies on.
func TestUniqueSorted(t *testing.T) {
	if got := uniqueSorted(nil); got != nil {
		t.Fatalf("uniqueSorted(nil) = %v, want nil", got)
	}
	got := uniqueSorted([]string{"b", "a", "b", "c", "a"})
	if !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("uniqueSorted = %v, want [a b c]", got)
	}
	if got := uniqueSorted([]string{"only"}); !reflect.DeepEqual(got, []string{"only"}) {
		t.Fatalf("uniqueSorted(single) = %v, want [only]", got)
	}
}
