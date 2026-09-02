package generators

import "math/rand"

// testNamedSport exists only to exercise the registry's named-endpoint
// support, which no shipping sport currently uses.
//
// Registering it from a _test.go file keeps it out of the production catalog
// entirely: `loadtest -endpoints` and every entitlement check see the real
// fourteen sports, while the mechanism that lets one sport publish two
// endpoints of the same kind stays covered.
const testNamedSport Sport = "registry-fixture"

func init() {
	for _, name := range []string{"schedule", "directory"} {
		n := name
		register(Endpoint{
			Sport: testNamedSport, Kind: FeedReference, Name: n,
			Provider: ProviderNone, Provenance: ProvenanceModeled,
			Path: "/fixture/" + n, Model: "Fixture",
		}, func(rnd *rand.Rand) (sim, renderer) {
			s := &fixtureSim{base: newBase(rnd)}
			s.reset()
			return s, func() (any, string, bool) {
				return map[string]any{"endpoint": n}, "", true
			}
		})
	}
}

// fixtureSim is the minimum a registered feed needs.
//
// It rolls over like a real simulation. A feed that never completes would fail
// TestFixtureRollover, which exists because a round-the-clock load test must
// never dry up — and the fixture should not be exempt from an invariant every
// shipping feed has to meet.
type fixtureSim struct {
	base
	ticks int
}

func (f *fixtureSim) reset() {
	f.newFixture("FIX", 1, 0, 0)
	f.ticks = 0
}

func (f *fixtureSim) advance() {
	f.ticks++
	if f.ticks >= 20 {
		f.over = true
	}
}
