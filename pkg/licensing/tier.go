// Package licensing verifies the local Ed25519-signed license that gates this
// binary, and exposes the entitlements it carries.
//
// The model is offline-first. A venue runs on its own hardware, often behind a
// restrictive network, so there is no call home: everything needed to decide
// whether this process may run is inside license.key, signed by a private key
// that never leaves the build system. The binary embeds only the public half.
//
// What the license decides:
//
//   - whether this build may run at all (product entitlement, expiry)
//   - which hardware it may run on (fingerprint pinning)
//   - which sports and regions the venue paid for
//   - how hard it may hit the upstream API (the tier)
//
// That last one is why licensing is not a bolt-on. The API-Sports quota is the
// scarcest resource in the system, and the tier in the license is what tells
// the rate limiter how much of it exists. A venue on the free plan and a venue
// on a custom contract run the same binary; the license is the difference.
package licensing

import (
	"fmt"
	"strings"
)

// TierName identifies an API-Sports subscription level.
type TierName string

const (
	TierFree   TierName = "free"
	TierPro    TierName = "pro"
	TierUltra  TierName = "ultra"
	TierMega   TierName = "mega"
	TierCustom TierName = "custom"
)

// Tier is the throughput a licence entitles a venue to.
//
// IMPORTANT: every ceiling here is PER SPORT HOST, not per account. API-Sports
// runs a separate host per vertical — v3.football, v1.basketball, v1.afl and so
// on — and meters each one independently against the same key. A free venue
// polling six sports therefore has 600 requests/day in total, not 100. Treating
// the quota as global would leave five sixths of it unspent; treating it as
// unlimited would get every sport throttled at once. The budget allocator in
// pkg/ingest works per host for exactly this reason.
type Tier struct {
	Name TierName `json:"name"`
	// RequestsPerMinute is the burst ceiling one host will accept.
	RequestsPerMinute int `json:"requests_per_minute"`
	// RequestsPerDay is the rolling daily allowance for one host.
	RequestsPerDay int `json:"requests_per_day"`
	// RequestsPerMonth is the contractual volume ceiling. Zero means the plan
	// is metered daily only, which is how every published tier works; a custom
	// contract can set it and the usage tracker will enforce it.
	RequestsPerMonth int `json:"requests_per_month,omitempty"`
}

// catalog holds the published API-Sports plans.
//
// PROVENANCE, in the same spirit as the feed catalog: `free` is VERIFIED — a
// live call to /status on all twelve hosts returned plan "Free", and the
// response headers on a real request read x-ratelimit-limit: 10 and
// x-ratelimit-requests-limit: 100. The paid rows are TRANSCRIBED from the
// published pricing page and have not been exercised against a live key, so
// they are a starting point that the header feedback loop corrects at runtime.
// The limiter never trusts these numbers over what the API actually says.
var catalog = map[TierName]Tier{
	TierFree:  {Name: TierFree, RequestsPerMinute: 10, RequestsPerDay: 100},
	TierPro:   {Name: TierPro, RequestsPerMinute: 300, RequestsPerDay: 7_500},
	TierUltra: {Name: TierUltra, RequestsPerMinute: 450, RequestsPerDay: 75_000},
	TierMega:  {Name: TierMega, RequestsPerMinute: 900, RequestsPerDay: 150_000},
}

// LookupTier resolves a named plan to its published ceilings.
func LookupTier(name TierName) (Tier, bool) {
	t, ok := catalog[TierName(strings.ToLower(string(name)))]
	return t, ok
}

// Resolve returns the effective tier for a licence.
//
// A named plan takes its ceilings from the catalog. `custom` must carry its own
// numbers, because a negotiated contract is exactly the case the catalog cannot
// know about — and a custom licence with no ceilings is rejected rather than
// silently defaulting to something generous.
func (t Tier) Resolve() (Tier, error) {
	name := TierName(strings.ToLower(string(t.Name)))
	if name == "" {
		return Tier{}, fmt.Errorf("licensing: tier has no name")
	}
	if name == TierCustom {
		if t.RequestsPerMinute <= 0 || t.RequestsPerDay <= 0 {
			return Tier{}, fmt.Errorf(
				"licensing: custom tier must state requests_per_minute and requests_per_day")
		}
		t.Name = TierCustom
		return t, nil
	}
	known, ok := catalog[name]
	if !ok {
		return Tier{}, fmt.Errorf("licensing: unknown tier %q", t.Name)
	}
	// A licence may tighten a published plan but never loosen it. An issuer
	// that wants more than the plan allows has to say `custom` and own it,
	// which keeps an over-generous number from riding in on a typo.
	out := known
	if t.RequestsPerMinute > 0 && t.RequestsPerMinute < known.RequestsPerMinute {
		out.RequestsPerMinute = t.RequestsPerMinute
	}
	if t.RequestsPerDay > 0 && t.RequestsPerDay < known.RequestsPerDay {
		out.RequestsPerDay = t.RequestsPerDay
	}
	out.RequestsPerMonth = t.RequestsPerMonth
	return out, nil
}

// String renders the tier the way the dashboard and logs show it.
func (t Tier) String() string {
	if t.RequestsPerMonth > 0 {
		return fmt.Sprintf("%s (%d/min, %d/day/host, %d/month)",
			t.Name, t.RequestsPerMinute, t.RequestsPerDay, t.RequestsPerMonth)
	}
	return fmt.Sprintf("%s (%d/min, %d/day/host)",
		t.Name, t.RequestsPerMinute, t.RequestsPerDay)
}
