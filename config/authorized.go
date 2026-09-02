package config

import (
	"sort"
	"strings"

	"github.com/offloadintelligence/offload-ingest/pkg/ingest/apisports"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest/scope"
)

// Licence claims to authorised scopes.
//
// This is where a licence stops being a document and becomes an enforceable
// list. Both claims feed in — the sports list and the regional bundles — and
// they are aggregated into one flat, de-duplicated slice.
//
// # Why aggregation lives here and not in LoadConfig itself
//
// LoadConfig reads the environment. It cannot resolve entitlements, because the
// entitlements come from the licence file, and reading that file requires the
// path LoadConfig supplies — the ordering is load configuration, verify licence,
// then aggregate. Putting the aggregation in the config package keeps the logic
// where the directive asked for it while respecting that a signed licence must
// be verified before any claim inside it is trusted.

// AuthorizedScope is re-exported so callers can build the list without
// importing the scope package directly.
type AuthorizedScope = scope.AuthorizedScope

// AuthorizedScopes aggregates a licence's sport and region claims into one
// flat authorised list.
//
// Returns the list plus the sports that carry no league restriction. A sport
// with no restriction is not an oversight: several API-Sports hosts serve
// exactly one competition, and there the sport claim IS the entitlement.
//
// The sport claim remains authoritative. A region can narrow the list but never
// widen it — a licence naming region "global" and sports ["nfl"] authorises the
// NFL, not the world. Regions describe a package; they must never become a
// second, looser path to content.
func AuthorizedScopes(licensedSports, licensedRegions []string) (authorized []AuthorizedScope, unconstrained []string) {
	return AuthorizedScopesFor(apisports.Entitled(licensedSports, licensedRegions), licensedRegions)
}

// AuthorizedScopesFor builds the list from bindings that have already been
// resolved, which is the path the runtime takes because it resolves them once
// at startup to decide what to poll.
//
// Using the same bindings for both jobs is deliberate: the set that shapes
// requests and the set that validates responses cannot then disagree about what
// the venue bought.
func AuthorizedScopesFor(bindings []apisports.Binding, licensedRegions []string) (authorized []AuthorizedScope, unconstrained []string) {
	regionOf := regionIndex(licensedRegions)

	for _, b := range bindings {
		sport := strings.ToLower(string(b.Sport))
		source := "sport"
		if r, ok := regionOf[sport]; ok {
			source = "region:" + r
		}

		if len(b.Leagues) == 0 {
			// The vertical serves one competition; the sport claim is the
			// entitlement and there is nothing to restrict.
			unconstrained = append(unconstrained, sport)
			continue
		}
		for i, id := range b.Leagues {
			entry := AuthorizedScope{Sport: sport, ID: id, Source: source}
			if i < len(b.LeagueNames) {
				entry.Name = b.LeagueNames[i]
			}
			authorized = append(authorized, entry)
		}
	}
	sort.Strings(unconstrained)
	return scope.Aggregate(authorized), unconstrained
}

// regionIndex maps a sport to the first licensed bundle that grants it, so an
// audit can say whether a competition arrived through the sports list or
// through a package.
func regionIndex(licensedRegions []string) map[string]string {
	out := map[string]string{}
	for _, r := range licensedRegions {
		token := strings.ToLower(strings.TrimSpace(r))
		sports, err := apisports.SportsForRegion(apisports.Region(token))
		if err != nil {
			continue
		}
		for _, s := range sports {
			name := strings.ToLower(string(s))
			if _, seen := out[name]; !seen {
				out[name] = token
			}
		}
	}
	return out
}
