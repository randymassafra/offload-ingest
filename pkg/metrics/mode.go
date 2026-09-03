package metrics

import "sort"

// Provider mode values, exported as offload_ingest_provider_mode.
//
// A gauge rather than a label, so a query can sum or alert arithmetically:
// `sum(offload_ingest_provider_mode)` is how many feeds are live, and
// `offload_ingest_provider_mode{provider="apisports"} == 0` is an alert
// condition on its own. Encoding the state as a label value instead would make
// both awkward, because a label change creates a new series and leaves the old
// one hanging at its last value.
const (
	// ProviderSimulation means this provider makes no upstream call. Its
	// sports are served by the local generators.
	ProviderSimulation = 0
	// ProviderLive means this provider is contacting its vendor and spending
	// real quota.
	ProviderLive = 1
)

// ProviderMode records whether one provider is live or simulated.
//
// # What "live" means here, precisely
//
// It means this provider is actually issuing upstream requests right now — not
// that it could, and not that a credential for it happens to be set. The
// distinction matters because those three conditions come apart in normal
// operation:
//
//   - The process may be in simulation mode with every key present. Nothing is
//     live. Reporting otherwise would tell an operator that quota is being
//     spent when it is not.
//   - A provider may have a key but no production streamer. Cricket and tennis
//     are in exactly this state: RAPIDAPI_KEY is set and used by the schema
//     tooling, but neither sport has a live path in the ingest runtime, so both
//     are simulated whatever the environment says.
//   - A provider may be wired and keyed but not entitled. Golf is skipped when
//     the licence does not name it.
//
// So this is set from the runtime assembly, at the point each source is either
// added or not added to the streamer, rather than inferred from configuration.
// Configuration is what someone intended; this is what happened.
type ProviderMode struct {
	// Live is the reported value.
	Live bool
	// Reason explains a zero. Left empty when live, because "it is running" is
	// not a fact anyone needs spelled out; a provider that is NOT running is
	// the one an operator has to explain, and making them read the assembly
	// code to find out is the failure this field prevents.
	Reason string
}

// SetProviderMode records one provider's live/simulated state.
func (r *Registry) SetProviderMode(provider string, live bool, reason string) {
	r.modeMu.Lock()
	defer r.modeMu.Unlock()
	if r.providerModes == nil {
		r.providerModes = map[string]ProviderMode{}
	}
	if live {
		reason = ""
	}
	r.providerModes[provider] = ProviderMode{Live: live, Reason: reason}
}

// ProviderModes returns every registered provider's mode, by name.
func (r *Registry) ProviderModes() map[string]ProviderMode {
	r.modeMu.RLock()
	defer r.modeMu.RUnlock()
	out := make(map[string]ProviderMode, len(r.providerModes))
	for k, v := range r.providerModes {
		out[k] = v
	}
	return out
}

// ProviderNames lists registered providers, sorted, so output is stable.
func (r *Registry) ProviderNames() []string {
	r.modeMu.RLock()
	defer r.modeMu.RUnlock()
	out := make([]string, 0, len(r.providerModes))
	for k := range r.providerModes {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// LiveProviders counts providers currently contacting a vendor.
func (r *Registry) LiveProviders() int {
	r.modeMu.RLock()
	defer r.modeMu.RUnlock()
	n := 0
	for _, m := range r.providerModes {
		if m.Live {
			n++
		}
	}
	return n
}
