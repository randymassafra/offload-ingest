package metrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Health answers one question for the venue's monitoring system: is this
// appliance producing data, or is it merely running?
//
// The distinction is the whole point. A process that is up, licensed, polling
// on schedule and receiving well-formed empty responses looks perfect on every
// other signal we expose, and is worth nothing to the screens behind the bar.
// A liveness probe that only proves the binary has not crashed would report
// that state as healthy, so this is deliberately a readiness probe: it goes red
// while the process stays up, because the operator's next action differs
// entirely between "restart it" and "find out why the provider stopped".
//
// # Two conditions, both required
//
// A healthy appliance has published data recently AND is not serving a rate
// limit penalty. They are separate because they have different remedies.
// Data starvation is usually upstream — a season ended, a licence narrowed, a
// provider outage. A hard floor is self-inflicted and time-boxed: we hit a 429
// and backed off deliberately, and the fix is to wait rather than to
// investigate.
//
// # The overnight problem
//
// "No data in fifteen minutes" is indistinguishable, from inside this process,
// from "no sport anywhere in the licensed region is currently playing". At
// 04:00 local time the second is overwhelmingly more likely, and an appliance
// that pages someone every night is an appliance whose alerts get muted.
//
// This is not solved here, because it cannot be: the fixture calendar that
// would answer it is upstream data we do not hold. What is done instead is to
// report LastPoll alongside LastData, so the monitoring system can tell the two
// apart without guessing. A box that is polling successfully and receiving
// empty cards is quiet; a box whose LastPoll is also stale is broken. Whoever
// configures the alert should suppress on the first and page on the second.
type Health struct {
	// OK is the verdict the HTTP status code follows.
	OK bool `json:"ok"`
	// Status is a stable machine-readable reason, never prose.
	Status HealthStatus `json:"status"`
	// Detail is the human sentence for whoever is reading the response body.
	Detail string `json:"detail"`

	// LastData is when any sport last produced a record for publication, and
	// LastDataSport names it. Zero when nothing has ever been produced.
	LastData      time.Time `json:"last_data,omitempty"`
	LastDataSport string    `json:"last_data_sport,omitempty"`
	// DataAgeSeconds is how stale LastData is. Negative when there is none.
	DataAgeSeconds float64 `json:"data_age_seconds"`

	// LastPoll is when any provider last answered successfully, including with
	// an empty card. This is what separates "quiet" from "broken"; see the
	// overnight note above.
	LastPoll       time.Time `json:"last_poll,omitempty"`
	LastPollSport  string    `json:"last_poll_sport,omitempty"`
	PollAgeSeconds float64   `json:"poll_age_seconds"`

	// WindowSeconds is the freshness window this verdict was measured against.
	WindowSeconds float64 `json:"window_seconds"`

	// RateLimited is true while any provider is serving a hard floor, and
	// RateLimitedProviders names them.
	RateLimited          bool     `json:"rate_limited"`
	RateLimitedProviders []string `json:"rate_limited_providers,omitempty"`

	// UptimeSeconds is included because a box that has been up for nine
	// seconds and has no data yet is starting, not starved.
	UptimeSeconds float64 `json:"uptime_seconds"`

	// Sports is per-sport freshness, so a 503 says which feed died rather than
	// only that one did.
	Sports []SportFreshness `json:"sports,omitempty"`
}

// SportFreshness is one sport's last-seen times.
type SportFreshness struct {
	Sport          string    `json:"sport"`
	LastData       time.Time `json:"last_data,omitempty"`
	LastPoll       time.Time `json:"last_poll,omitempty"`
	DataAgeSeconds float64   `json:"data_age_seconds"`
	Messages       int64     `json:"messages"`
}

// HealthStatus is the machine-readable verdict.
//
// It is a closed set of tokens rather than a free-text message because an
// alerting rule will match on it, and a rule that matches on prose breaks the
// first time someone improves the wording.
type HealthStatus string

const (
	// StatusOK means data is flowing and no penalty is in force.
	StatusOK HealthStatus = "ok"
	// StatusStarting means the process is inside its grace period and has not
	// had a fair chance to produce anything yet.
	StatusStarting HealthStatus = "starting"
	// StatusDataStarved means nothing has been published inside the window.
	StatusDataStarved HealthStatus = "data_starved"
	// StatusRateLimited means a provider hard floor is in force.
	StatusRateLimited HealthStatus = "rate_limited"
)

// DefaultHealthWindow is how recently a sport must have produced data for the
// appliance to count as healthy.
//
// Fifteen minutes is chosen against the slowest cadence we actually run, not
// against the fastest. Live football sweeps every few seconds, but a pre-game
// vertical polls every twelve minutes and golf rests at fifty-five between
// tournaments. A window tighter than the slowest legitimate interval would
// report a correctly-idling appliance as broken, which is the failure mode that
// gets health checks disabled.
const DefaultHealthWindow = 15 * time.Minute

// startupGrace is how long after boot a box with no data is called starting
// rather than starved.
//
// Without it every deploy flaps red for the first sweep, and a rolling restart
// across a venue estate would look like an outage.
const startupGrace = 90 * time.Second

// RecordPoll marks a successful provider response for a sport.
//
// Called even when the response carried no records. That is the point: an
// empty card is proof the provider is reachable and answering, which is
// exactly the evidence needed to tell a quiet night from a dead feed.
func (r *Registry) RecordPoll(sport string) {
	r.Sport(sport).LastPoll.Set(r.now())
}

// RecordData marks that a sport produced records for publication.
func (r *Registry) RecordData(sport string) {
	r.Sport(sport).LastData.Set(r.now())
}

// MarkRateLimited records whether a named provider is serving a hard floor.
//
// Keyed by provider rather than by sport because a floor is charged against a
// subscription: golf's 429 stops golf, and would stop every sport on that key
// if there were more than one.
func (r *Registry) MarkRateLimited(provider string, limited bool) {
	r.floorMu.Lock()
	defer r.floorMu.Unlock()
	if r.floors == nil {
		r.floors = map[string]bool{}
	}
	r.floors[provider] = limited
}

// rateLimitedProviders lists providers currently under a hard floor.
func (r *Registry) rateLimitedProviders() []string {
	r.floorMu.RLock()
	defer r.floorMu.RUnlock()
	var out []string
	for name, limited := range r.floors {
		if limited {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// Health evaluates readiness against a freshness window.
//
// Pass zero for DefaultHealthWindow.
func (r *Registry) Health(window time.Duration) Health {
	if window <= 0 {
		window = DefaultHealthWindow
	}
	now := r.now()
	uptime := now.Sub(r.started)

	h := Health{
		WindowSeconds:  window.Seconds(),
		UptimeSeconds:  uptime.Seconds(),
		DataAgeSeconds: -1,
		PollAgeSeconds: -1,
	}

	// Freshest data and freshest successful poll across every sport. "At least
	// one" is the licensed reading: a venue running nine sports out of season
	// and one in season is healthy, and demanding all ten would report that
	// correct state as an outage.
	r.mu.RLock()
	names := make([]string, 0, len(r.perSport))
	for name := range r.perSport {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		m := r.perSport[name]
		data, poll := m.LastData.Value(), m.LastPoll.Value()
		if !data.IsZero() && data.After(h.LastData) {
			h.LastData, h.LastDataSport = data, name
		}
		if !poll.IsZero() && poll.After(h.LastPoll) {
			h.LastPoll, h.LastPollSport = poll, name
		}
		age := -1.0
		if !data.IsZero() {
			age = now.Sub(data).Seconds()
		}
		h.Sports = append(h.Sports, SportFreshness{
			Sport: name, LastData: data, LastPoll: poll,
			DataAgeSeconds: age, Messages: m.Messages.Value(),
		})
	}
	r.mu.RUnlock()

	if !h.LastData.IsZero() {
		h.DataAgeSeconds = now.Sub(h.LastData).Seconds()
	}
	if !h.LastPoll.IsZero() {
		h.PollAgeSeconds = now.Sub(h.LastPoll).Seconds()
	}

	h.RateLimitedProviders = r.rateLimitedProviders()
	h.RateLimited = len(h.RateLimitedProviders) > 0

	// A hard floor outranks staleness. Both may be true at once — a floor
	// causes starvation — and reporting the floor is more useful, because it
	// names a cause and a remedy rather than only a symptom.
	switch {
	case h.RateLimited:
		h.OK = false
		h.Status = StatusRateLimited
		h.Detail = fmt.Sprintf("rate-limit hard floor in force for: %v", h.RateLimitedProviders)
	case !h.LastData.IsZero() && now.Sub(h.LastData) <= window:
		h.OK = true
		h.Status = StatusOK
		h.Detail = fmt.Sprintf("%s produced data %.0fs ago", h.LastDataSport, h.DataAgeSeconds)
	case h.LastData.IsZero() && uptime < startupGrace:
		// Deliberately healthy. See startupGrace.
		h.OK = true
		h.Status = StatusStarting
		h.Detail = fmt.Sprintf("no data yet; %.0fs into the %.0fs startup grace",
			uptime.Seconds(), startupGrace.Seconds())
	case h.LastData.IsZero():
		h.OK = false
		h.Status = StatusDataStarved
		h.Detail = "no sport has produced data since startup"
	default:
		h.OK = false
		h.Status = StatusDataStarved
		h.Detail = fmt.Sprintf("no data for %.0fs, window is %.0fs (last: %s)",
			h.DataAgeSeconds, window.Seconds(), h.LastDataSport)
	}
	return h
}

// HealthHandler serves the readiness probe.
//
// 200 when healthy, 503 otherwise, with the same JSON body either way so a
// human curling it after a page gets the reason without a second request.
// HEAD is answered with the status code alone, because that is all a
// load-balancer probe reads and rendering a body for it is waste.
func (r *Registry) HealthHandler(window time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		h := r.Health(window)
		code := http.StatusServiceUnavailable
		if h.OK {
			code = http.StatusOK
		}
		// A probe result must never be served from an intermediary cache.
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(code)
		if req.Method == http.MethodHead {
			return
		}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(h)
	})
}

// Timestamp is a concurrency-safe time, zero until first set.
//
// A Gauge holding a Unix second would have done, but loses monotonic ordering
// against a test clock and forces every reader to remember the unit.
type Timestamp struct {
	mu sync.RWMutex
	v  time.Time
}

// Set records a time.
func (t *Timestamp) Set(v time.Time) {
	t.mu.Lock()
	t.v = v
	t.mu.Unlock()
}

// Value returns the time, zero if never set.
func (t *Timestamp) Value() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.v
}

// IsZero reports whether the timestamp was never set.
func (t *Timestamp) IsZero() bool { return t.Value().IsZero() }
