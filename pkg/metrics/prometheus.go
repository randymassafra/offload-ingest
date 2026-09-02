package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Prometheus exposition.
//
// Written by hand rather than pulled in from client_golang, and it is worth
// saying why given the earlier decision in this package went the other way.
//
// The exposition format is a stable, documented, line-oriented text format, and
// what this process exposes is a fixed set of metrics known at compile time —
// there is no dynamic registration, no collector plugin surface, no need for
// the registry, the gatherer or the descriptor machinery that makes up most of
// client_golang. Against that, the library and its transitive dependencies are
// several megabytes and a supply-chain surface on an appliance that ships to
// venues. Hand-writing ~200 lines of well-tested formatting is the smaller,
// more auditable option here. A test asserts the output parses.
//
// If the suite later standardises on client_golang across all four products,
// this file is the only thing that changes.

// Namespace prefixes every metric, so a Prometheus server scraping all four
// Offload products can tell them apart.
const Namespace = "offload_ingest"

// WritePrometheus renders the registry in the text exposition format.
func (r *Registry) WritePrometheus(w io.Writer, product string) error {
	e := &promEncoder{w: w, product: product}

	e.counter("up", "1 when the process is serving metrics.", 1, nil)
	e.gauge("build_info", "Build and design-system versions, as labels.", 1,
		labels{{"product", product}})
	e.gauge("uptime_seconds", "Seconds since process start.", r.Uptime().Seconds(), nil)

	// --- throughput ---------------------------------------------------------
	e.counter("requests_total", "Upstream requests issued.", float64(r.Requests.Value()), nil)
	e.counter("messages_total", "Messages published downstream.", float64(r.Messages.Value()), nil)
	e.counter("sweeps_total", "Bulk sweeps performed.", float64(r.Sweeps.Value()), nil)
	e.counter("throttles_total", "HTTP 429 responses received.", float64(r.Throttles.Value()), nil)
	e.counter("retries_total", "Requests retried after a throttle.", float64(r.Retries.Value()), nil)
	e.gauge("requests_per_second", "Request rate over a sliding minute.", r.RequestRate.PerSecond(), nil)

	// --- latency ------------------------------------------------------------
	e.histogram("publish_latency_ms",
		"Poll-to-Kafka latency: age of a record when handed to the producer.",
		r.PublishLatency, nil)
	e.histogram("request_latency_ms",
		"Upstream HTTP round-trip latency.", r.RequestLatency, nil)

	// --- real-time fidelity -------------------------------------------------
	e.histogram("ingest_age_seconds",
		"Age of a record when it entered the pipeline.", r.IngestAge, nil)
	e.gauge("provider_clock_skew_seconds",
		"Provider clock minus local clock, from the HTTP Date header.",
		r.ProviderSkew.Value(), nil)
	e.histogram("live_match_lag_seconds",
		"Provider data lag behind live play, first-half fixtures only.",
		r.LiveMatchLag, nil)

	// --- per sport ----------------------------------------------------------
	r.mu.RLock()
	names := make([]string, 0, len(r.perSport))
	for n := range r.perSport {
		names = append(names, n)
	}
	sports := make(map[string]*SportMetrics, len(names))
	for _, n := range names {
		sports[n] = r.perSport[n]
	}
	r.mu.RUnlock()
	sort.Strings(names)

	e.help("sport_requests_total", "counter", "Upstream requests, by sport.")
	for _, n := range names {
		e.sample("sport_requests_total", labels{{"sport", n}}, float64(sports[n].Requests.Value()))
	}
	e.help("sport_messages_total", "counter", "Messages published, by sport.")
	for _, n := range names {
		e.sample("sport_messages_total", labels{{"sport", n}}, float64(sports[n].Messages.Value()))
	}

	// Errors carry a class label so a 4xx (our request is wrong) is
	// distinguishable from a 5xx (the provider is failing) and from a transport
	// failure that never reached them at all.
	e.help("sport_errors_total", "counter", "Upstream errors, by sport and status class.")
	for _, n := range names {
		m := sports[n]
		e.sample("sport_errors_total", labels{{"sport", n}, {"class", "4xx"}}, float64(m.Errors4xx.Value()))
		e.sample("sport_errors_total", labels{{"sport", n}, {"class", "5xx"}}, float64(m.Errors5xx.Value()))
		e.sample("sport_errors_total", labels{{"sport", n}, {"class", "transport"}}, float64(m.ErrorsTransport.Value()))
	}

	e.help("sport_throttles_total", "counter", "HTTP 429 responses, by sport.")
	for _, n := range names {
		e.sample("sport_throttles_total", labels{{"sport", n}}, float64(sports[n].Throttles.Value()))
	}
	e.help("sport_rate_limit_tokens", "gauge", "Rate-limiter tokens currently available, by sport.")
	for _, n := range names {
		e.sample("sport_rate_limit_tokens", labels{{"sport", n}}, sports[n].Tokens.Value())
	}
	e.help("sport_quota_remaining", "gauge", "Provider-reported daily quota remaining, by sport.")
	for _, n := range names {
		e.sample("sport_quota_remaining", labels{{"sport", n}}, sports[n].QuotaRemaining.Value())
	}
	e.help("sport_crowd_weight", "gauge", "Crowd-interest share of the request budget, by sport.")
	for _, n := range names {
		e.sample("sport_crowd_weight", labels{{"sport", n}}, sports[n].CrowdWeight.Value())
	}

	// --- dropped records ----------------------------------------------------
	//
	// Labelled by sport and reason so a licence mismatch (out_of_scope) is
	// distinguishable from a modelling gap (unidentified) in a query, not only
	// on the dashboard.
	if drops := r.Drops(); len(drops) > 0 {
		e.help("dropped_records_total", "counter",
			"Records refused before publication, by sport and reason.")
		for _, d := range drops {
			e.sample("dropped_records_total",
				labels{{"sport", d.Sport}, {"reason", d.Reason}}, float64(d.Count))
		}
	}
	e.help("sport_drop_rate", "gauge",
		"Dropped share of a sport's feed. Above 0.05 the dashboard warns of a licence mismatch.")
	for _, n := range names {
		e.sample("sport_drop_rate", labels{{"sport", n}}, r.DropRate(n))
	}

	// --- kafka partitions ---------------------------------------------------
	parts := r.Partitions()
	if len(parts) > 0 {
		e.help("kafka_partition_writes_total", "counter", "Messages written, by topic partition.")
		for _, p := range parts {
			e.sample("kafka_partition_writes_total",
				labels{{"topic", p.Topic}, {"partition", strconv.Itoa(p.Partition)}},
				float64(p.Writes))
		}
	}
	e.gauge("kafka_partition_skew",
		"Hot-partition indicator: (max writes - mean) / mean. 0 is balanced.",
		r.PartitionSkew(), nil)

	// --- host ---------------------------------------------------------------
	if r.Host.Available.Value() {
		e.gauge("host_cpu_percent", "Edge host CPU utilisation.", r.Host.CPUPercent.Value(), nil)
		e.gauge("host_memory_used_bytes", "Edge host memory in use.", r.Host.MemUsed.Value(), nil)
		e.gauge("host_memory_total_bytes", "Edge host memory installed.", r.Host.MemTotal.Value(), nil)
		e.gauge("host_load1", "Edge host 1-minute load average.", r.Host.LoadAvg1.Value(), nil)
	}
	e.gauge("process_memory_bytes", "Memory obtained from the OS by this process.",
		r.Host.ProcessRSS.Value(), nil)
	e.gauge("process_goroutines", "Goroutines currently running.", r.Host.Goroutines.Value(), nil)

	// --- golf ---------------------------------------------------------------
	//
	// Exported only once the feed has set a cadence, so a deployment that does
	// not run golf produces no series rather than a misleading zero.
	if r.Golf.CadenceMinutes.Value() > 0 {
		e.gauge("golf_cadence_minutes",
			"Minutes between golf leaderboard polls. Falls with tournament activity and is forced back to the resting rate by a 429.",
			r.Golf.CadenceMinutes.Value(), nil)
		e.gauge("golf_throttled",
			"1 while the RapidAPI 429 hard floor is holding the golf cadence at its resting rate.",
			boolValue(r.Golf.Throttled.Value()), nil)
	}

	// --- flink --------------------------------------------------------------
	// Exported only when configured. A gauge that is always zero because
	// nothing is being scraped is worse than an absent one: it graphs as a
	// healthy flat line.
	if r.Flink.Configured.Value() {
		e.gauge("flink_reachable", "1 when the Flink JobManager answered the last scrape.",
			boolValue(r.Flink.Reachable.Value()), nil)
		e.gauge("flink_state_bytes", "Total checkpointed state across running jobs.",
			r.Flink.StateBytes.Value(), nil)
		e.gauge("flink_checkpoint_age_seconds", "Age of the newest completed checkpoint.",
			r.Flink.CheckpointAge.Value(), nil)
		e.gauge("flink_state_ttl_seconds", "Configured state retention.",
			r.Flink.TTLSeconds.Value(), nil)
	}
	return e.err
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// --- encoder ----------------------------------------------------------------

type label struct{ name, value string }
type labels []label

type promEncoder struct {
	w       io.Writer
	product string
	err     error
	// seen guards against emitting two HELP lines for one metric name, which
	// makes a scrape fail outright rather than degrade.
	seen map[string]bool
}

func (e *promEncoder) help(name, typ, help string) {
	if e.seen == nil {
		e.seen = map[string]bool{}
	}
	full := Namespace + "_" + name
	if e.seen[full] {
		return
	}
	e.seen[full] = true
	e.printf("# HELP %s %s\n", full, help)
	e.printf("# TYPE %s %s\n", full, typ)
}

func (e *promEncoder) sample(name string, l labels, v float64) {
	e.printf("%s%s %s\n", Namespace+"_"+name, renderLabels(l), formatFloat(v))
}

func (e *promEncoder) counter(name, help string, v float64, l labels) {
	e.help(name, "counter", help)
	e.sample(name, l, v)
}

func (e *promEncoder) gauge(name, help string, v float64, l labels) {
	e.help(name, "gauge", help)
	e.sample(name, l, v)
}

func (e *promEncoder) histogram(name, help string, h *Histogram, base labels) {
	e.help(name, "histogram", help)
	bounds, counts := h.Cumulative()
	for i, b := range bounds {
		l := append(append(labels{}, base...), label{"le", formatFloat(b)})
		e.printf("%s_bucket%s %d\n", Namespace+"_"+name, renderLabels(l), counts[i])
	}
	l := append(append(labels{}, base...), label{"le", "+Inf"})
	e.printf("%s_bucket%s %d\n", Namespace+"_"+name, renderLabels(l), counts[len(counts)-1])
	e.printf("%s_sum%s %s\n", Namespace+"_"+name, renderLabels(base), formatFloat(h.Sum()))
	e.printf("%s_count%s %d\n", Namespace+"_"+name, renderLabels(base), h.Count())
}

func (e *promEncoder) printf(format string, args ...any) {
	if e.err != nil {
		return
	}
	_, e.err = fmt.Fprintf(e.w, format, args...)
}

func renderLabels(l labels) string {
	if len(l) == 0 {
		return ""
	}
	parts := make([]string, 0, len(l))
	for _, kv := range l {
		parts = append(parts, kv.name+`="`+escapeLabel(kv.value)+`"`)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// escapeLabel applies the exposition format's escaping rules. A sport name or a
// topic containing a quote would otherwise produce a line Prometheus rejects.
func escapeLabel(v string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(v)
}

// formatFloat renders a value the way the exposition format expects: no
// exponent for ordinary magnitudes, and the literal +Inf/NaN tokens rather than
// Go's spelling.
func formatFloat(v float64) string {
	switch {
	case v != v:
		return "NaN"
	case v > 1e308:
		return "+Inf"
	case v < -1e308:
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// --- handler ----------------------------------------------------------------

// PrometheusHandler serves the exposition endpoint.
func (r *Registry) PrometheusHandler(product string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if err := r.WritePrometheus(w, product); err != nil {
			// The status is already written by then; there is nothing useful
			// left to say to the scraper, so this only avoids a silent partial.
			return
		}
	})
}

// ServeMetrics starts a dedicated listener for the exposition endpoint.
//
// Its own listener, not a route on the dashboard, because the two have
// different audiences and different exposure: the dashboard is for a human on
// the venue LAN, and /metrics is for a scraper that may be firewalled
// separately. Returns the bound address.
func (r *Registry) ServeMetrics(addr, product string) (string, *http.Server, error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", r.PrometheusHandler(product))
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, "/metrics", http.StatusFound)
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return addr, srv, nil
}
