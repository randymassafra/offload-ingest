// Command loadtest drives synthetic ingest traffic for the offload-intelligence
// pipeline. It runs two load profiles concurrently — steady REST polling and
// event-driven webhook bursts — across all 13 supported sports, and publishes
// everything to Kafka.
//
//	loadtest -brokers localhost:9092 -sports all -duration 60s
//	loadtest -dry-run -sports nfl,nba -poll-workers 4 -duration 10s
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/offloadintelligence/offload-ingest/config"
	"github.com/offloadintelligence/offload-ingest/internal/generators"
	"github.com/offloadintelligence/offload-ingest/internal/poller"
	"github.com/offloadintelligence/offload-ingest/internal/producer"
	"github.com/offloadintelligence/offload-ingest/internal/webhook"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest"
)

// version is stamped at build time via -ldflags (see the Makefile).
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

type options struct {
	// Kafka
	brokers       string
	topic         string
	topicPerSport bool
	topicPerFeed  bool
	compression   string
	acks          int
	batchSize     int
	batchTimeout  time.Duration
	async         bool
	saslMechanism string
	saslUsername  string
	saslPassword  string
	tlsEnabled    bool
	tlsSkipVerify bool

	// Load profile
	sports        string
	pollKinds     string
	burstKinds    string
	duration      time.Duration
	seed          int64
	pollWorkers   int
	pollInterval  time.Duration
	pollJitter    time.Duration
	eventsPerPoll int
	pollEndpoints string
	apiKey        string
	capturedDir   string
	envFile       string
	httpSport     string
	httpKind      string

	burstEmitters int
	burstSize     int
	burstJitter   int
	burstInterval time.Duration
	inBurstDelay  time.Duration

	// Licensing and mode
	mode          string
	licensePath   string
	dashboardAddr string
	metricsAddr   string
	flinkAddr     string
	flinkTTL      time.Duration
	noLicense     bool

	// Runtime
	webhookAddr   string
	statsEvery    time.Duration
	dryRun        bool
	stdout        bool
	logLevel      string
	showVersion   bool
	listEndpoints bool
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(1)
	}
}

func run() error {
	// Configuration is read once, before flags are parsed, so that environment
	// defaults are visible to flag defaulting and before anything reads a
	// credential. Real environment variables always win over the file.
	cfg, envErr := config.Load(preflightEnvFile())
	opts := parseFlags(cfg)
	if opts.showVersion {
		fmt.Printf("loadtest %s (commit %s, built %s)\n", version, commit, date)
		return nil
	}
	// Captures are registered before anything reads the catalog, so
	// -endpoints and every emitter see the replayed feeds.
	if opts.capturedDir != "" {
		loaded, err := generators.LoadCaptures(opts.capturedDir)
		if err != nil {
			return err
		}
		for _, c := range loaded {
			fmt.Fprintf(os.Stderr, "captured: %s/%s <- %s (%d document(s))\n", c.Sport, c.Kind, c.File, c.Docs)
		}
	}
	if opts.listEndpoints {
		printEndpoints()
		return nil
	}

	log := newLogger(opts.logLevel)
	if envErr != nil {
		return envErr
	}
	if cfg.Source != "" {
		// The path is logged, never the contents: this file holds live keys.
		log.Debug("loaded environment file", "path", cfg.Source)
	}
	// Redacted, always: enough to tell which credential is loaded, never
	// enough to use it.
	for name, key := range map[string]string{
		"api-sports": cfg.APISportsKey,
		"golf":       cfg.GolfAPIKey,
		"rapidapi":   cfg.RapidAPIKey,
	} {
		if key != "" {
			log.Debug("credential available", "provider", name, "key", config.Redact(key))
		}
	}

	sports, err := generators.ParseSportList(opts.sports)
	if err != nil {
		return err
	}
	pollKinds, err := generators.ParseKindList(opts.pollKinds)
	if err != nil {
		return err
	}
	burstKinds, err := generators.ParseKindList(opts.burstKinds)
	if err != nil {
		return err
	}
	if opts.seed == 0 {
		opts.seed = time.Now().UnixNano()
	}

	sink, closeSink, err := buildSink(opts, log)
	if err != nil {
		return err
	}
	defer closeSink()

	// The licence gate. Production is always licensed; simulation can be run
	// unlicensed with -no-license so a developer without a signed key can still
	// exercise the generators, but that flag can never reach production — the
	// licence is what carries the tier, and without a tier there is no budget
	// to rate-limit against.
	if opts.mode == "" {
		opts.mode = cfg.Mode
	}
	mode, err := ingest.ParseMode(opts.mode)
	if err != nil {
		return err
	}
	// Required only where it is actually needed. Simulation contacts no
	// provider and must keep running with no credentials at all — that is what
	// makes round-the-clock load testing possible on a metered plan.
	if mode == ingest.ModeProduction {
		if err := requireProviders(cfg, config.RequireAPISports); err != nil {
			log.Error("provider initialisation failed", "err", err)
			return err
		}
	}

	// Directly-constructed providers, threaded from configuration. Golf has no
	// API-Sports host and volleyball has no pipeline binding yet, so both are
	// reached through their own clients rather than the sweeper.
	providers := buildProviders(cfg, log)
	if providers.Golf != nil {
		log.Info("golf provider enabled", "host", golfHost(), "cache", providers.Golf.CachePath())
	}
	if providers.Volleyball != nil {
		log.Info("volleyball provider enabled", "vertical", volleyballVertical())
	}
	if opts.noLicense && mode == ingest.ModeProduction {
		return errors.New("-no-license cannot be used in production mode: " +
			"the licence carries the API tier the rate limiter is sized from")
	}

	// Ctrl-C and SIGTERM stop the run cleanly so the producer can flush.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if opts.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.duration)
		defer cancel()
	}

	var runtime *ingest.Runtime
	if !opts.noLicense {
		runtime, err = ingest.NewRuntime(ingest.RuntimeConfig{
			Mode:             string(mode),
			APIKey:           cfg.APISportsKey,
			LicensePublicKey: cfg.LicensePublicKey,
			LicensePath:      firstNonEmpty(opts.licensePath, cfg.LicensePath),
			Sports:           sports,
			Kinds:            pollKinds,
			Seed:             opts.seed,
			Batch:            opts.eventsPerPoll,
			GolfAPIKey:       cfg.GolfAPIKey,
			GolfCachePath:    cfg.GolfCachePath,
			FlinkAddr:        firstNonEmpty(opts.flinkAddr, cfg.FlinkAddr),
			FlinkTTL:         opts.flinkTTL,
			Logger:           log,
		})
		if err != nil {
			return err
		}
		defer runtime.Close()
		// The 24-hour verification ticker. It outlives nothing: the context is
		// the run's, so the goroutine ends when the process does.
		runtime.Watch(ctx)

		// Production replaces the generator-driven poller entirely — the whole
		// point of the DataStreamer seam. Simulation keeps the existing poller
		// and burst emitters, which is what makes round-the-clock load testing
		// possible on a plan that could never fund it live.
		if mode == ingest.ModeProduction {
			return runProduction(ctx, runtime, opts, sink, log)
		}
	} else {
		log.Warn("running unlicensed in simulation mode (-no-license)")
	}

	// Telemetry is attached to the sink so poll-to-Kafka latency and partition
	// balance are measured on the simulation path too, not only in production.
	if runtime != nil {
		attachTelemetry(sink, runtime, opts)
	}
	if opts.metricsAddr != "" && runtime != nil {
		stopMetrics, err := startMetrics(opts.metricsAddr, runtime, log)
		if err != nil {
			return err
		}
		defer stopMetrics()
	}
	if opts.dashboardAddr != "" {
		stopDash, err := startDashboard(opts.dashboardAddr, runtime, log)
		if err != nil {
			return err
		}
		defer stopDash()
	}

	log.Info("starting load test",
		"version", version,
		"mode", mode,
		"sports", len(sports),
		"poll_workers", opts.pollWorkers,
		"burst_emitters", opts.burstEmitters,
		"duration", opts.duration,
		"seed", opts.seed,
		"dry_run", opts.dryRun,
	)

	pool, err := buildPoller(opts, sports, pollKinds, sink, log)
	if err != nil {
		return err
	}
	emitter, err := buildEmitter(opts, sports, burstKinds, sink, log)
	if err != nil {
		return err
	}

	var (
		wg       sync.WaitGroup
		runErrMu sync.Mutex
		runErr   error
	)
	record := func(err error) {
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		runErrMu.Lock()
		defer runErrMu.Unlock()
		if runErr == nil {
			runErr = err
		}
	}

	if pool != nil {
		wg.Add(1)
		go func() { defer wg.Done(); record(pool.Run(ctx)) }()
	}
	if emitter != nil {
		wg.Add(1)
		go func() { defer wg.Done(); record(emitter.Run(ctx)) }()
	}
	if opts.webhookAddr != "" {
		recv, err := webhook.NewReceiver(webhook.ReceiverConfig{Addr: opts.webhookAddr}, sink)
		if err != nil {
			return err
		}
		log.Info("webhook receiver listening", "addr", opts.webhookAddr, "path", "/webhook")
		wg.Add(1)
		go func() { defer wg.Done(); record(recv.Run(ctx)) }()
	}
	if pool == nil && emitter == nil && opts.webhookAddr == "" {
		return errors.New("nothing to do: enable the poller, the burst emitter, or the webhook receiver")
	}

	if opts.statsEvery > 0 {
		wg.Add(1)
		go func() { defer wg.Done(); reportLoop(ctx, opts.statsEvery, pool, emitter, sink, log) }()
	}

	wg.Wait()
	// When -stdout is carrying payloads, the summary goes to stderr so the
	// payload stream stays valid newline-delimited JSON that can be piped
	// straight into a consumer or a schema check.
	summaryOut := os.Stdout
	if opts.stdout {
		summaryOut = os.Stderr
	}
	printSummary(summaryOut, pool, emitter, sink)
	return runErr
}

// preflightEnvFile scans os.Args for -env-file before the flag package runs, so
// the file can be loaded ahead of flag parsing.
func preflightEnvFile() string {
	for i, a := range os.Args[1:] {
		switch {
		case a == "-env-file" || a == "--env-file":
			if i+2 <= len(os.Args)-1 {
				return os.Args[i+2]
			}
		case strings.HasPrefix(a, "-env-file="):
			return strings.TrimPrefix(a, "-env-file=")
		case strings.HasPrefix(a, "--env-file="):
			return strings.TrimPrefix(a, "--env-file=")
		}
	}
	return ""
}

// parseFlags parses the command line, using cfg for any value the environment
// supplies. Flags win: an operator who typed it means it.
func parseFlags(cfg *config.Config) *options {
	o := &options{}
	kdef := producer.DefaultConfig()
	pdef := poller.DefaultConfig()
	wdef := webhook.DefaultConfig()

	flag.StringVar(&o.brokers, "brokers", strings.Join(kdef.Brokers, ","), "comma-separated Kafka bootstrap brokers")
	flag.StringVar(&o.topic, "topic", kdef.Topic, "destination topic (prefix when -topic-per-sport is set)")
	flag.BoolVar(&o.topicPerSport, "topic-per-sport", false, "publish each sport to <topic>.<sport>")
	flag.BoolVar(&o.topicPerFeed, "topic-per-feed", false, "append the feed kind to the topic, e.g. <topic>.<sport>.boxscore")
	flag.StringVar(&o.compression, "compression", kdef.Compression, "none|gzip|snappy|lz4|zstd")
	flag.IntVar(&o.acks, "acks", kdef.RequiredAcks, "required acks: 0 none, 1 leader, -1 all")
	flag.IntVar(&o.batchSize, "batch-size", kdef.BatchSize, "producer batch size in messages")
	flag.DurationVar(&o.batchTimeout, "batch-timeout", kdef.BatchTimeout, "producer linger before flushing a partial batch")
	flag.BoolVar(&o.async, "async", kdef.Async, "fire-and-forget writes (higher throughput, errors reported asynchronously)")
	flag.StringVar(&o.saslMechanism, "sasl", "", "SASL mechanism: plain|scram-sha-256|scram-sha-512")
	flag.StringVar(&o.saslUsername, "sasl-user", "", "SASL username")
	flag.StringVar(&o.saslPassword, "sasl-pass", "", "SASL password (prefer KAFKA_SASL_PASSWORD)")
	flag.BoolVar(&o.tlsEnabled, "tls", false, "connect to the broker over TLS")
	flag.BoolVar(&o.tlsSkipVerify, "tls-skip-verify", false, "skip broker certificate verification")

	flag.StringVar(&o.sports, "sports", "all", "comma-separated sports, or \"all\" for every supported feed")
	flag.DurationVar(&o.duration, "duration", 0, "how long to run; 0 runs until interrupted")
	flag.Int64Var(&o.seed, "seed", 0, "RNG seed; 0 picks one from the clock. Reuse a seed to replay a run")

	flag.IntVar(&o.pollWorkers, "poll-workers", pdef.Workers, "concurrent REST polling workers (0 disables polling)")
	flag.DurationVar(&o.pollInterval, "poll-interval", pdef.Interval, "base interval between polls")
	flag.DurationVar(&o.pollJitter, "poll-jitter", pdef.Jitter, "random jitter added to each poll interval")
	flag.IntVar(&o.eventsPerPoll, "events-per-poll", pdef.EventsPerPoll, "events returned by a single poll")
	flag.StringVar(&o.pollEndpoints, "poll-endpoints", "", "comma-separated live provider URLs to poll; empty uses the in-process mock provider")
	flag.StringVar(&o.apiKey, "api-key", "", "provider key for -poll-endpoints")
	flag.StringVar(&o.envFile, "env-file", "", "path to a .env file; empty searches the working directory and its parents")
	flag.StringVar(&o.capturedDir, "captured-dir", "", "directory of captured provider responses (<sport>.<kind>.json) to replay instead of simulating")
	flag.StringVar(&o.httpSport, "poll-endpoint-sport", "nfl", "sport label applied to payloads from -poll-endpoints")
	flag.StringVar(&o.httpKind, "poll-endpoint-kind", "boxscore", "feed kind label applied to payloads from -poll-endpoints")
	flag.StringVar(&o.pollKinds, "poll-kinds", "boxscore,playerstats,playbyplay", "feed kinds the pollers cover: reference|boxscore|playbyplay|playerstats|telemetry, or all")

	flag.IntVar(&o.burstEmitters, "burst-emitters", wdef.Emitters, "concurrent webhook burst sources (0 disables bursts)")
	flag.IntVar(&o.burstSize, "burst-size", wdef.BurstSize, "mean events per burst")
	flag.IntVar(&o.burstJitter, "burst-jitter", wdef.BurstJitter, "+/- variation applied to burst size")
	flag.DurationVar(&o.burstInterval, "burst-interval", wdef.Interval, "mean gap between bursts")
	flag.DurationVar(&o.inBurstDelay, "in-burst-delay", 0, "delay between events within a burst; 0 publishes the burst as one batch")
	flag.StringVar(&o.burstKinds, "burst-kinds", "telemetry,playbyplay", "feed kinds the webhook emitters push, or all")

	flag.StringVar(&o.mode, "mode", "", "simulation or production; defaults to OFFLOAD_MODE, then simulation")
	flag.StringVar(&o.licensePath, "license", "", "licence file; defaults to OFFLOAD_LICENSE_PATH, then license.key")
	flag.StringVar(&o.dashboardAddr, "dashboard-addr", cfg.DashboardAddr, "listen address for the local dashboard, e.g. :8090")
	// 9102, not 9090: 9090 is Prometheus server's own default port, and an
	// appliance that also runs Prometheus locally would have the two fight over
	// the bind. 9100 is node_exporter's, so the next free slot in the exporter
	// range is used.
	flag.StringVar(&o.metricsAddr, "metrics-addr", cfg.MetricsAddr, "listen address for the Prometheus /metrics endpoint, e.g. :9102")
	flag.StringVar(&o.flinkAddr, "flink-addr", "", "optional Flink JobManager REST endpoint to scrape state size from, e.g. http://flink:8081")
	flag.DurationVar(&o.flinkTTL, "flink-ttl", 10*time.Hour, "configured Flink state retention, used to scale the state gauge")
	flag.BoolVar(&o.noLicense, "no-license", false, "run the generators without a licence (simulation only; refuses production)")

	flag.StringVar(&o.webhookAddr, "webhook-addr", "", "listen address for the inbound webhook receiver, e.g. :8088")
	flag.DurationVar(&o.statsEvery, "stats-every", 10*time.Second, "interval between progress reports; 0 disables them")
	flag.BoolVar(&o.dryRun, "dry-run", false, "generate and count events without connecting to Kafka")
	flag.BoolVar(&o.stdout, "stdout", false, "also print every event as newline-delimited JSON")
	flag.StringVar(&o.logLevel, "log-level", "info", "debug|info|warn|error")
	flag.BoolVar(&o.showVersion, "version", false, "print version and exit")
	flag.BoolVar(&o.listEndpoints, "endpoints", false, "list every provider endpoint the generators cover, then exit")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(),
			"loadtest drives synthetic provider-shaped ingest traffic across %d sports\n"+
				"and %d endpoints. Run with -endpoints to list them.\n\n"+
				"Usage:\n  loadtest [flags]\n\nSports: %s\nFeed kinds: reference, boxscore, playbyplay, playerstats, telemetry\n\nFlags:\n",
			len(generators.AllSports), len(generators.Endpoints()), sportList())
		flag.PrintDefaults()
	}
	flag.Parse()

	if env := cfg.KafkaSASLPassword; env != "" {
		o.saslPassword = env
	}
	if o.apiKey == "" {
		o.apiKey = cfg.APISportsKey
	}
	if env := cfg.KafkaBrokers; env != "" && !flagPassed("brokers") {
		o.brokers = env
	}
	return o
}

func buildSink(o *options, log *slog.Logger) (producer.Publisher, func(), error) {
	var sinks []producer.Publisher

	if o.dryRun {
		log.Info("dry run: events are counted and discarded")
		sinks = append(sinks, producer.NewDiscard())
	} else {
		cfg := producer.DefaultConfig()
		cfg.Brokers = splitCSV(o.brokers)
		cfg.Topic = o.topic
		cfg.TopicPerSport = o.topicPerSport
		cfg.TopicPerFeed = o.topicPerFeed
		cfg.Compression = o.compression
		cfg.RequiredAcks = o.acks
		cfg.BatchSize = o.batchSize
		cfg.BatchTimeout = o.batchTimeout
		cfg.Async = o.async
		cfg.SASLMechanism = o.saslMechanism
		cfg.SASLUsername = o.saslUsername
		cfg.SASLPassword = o.saslPassword
		cfg.TLSEnabled = o.tlsEnabled
		cfg.TLSSkipVerify = o.tlsSkipVerify

		k, err := producer.NewKafka(cfg)
		if err != nil {
			return nil, nil, err
		}
		log.Info("kafka producer configured", "brokers", cfg.Brokers, "topic", cfg.Topic, "async", cfg.Async, "compression", cfg.Compression)
		sinks = append(sinks, k)
	}

	if o.stdout {
		sinks = append(sinks, producer.NewWriter(os.Stdout))
	}

	sink := sinks[0]
	if len(sinks) > 1 {
		sink = producer.NewMulti(sinks...)
	}
	return sink, func() {
		if err := sink.Close(); err != nil {
			log.Error("closing sink", "err", err)
		}
	}, nil
}

func buildPoller(o *options, sports []generators.Sport, kinds []generators.FeedKind, sink producer.Publisher, log *slog.Logger) (*poller.Pool, error) {
	if o.pollWorkers <= 0 {
		return nil, nil
	}
	cfg := poller.Config{
		Sports:        sports,
		Kinds:         kinds,
		Workers:       o.pollWorkers,
		Interval:      o.pollInterval,
		Jitter:        o.pollJitter,
		EventsPerPoll: o.eventsPerPoll,
		Seed:          o.seed,
		OnError: func(worker int, err error) {
			log.Warn("poll failed", "worker", worker, "err", err)
		},
	}
	if eps := splitCSV(o.pollEndpoints); len(eps) > 0 {
		sport, err := generators.ParseSport(o.httpSport)
		if err != nil {
			return nil, err
		}
		kind, err := generators.ParseKind(o.httpKind)
		if err != nil {
			return nil, err
		}
		f, err := poller.NewHTTPFetcher(eps, o.pollWorkers, 5*time.Second, o.apiKey, sport, kind)
		if err != nil {
			return nil, err
		}
		cfg.Fetcher = f
		log.Info("polling live provider endpoints", "count", len(eps), "sport", sport, "kind", kind, "authenticated", o.apiKey != "")
	}
	return poller.New(cfg, sink)
}

func buildEmitter(o *options, sports []generators.Sport, kinds []generators.FeedKind, sink producer.Publisher, log *slog.Logger) (*webhook.Emitter, error) {
	if o.burstEmitters <= 0 {
		return nil, nil
	}
	return webhook.New(webhook.Config{
		Sports:       sports,
		Kinds:        kinds,
		Emitters:     o.burstEmitters,
		BurstSize:    o.burstSize,
		BurstJitter:  o.burstJitter,
		Interval:     o.burstInterval,
		InBurstDelay: o.inBurstDelay,
		Seed:         o.seed + 1,
		OnError: func(emitter int, err error) {
			log.Warn("burst failed", "emitter", emitter, "err", err)
		},
	}, sink)
}

// reportLoop prints a progress line on an interval until ctx is cancelled.
func reportLoop(ctx context.Context, every time.Duration, pool *poller.Pool, em *webhook.Emitter, sink producer.Publisher, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			attrs := []any{}
			if pool != nil {
				s := pool.Stats()
				attrs = append(attrs, "polls", s.Polls, "poll_events", s.Events, "poll_eps", round1(s.EventsPerSecond()), "poll_errors", s.Errors)
			}
			if em != nil {
				s := em.Stats()
				attrs = append(attrs, "bursts", s.Bursts, "burst_events", s.Events, "burst_eps", round1(s.EventsPerSecond()), "burst_errors", s.Errors)
			}
			if r, ok := sink.(producer.StatsReporter); ok {
				s := r.Stats()
				attrs = append(attrs, "published", s.Published, "publish_errors", s.Errors, "mb", round1(float64(s.Bytes)/(1<<20)))
			}
			log.Info("progress", attrs...)
		}
	}
}

// printSummary writes a machine-readable summary at the end of a run.
func printSummary(out *os.File, pool *poller.Pool, em *webhook.Emitter, sink producer.Publisher) {
	summary := map[string]any{}
	if pool != nil {
		s := pool.Stats()
		summary["poller"] = map[string]any{
			"polls": s.Polls, "events": s.Events, "errors": s.Errors,
			"workers": s.Workers, "events_per_second": round1(s.EventsPerSecond()),
			"poll_latency_avg_ms": round1(s.LatencyAvgMs),
		}
	}
	if em != nil {
		s := em.Stats()
		summary["webhook"] = map[string]any{
			"bursts": s.Bursts, "events": s.Events, "errors": s.Errors,
			"emitters": s.Emitters, "peak_burst": s.PeakBurst,
			"events_per_second": round1(s.EventsPerSecond()),
		}
	}
	if r, ok := sink.(producer.StatsReporter); ok {
		summary["producer"] = r.Stats()
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	_ = enc.Encode(summary)
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

func splitCSV(s string) []string {
	out := []string{}
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func sportList() string {
	names := make([]string, 0, len(generators.AllSports))
	for _, s := range generators.AllSports {
		names = append(names, string(s))
	}
	return strings.Join(names, ", ")
}

func flagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func round1(f float64) float64 { return float64(int(f*10+0.5)) / 10 }

// printEndpoints lists the generated catalog, flagging the feeds whose schema
// could not be verified against a live provider.
func printEndpoints() {
	eps := generators.Endpoints()
	fmt.Printf("%-8s %-20s %-13s %-10s %-22s %-46s %s\n",
		"SPORT", "FEED", "PROVIDER", "SCHEMA", "MODEL", "ENDPOINT", "PROJECTION")

	counts := map[generators.Provenance]int{}
	byProvider := map[generators.Provider]int{}
	for _, ep := range eps {
		counts[ep.Provenance]++
		byProvider[ep.Provider]++
		schema := string(ep.Provenance)
		if ep.Replayed {
			schema = "replayed"
		}
		projection := ep.Projection
		if projection == "" {
			projection = "(whole response)"
		}
		fmt.Printf("%-8s %-20s %-13s %-10s %-22s %-46s %s\n",
			ep.Sport, ep.Ref(), ep.Provider, schema, ep.Model, ep.Path, projection)
	}

	fmt.Printf("\n%d endpoints across %d sports.\n", len(eps), len(generators.AllSports))
	fmt.Println("Upstream providers:")
	for _, p := range append(generators.AllProviders, generators.ProviderNone) {
		if n := byProvider[p]; n > 0 {
			note := ""
			if p == generators.ProviderNone {
				note = "  (no provider offers these sports)"
			}
			fmt.Printf("  %-13s %3d%s\n", p, n, note)
		}
	}
	fmt.Println("Schema evidence, strongest first:")
	for _, p := range []struct {
		prov generators.Provenance
		note string
	}{
		{generators.ProvenanceCaptured, "diffed against a real provider response"},
		{generators.ProvenanceOpenAPI, "from the published OpenAPI spec, not yet captured"},
		{generators.ProvenanceDataDict, "from a data-dictionary page, not yet captured"},
		{generators.ProvenanceInferred, "modelled on a sibling API's captured shape"},
		{generators.ProvenanceModeled, "no authoritative source"},
	} {
		if n := counts[p.prov]; n > 0 {
			fmt.Printf("  %-9s %3d  %s\n", p.prov, n, p.note)
		}
	}
	if counts[generators.ProvenanceModeled] > 0 {
		fmt.Println("\nMODELED feeds follow their provider's conventions but describe no verified")
		fmt.Println("endpoint. See the provider package's doc comment before trusting them.")
	}
}

// firstNonEmpty returns the first non-empty value.
//
// The precedence it encodes is flag over environment: an operator who passes
// -license on the command line means it, and a value inherited from a .env
// should never silently win over one they typed.
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
