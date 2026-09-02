package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/offloadintelligence/offload-ingest/internal/dashboard"
	"github.com/offloadintelligence/offload-ingest/internal/producer"
	"github.com/offloadintelligence/offload-ingest/pkg/ingest"
)

// runProduction drives the pipeline from API-Sports.
//
// It is deliberately a different loop from the simulation poller, and much
// simpler. The simulation side has workers, intervals and jitter because it is
// trying to generate load; production has none of those, because the request
// budget decides the pace and the scheduler already knows what it can afford.
// Adding worker concurrency here would only mean racing to spend the same
// hundred requests faster.
func runProduction(ctx context.Context, rt *ingest.Runtime, opts *options, sink producer.Publisher, log *slog.Logger) error {
	attachTelemetry(sink, rt, opts)
	if opts.metricsAddr != "" {
		stop, err := startMetrics(opts.metricsAddr, rt, log)
		if err != nil {
			return err
		}
		defer stop()
	}
	if opts.dashboardAddr != "" {
		stop, err := startDashboard(opts.dashboardAddr, rt, log)
		if err != nil {
			return err
		}
		defer stop()
	}

	streamer := rt.Streamer()
	log.Info("production ingest starting",
		"provider", "api-sports",
		"sports", streamer.Sports(),
		"dashboard", opts.dashboardAddr)

	registry := rt.Registry()
	var published int64
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				snap := registry.Snapshot()
				log.Info("production ingest",
					"requests", snap.Requests, "messages", snap.Messages,
					"429s", snap.Throttles, "errors", snap.Errors,
					"req_per_min", snap.RequestsPerMin)
			}
		}
	}()

	for {
		msgs, err := streamer.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				log.Info("production ingest stopped", "published", published)
				return nil
			}
			return err
		}
		for _, m := range msgs {
			if err := sink.Publish(ctx, m); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				log.Error("publish failed", "err", err, "sport", m.Sport)
				registry.Errors.Inc()
				continue
			}
			published++
		}
	}
}

// attachTelemetry wires publication metrics into whichever sink is in use.
//
// Type-switched rather than added to the Publisher interface, because a sink
// that cannot report telemetry — the stdout writer, a fan-out — is still a
// perfectly good sink, and widening the interface would force every one of them
// to carry a method it has nothing to say through.
func attachTelemetry(sink producer.Publisher, rt *ingest.Runtime, opts *options) {
	obs := ingest.NewPublishObserver(rt.Registry())
	switch s := sink.(type) {
	case *producer.Kafka:
		s.SetObserver(obs)
	case *producer.Discard:
		s.SetObserver(obs)
		// Without a broker there is no real partition, but the skew question is
		// decidable offline because the balancer is a pure function of the key.
		// Projecting it lets a venue find a hot-partition problem before
		// deploying; the dashboard labels the figure as projected.
		s.SetProjectedPartitions(defaultProjectedPartitions)
	}
}

// defaultProjectedPartitions is the assumed topic width for dry-run skew
// projection. Six is the Compose stack's default.
const defaultProjectedPartitions = 6

// startMetrics serves the Prometheus exposition endpoint on its own listener.
//
// Separate from the dashboard because the two have different audiences and
// different exposure: the dashboard is for a human on the venue LAN, and
// /metrics is for a scraper that may be firewalled differently.
func startMetrics(addr string, rt *ingest.Runtime, log *slog.Logger) (func(), error) {
	_, srv, err := rt.Registry().ServeMetrics(addr, dashboard.ProductName)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics: cannot bind %s: %w", addr, err)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Error("metrics: server stopped", "err", err)
		}
	}()
	log.Info("prometheus metrics listening", "addr", "http://"+ln.Addr().String()+"/metrics")
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}

// startDashboard binds the operator page and returns a shutdown func.
//
// A nil runtime is valid: an unlicensed simulation run still gets a dashboard,
// it simply has no licence or budget panel to show.
func startDashboard(addr string, rt *ingest.Runtime, log *slog.Logger) (func(), error) {
	var provider dashboard.Provider
	if rt != nil {
		provider = rt
	}
	srv := dashboard.New(dashboard.Config{
		Addr: addr, Provider: provider, Version: version, Logger: log,
	})
	bound, err := srv.Start()
	if err != nil {
		return nil, err
	}
	log.Info("dashboard listening", "addr", "http://"+bound)
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}
