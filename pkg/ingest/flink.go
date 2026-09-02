package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/offloadintelligence/offload-ingest/pkg/metrics"
)

// Flink state observation.
//
// # Why this is optional, and off by default
//
// offload-ingest is the PRODUCER. Flink is a separate process — a different
// job, usually a different host, and in this suite a different product. A
// process cannot measure another process's state buffer, so the State-TTL gauge
// the DDS asks for cannot be a native metric here.
//
// The architectural answer is that the card belongs to the Flink product's own
// dashboard, where the state actually lives. That is the default: with no
// endpoint configured, the Ingest dashboard shows the card as "not configured"
// and says where the metric belongs, rather than displaying a confident zero
// for something it cannot see.
//
// The scraper below exists for venues that want the number surfaced on the
// ingest box anyway. It reads Flink's own REST API, which is the only honest
// way to get it: every figure is Flink's, not ours.
//
// Endpoint used: GET /jobs/overview, then per-job /jobs/{id}/checkpoints. Both
// are stable across Flink 1.13+.

// FlinkConfig configures the optional state scraper.
type FlinkConfig struct {
	// Addr is the JobManager REST endpoint, e.g. http://flink:8081. Empty
	// disables the scraper entirely.
	Addr string
	// TTL is the configured state retention, used to scale the gauge. The
	// pipeline's design target is 10 hours.
	TTL time.Duration
	// Interval between scrapes. State size changes on checkpoint boundaries,
	// so polling faster than the checkpoint interval learns nothing.
	Interval time.Duration
	Logger   *slog.Logger
	Client   *http.Client
}

// DefaultFlinkTTL is the pipeline's designed state retention.
const DefaultFlinkTTL = 10 * time.Hour

// StartFlinkCollector begins scraping Flink, if an address was configured.
//
// It returns whether a collector was actually started, so the caller can report
// the distinction between "configured and unreachable" and "not configured" —
// which are very different operational situations.
func StartFlinkCollector(ctx context.Context, cfg FlinkConfig, reg *metrics.Registry) bool {
	if strings.TrimSpace(cfg.Addr) == "" {
		reg.Flink.Configured.Set(false)
		return false
	}
	if cfg.TTL <= 0 {
		cfg.TTL = DefaultFlinkTTL
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 10 * time.Second}
	}
	reg.Flink.Configured.Set(true)
	reg.Flink.TTLSeconds.Set(cfg.TTL.Seconds())

	c := &flinkCollector{cfg: cfg, reg: reg}
	go func() {
		c.scrape(ctx)
		t := time.NewTicker(cfg.Interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.scrape(ctx)
			}
		}
	}()
	return true
}

type flinkCollector struct {
	cfg FlinkConfig
	reg *metrics.Registry
}

type flinkJobsOverview struct {
	Jobs []struct {
		JID   string `json:"jid"`
		Name  string `json:"name"`
		State string `json:"state"`
	} `json:"jobs"`
}

type flinkCheckpoints struct {
	Latest struct {
		Completed *struct {
			StateSize   int64 `json:"state_size"`
			LatestAckTS int64 `json:"latest_ack_timestamp"`
		} `json:"completed"`
	} `json:"latest"`
}

func (c *flinkCollector) scrape(ctx context.Context) {
	jobs, err := c.jobs(ctx)
	if err != nil {
		c.reg.Flink.Reachable.Set(false)
		c.cfg.Logger.Debug("flink: state scrape failed", "addr", c.cfg.Addr, "err", err)
		return
	}

	// Sum across running jobs. A pipeline is usually one job, but a venue
	// running a second enrichment job shares the same state backend and the
	// same memory budget, so the interesting number is the total.
	var totalBytes int64
	var newestAck int64
	var counted int
	for _, j := range jobs.Jobs {
		if !strings.EqualFold(j.State, "RUNNING") {
			continue
		}
		size, ack, err := c.checkpoint(ctx, j.JID)
		if err != nil {
			continue
		}
		totalBytes += size
		counted++
		if ack > newestAck {
			newestAck = ack
		}
	}
	if counted == 0 {
		c.reg.Flink.Reachable.Set(false)
		return
	}

	c.reg.Flink.Reachable.Set(true)
	c.reg.Flink.StateBytes.Set(float64(totalBytes))
	c.reg.Flink.StateSeries.Add(float64(totalBytes))
	if newestAck > 0 {
		age := time.Since(time.UnixMilli(newestAck)).Seconds()
		if age < 0 {
			age = 0
		}
		c.reg.Flink.CheckpointAge.Set(age)
	}
}

func (c *flinkCollector) jobs(ctx context.Context) (*flinkJobsOverview, error) {
	var out flinkJobsOverview
	if err := c.get(ctx, "/jobs/overview", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *flinkCollector) checkpoint(ctx context.Context, jid string) (int64, int64, error) {
	var out flinkCheckpoints
	if err := c.get(ctx, "/jobs/"+jid+"/checkpoints", &out); err != nil {
		return 0, 0, err
	}
	if out.Latest.Completed == nil {
		return 0, 0, fmt.Errorf("flink: job %s has no completed checkpoint", jid)
	}
	return out.Latest.Completed.StateSize, out.Latest.Completed.LatestAckTS, nil
}

func (c *flinkCollector) get(ctx context.Context, path string, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimRight(c.cfg.Addr, "/")+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.cfg.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("flink: %s returned HTTP %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(into)
}
