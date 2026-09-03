package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// healthCheckTimeout bounds the probe.
//
// Shorter than the container healthcheck's own timeout so the process exits
// with a verdict of its own rather than being killed without one — a probe
// reported as "timed out by Docker" tells an operator less than one that says
// which URL did not answer.
const healthCheckTimeout = 4 * time.Second

// runHealthCheck probes a /health endpoint and reports the verdict as an exit
// code: 0 healthy, 1 anything else.
//
// This exists because the runtime image is distroless. There is no shell, no
// curl and no wget in it, which is the point — a container with a shell is a
// container an attacker has a shell in — but it leaves nothing to write a
// HEALTHCHECK with. The usual answers are to fatten the image or to drop the
// healthcheck; running the binary we already ship costs neither.
//
// It is a separate mode rather than a subcommand so the entrypoint stays a
// single binary invocation, which is what Docker's exec-form CMD wants.
func runHealthCheck(url string) error {
	ctx, cancel := context.WithTimeout(context.Background(), healthCheckTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("health-check: %s is not a usable URL: %w", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("health-check: %s did not answer: %w", url, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	if resp.StatusCode == http.StatusOK {
		return nil
	}

	// Print why, not just that. This lands in `docker inspect`'s health log,
	// which is frequently the only forensic record of an appliance that was
	// restarting all night.
	var h struct {
		Status string `json:"status"`
		Detail string `json:"detail"`
	}
	if err := json.Unmarshal(body, &h); err == nil && h.Status != "" {
		fmt.Fprintf(os.Stderr, "health-check: %s -> %d %s: %s\n",
			url, resp.StatusCode, h.Status, h.Detail)
	} else {
		fmt.Fprintf(os.Stderr, "health-check: %s -> %d\n", url, resp.StatusCode)
	}
	return fmt.Errorf("health-check: not healthy")
}
