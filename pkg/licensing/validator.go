package licensing

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// VerifyInterval is how often the background ticker re-checks the licence.
//
// Daily, not hourly. Nothing the check reads changes faster than a day: the
// expiry is a fixed timestamp in a signed file. The ticker exists to catch a
// long-running process crossing its own deadline — a venue box that has been up
// for months — not to poll for changes, so a tighter interval would burn wakeups
// to learn nothing. Re-reading from disk each tick does mean a licence swapped
// in underneath a running process is picked up within a day, without a restart.
const VerifyInterval = 24 * time.Hour

// Status is a point-in-time view of the licence, for the dashboard and metrics.
type Status struct {
	Valid       bool      `json:"valid"`
	InGrace     bool      `json:"in_grace"`
	TenantID    string    `json:"tenant_id"`
	VenueName   string    `json:"venue_name,omitempty"`
	LicenseID   string    `json:"license_id"`
	Tier        Tier      `json:"tier"`
	Sports      []string  `json:"sports"`
	Regions     []string  `json:"regions,omitempty"`
	ExpiresAt   time.Time `json:"expires_at"`
	Deadline    time.Time `json:"deadline"`
	LastChecked time.Time `json:"last_checked"`
	Fingerprint string    `json:"fingerprint"`
	Error       string    `json:"error,omitempty"`
}

// DaysRemaining is whole days from now until the hard deadline, floored at zero.
func (s Status) DaysRemaining(now time.Time) int {
	d := int(s.Deadline.Sub(now).Hours() / 24)
	if d < 0 {
		return 0
	}
	return d
}

// Validator holds a verified licence and keeps re-verifying it.
type Validator struct {
	path        string
	pub         ed25519.PublicKey
	product     string
	fingerprint string

	// now and shutdown are injected so the failure path can be tested. A
	// package whose only exit is a real os.Exit(1) cannot have its most
	// important behaviour covered, and that behaviour is precisely the one
	// nobody wants to discover is wrong in production.
	now      func() time.Time
	shutdown func(int)
	log      *slog.Logger

	mu     sync.RWMutex
	lic    *License
	tier   Tier
	status Status
}

// Config configures a Validator.
type Config struct {
	Path        string            // license file; defaults to OFFLOAD_LICENSE_PATH or license.key
	PublicKey   ed25519.PublicKey // defaults to the build-time embedded key
	Product     string            // defaults to ProductIngest
	Fingerprint string            // defaults to this machine's
	Logger      *slog.Logger
	Now         func() time.Time
	Shutdown    func(int)
}

// New verifies a licence once and returns a Validator holding it.
//
// This is the startup gate: it returns an error rather than exiting, so the
// caller decides how a cold start failure is reported. The ticker started by
// Watch is what enforces the deadline from then on.
func New(cfg Config) (*Validator, error) {
	if cfg.Path == "" {
		cfg.Path = os.Getenv("OFFLOAD_LICENSE_PATH")
	}
	if cfg.Path == "" {
		cfg.Path = "license.key"
	}
	if cfg.Product == "" {
		cfg.Product = ProductIngest
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Shutdown == nil {
		cfg.Shutdown = os.Exit
	}
	if len(cfg.PublicKey) == 0 {
		pub, err := EmbeddedPublicKey()
		if err != nil {
			return nil, err
		}
		cfg.PublicKey = pub
	}
	if cfg.Fingerprint == "" {
		// A machine with no readable identity is not a reason to fail closed at
		// this point: a floating licence is still perfectly valid on it. Verify
		// rejects the pinned case separately.
		if fp, err := Fingerprint(); err == nil {
			cfg.Fingerprint = fp
		} else {
			cfg.Logger.Warn("licensing: no hardware fingerprint available", "err", err)
		}
	}

	v := &Validator{
		path: cfg.Path, pub: cfg.PublicKey, product: cfg.Product,
		fingerprint: cfg.Fingerprint, now: cfg.Now,
		shutdown: cfg.Shutdown, log: cfg.Logger,
	}
	if err := v.check(); err != nil {
		return nil, err
	}
	return v, nil
}

// check re-reads and re-verifies the licence, updating the cached status.
func (v *Validator) check() error {
	lic, err := Load(v.path)
	if err != nil {
		v.fail(err)
		return err
	}
	now := v.now()
	if err := lic.Verify(v.pub, Options{
		Product: v.product, Now: now, Fingerprint: v.fingerprint,
	}); err != nil {
		v.fail(err)
		return err
	}
	tier, err := lic.Claims.Tier.Resolve()
	if err != nil {
		v.fail(err)
		return err
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	v.lic, v.tier = lic, tier
	v.status = Status{
		Valid: true, InGrace: lic.InGrace(now),
		TenantID: lic.Claims.TenantID, VenueName: lic.Claims.VenueName,
		LicenseID: lic.Claims.LicenseID, Tier: tier,
		Sports: lic.Claims.Sports, Regions: lic.Claims.Regions,
		ExpiresAt: lic.Claims.ExpiresAt, Deadline: lic.Claims.Deadline(),
		LastChecked: now, Fingerprint: v.fingerprint,
	}
	return nil
}

func (v *Validator) fail(err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.status.Valid = false
	v.status.LastChecked = v.now()
	v.status.Error = err.Error()
}

// Watch starts the background verification ticker.
//
// On any failure it logs the reason and calls shutdown(1). That is blunt on
// purpose: this process publishes licensed data into a venue's Kafka cluster,
// and continuing to publish on a licence that has stopped verifying is the one
// outcome the licence exists to prevent. Degrading to a read-only mode would
// leave a half-running pipeline that nobody notices for weeks.
//
// Watch returns immediately; the caller keeps the context.
func (v *Validator) Watch(ctx context.Context) {
	go func() {
		t := time.NewTicker(VerifyInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				v.Enforce()
			}
		}
	}()
}

// Enforce re-verifies and shuts the process down if the licence no longer
// holds. Exported so the ticker's behaviour can be exercised directly.
func (v *Validator) Enforce() {
	if err := v.check(); err != nil {
		v.log.Error("licensing: verification failed, shutting down",
			"err", err, "path", v.path, "fingerprint", v.fingerprint)
		v.shutdown(1)
		return
	}
	st := v.Status()
	if st.InGrace {
		v.log.Warn("licensing: license has expired and is running on borrowed time",
			"expired", st.ExpiresAt.Format(time.RFC3339),
			"shutdown_at", st.Deadline.Format(time.RFC3339),
			"days_remaining", st.DaysRemaining(v.now()))
	}
}

// Status returns the current cached status.
func (v *Validator) Status() Status {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.status
}

// Tier returns the entitled throughput.
func (v *Validator) Tier() Tier {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.tier
}

// Claims returns the verified claims.
func (v *Validator) Claims() Claims {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.lic == nil {
		return Claims{}
	}
	return v.lic.Claims
}

// AllowsSport reports whether the licence entitles a sport.
func (v *Validator) AllowsSport(sport string) bool { return v.Claims().AllowsSport(sport) }

// EmbeddedPublicKey returns the verification key this binary was built with.
//
// It is a var rather than a const so a release build can override it with
// -ldflags, and it falls back to OFFLOAD_LICENSE_PUBKEY for development. A
// build with neither cannot verify anything and says so rather than accepting
// every licence, which is the failure mode that matters.
func EmbeddedPublicKey() (ed25519.PublicKey, error) {
	raw := publicKeyB64
	if raw == "" {
		raw = os.Getenv("OFFLOAD_LICENSE_PUBKEY")
	}
	if raw == "" {
		return nil, fmt.Errorf(
			"licensing: this build has no license public key " +
				"(set -ldflags -X licensing.publicKeyB64=... or OFFLOAD_LICENSE_PUBKEY)")
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("licensing: public key is not base64: %w", err)
	}
	if len(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("licensing: public key is %d bytes, want %d",
			len(key), ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(key), nil
}

// publicKeyB64 is set at build time:
//
//	go build -ldflags "-X github.com/offloadintelligence/offload-ingest/pkg/licensing.publicKeyB64=<base64>"
var publicKeyB64 string
