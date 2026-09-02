package licensing

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// quietLogger keeps the expected failure paths from spraying the test output.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testKeys(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return pub, priv
}

func validClaims(now time.Time) Claims {
	return Claims{
		LicenseID:       "lic-test-1",
		TenantID:        "acme-arena",
		VenueName:       "Acme Arena",
		AllowedProducts: []string{ProductIngest},
		Sports:          []string{"nfl", "nba", "soccer"},
		Regions:         []string{"us", "eu"},
		Tier:            Tier{Name: TierFree},
		IssuedAt:        now,
		ExpiresAt:       now.Add(30 * 24 * time.Hour),
	}
}

func writeLicense(t *testing.T, dir string, lic *License) string {
	t.Helper()
	body, err := json.MarshalIndent(lic, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	path := filepath.Join(dir, "license.key")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

func TestSignedLicenseVerifies(t *testing.T) {
	pub, priv := testKeys(t)
	now := time.Now()
	lic, err := Sign(validClaims(now), priv)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if err := lic.Verify(pub, Options{Product: ProductIngest, Now: now}); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

// TestTamperedClaimsFailVerification is the whole point of signing the licence.
// Every field below is one a venue would have a motive to edit.
func TestTamperedClaimsFailVerification(t *testing.T) {
	pub, priv := testKeys(t)
	now := time.Now()

	for _, tc := range []struct {
		name   string
		tamper func(*License)
	}{
		{"extend expiry", func(l *License) { l.Claims.ExpiresAt = now.AddDate(10, 0, 0) }},
		{"upgrade tier", func(l *License) { l.Claims.Tier.Name = TierMega }},
		{"raise custom ceiling", func(l *License) { l.Claims.Tier.RequestsPerDay = 1_000_000 }},
		{"add a sport", func(l *License) { l.Claims.Sports = append(l.Claims.Sports, "golf") }},
		{"change tenant", func(l *License) { l.Claims.TenantID = "someone-else" }},
		{"widen products", func(l *License) { l.Claims.AllowedProducts = []string{"*"} }},
		{"stretch grace", func(l *License) { l.Claims.GraceDays = 3650 }},
		{"unpin hardware", func(l *License) { l.Claims.Fingerprints = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claims := validClaims(now)
			claims.Fingerprints = []string{"machine-a"}
			lic, err := Sign(claims, priv)
			if err != nil {
				t.Fatalf("Sign: %v", err)
			}
			tc.tamper(lic)
			err = lic.Verify(pub, Options{
				Product: ProductIngest, Now: now, Fingerprint: "machine-a",
			})
			if !errors.Is(err, ErrBadSignature) {
				t.Errorf("tampering with %s gave %v, want ErrBadSignature", tc.name, err)
			}
		})
	}
}

// TestSignatureIsStableAcrossReserialisation pins the reason claims are signed
// over a canonical encoding: a licence that a support tool has round-tripped
// through a different JSON writer must still verify.
func TestSignatureIsStableAcrossReserialisation(t *testing.T) {
	pub, priv := testKeys(t)
	now := time.Now()
	lic, err := Sign(validClaims(now), priv)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	// Round-trip through a generic map, which loses Go's field ordering.
	raw, _ := json.Marshal(lic)
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	reordered, _ := json.MarshalIndent(generic, "", "    ")
	var back License
	if err := json.Unmarshal(reordered, &back); err != nil {
		t.Fatalf("unmarshal back: %v", err)
	}
	if err := back.Verify(pub, Options{Product: ProductIngest, Now: now}); err != nil {
		t.Errorf("re-serialised licence failed to verify: %v", err)
	}
}

func TestWrongKeyIsRejected(t *testing.T) {
	_, priv := testKeys(t)
	otherPub, _ := testKeys(t)
	now := time.Now()
	lic, _ := Sign(validClaims(now), priv)
	if err := lic.Verify(otherPub, Options{Product: ProductIngest, Now: now}); !errors.Is(err, ErrBadSignature) {
		t.Errorf("got %v, want ErrBadSignature", err)
	}
}

// TestAlgorithmCannotBeDowngraded is the "alg: none" lesson from JWT. The field
// is on the wire, so it has to be checked against a constant.
func TestAlgorithmCannotBeDowngraded(t *testing.T) {
	pub, priv := testKeys(t)
	now := time.Now()
	lic, _ := Sign(validClaims(now), priv)
	for _, alg := range []string{"none", "", "hs256", "ED25519-FAKE"} {
		tampered := *lic
		tampered.Algorithm = alg
		if err := tampered.Verify(pub, Options{Product: ProductIngest, Now: now}); err == nil {
			t.Errorf("algorithm %q was accepted", alg)
		}
	}
}

func TestProductEntitlementIsEnforced(t *testing.T) {
	pub, priv := testKeys(t)
	now := time.Now()
	claims := validClaims(now)
	claims.AllowedProducts = []string{"offload-analytics"}
	lic, _ := Sign(claims, priv)
	if err := lic.Verify(pub, Options{Product: ProductIngest, Now: now}); !errors.Is(err, ErrProductDenied) {
		t.Errorf("got %v, want ErrProductDenied", err)
	}
}

// TestGracePeriodBoundaries pins the 7-day offline allowance exactly: valid the
// day before it lapses, dead the moment after.
func TestGracePeriodBoundaries(t *testing.T) {
	pub, priv := testKeys(t)
	issued := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	claims := validClaims(issued)
	claims.ExpiresAt = issued.Add(24 * time.Hour)
	lic, _ := Sign(claims, priv)

	expiry := claims.ExpiresAt
	for _, tc := range []struct {
		name    string
		at      time.Time
		wantErr bool
		inGrace bool
	}{
		{"before expiry", expiry.Add(-time.Hour), false, false},
		{"one second after expiry", expiry.Add(time.Second), false, true},
		{"six days into grace", expiry.Add(6 * 24 * time.Hour), false, true},
		{"one second before grace ends", expiry.Add(DefaultGrace - time.Second), false, true},
		{"one second after grace ends", expiry.Add(DefaultGrace + time.Second), true, false},
		{"a month later", expiry.Add(30 * 24 * time.Hour), true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := lic.Verify(pub, Options{Product: ProductIngest, Now: tc.at})
			if tc.wantErr && !errors.Is(err, ErrExpired) {
				t.Errorf("at %s got %v, want ErrExpired", tc.name, err)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("at %s got %v, want valid", tc.name, err)
			}
			if got := lic.InGrace(tc.at); got != tc.inGrace {
				t.Errorf("at %s InGrace = %v, want %v", tc.name, got, tc.inGrace)
			}
		})
	}
}

func TestCustomGraceIsHonoured(t *testing.T) {
	pub, priv := testKeys(t)
	issued := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	claims := validClaims(issued)
	claims.ExpiresAt = issued.Add(time.Hour)
	claims.GraceDays = 1
	lic, _ := Sign(claims, priv)

	if err := lic.Verify(pub, Options{Product: ProductIngest, Now: claims.ExpiresAt.Add(23 * time.Hour)}); err != nil {
		t.Errorf("inside a 1-day grace: %v", err)
	}
	if err := lic.Verify(pub, Options{Product: ProductIngest, Now: claims.ExpiresAt.Add(25 * time.Hour)}); !errors.Is(err, ErrExpired) {
		t.Errorf("past a 1-day grace got %v, want ErrExpired", err)
	}
}

func TestNotBeforeIsEnforced(t *testing.T) {
	pub, priv := testKeys(t)
	now := time.Now()
	claims := validClaims(now)
	claims.NotBefore = now.Add(48 * time.Hour)
	lic, _ := Sign(claims, priv)
	if err := lic.Verify(pub, Options{Product: ProductIngest, Now: now}); !errors.Is(err, ErrNotYetValid) {
		t.Errorf("got %v, want ErrNotYetValid", err)
	}
}

func TestFingerprintPinning(t *testing.T) {
	pub, priv := testKeys(t)
	now := time.Now()
	claims := validClaims(now)
	claims.Fingerprints = []string{"machine-a", "machine-b"}
	lic, _ := Sign(claims, priv)

	for _, fp := range []string{"machine-a", "machine-b"} {
		if err := lic.Verify(pub, Options{Product: ProductIngest, Now: now, Fingerprint: fp}); err != nil {
			t.Errorf("pinned machine %s rejected: %v", fp, err)
		}
	}
	if err := lic.Verify(pub, Options{Product: ProductIngest, Now: now, Fingerprint: "machine-c"}); !errors.Is(err, ErrFingerprint) {
		t.Errorf("unpinned machine got %v, want ErrFingerprint", err)
	}
	// A pinned licence on a host with no readable identity must fail, not pass.
	if err := lic.Verify(pub, Options{Product: ProductIngest, Now: now, Fingerprint: ""}); !errors.Is(err, ErrFingerprint) {
		t.Errorf("pinned licence with no fingerprint got %v, want ErrFingerprint", err)
	}
}

func TestFloatingLicenseRunsAnywhere(t *testing.T) {
	pub, priv := testKeys(t)
	now := time.Now()
	lic, _ := Sign(validClaims(now), priv) // no fingerprints
	if err := lic.Verify(pub, Options{Product: ProductIngest, Now: now, Fingerprint: "anything"}); err != nil {
		t.Errorf("floating licence rejected: %v", err)
	}
}

// TestTierResolution covers the rule that a licence may tighten a published
// plan but never loosen it.
func TestTierResolution(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      Tier
		wantRPM int
		wantRPD int
		wantErr bool
	}{
		{"free from catalog", Tier{Name: TierFree}, 10, 100, false},
		{"pro from catalog", Tier{Name: TierPro}, 300, 7500, false},
		{"tightened below plan", Tier{Name: TierPro, RequestsPerMinute: 60, RequestsPerDay: 1000}, 60, 1000, false},
		{"cannot exceed plan", Tier{Name: TierFree, RequestsPerMinute: 5000, RequestsPerDay: 900000}, 10, 100, false},
		{"custom with ceilings", Tier{Name: TierCustom, RequestsPerMinute: 1200, RequestsPerDay: 400000}, 1200, 400000, false},
		{"custom without ceilings", Tier{Name: TierCustom}, 0, 0, true},
		{"unknown plan", Tier{Name: "platinum"}, 0, 0, true},
		{"unnamed", Tier{}, 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.in.Resolve()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want an error, got %s", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if got.RequestsPerMinute != tc.wantRPM || got.RequestsPerDay != tc.wantRPD {
				t.Errorf("got %d/min %d/day, want %d/min %d/day",
					got.RequestsPerMinute, got.RequestsPerDay, tc.wantRPM, tc.wantRPD)
			}
		})
	}
}

func TestClaimsValidateRejectsIncoherentLicences(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name string
		mut  func(*Claims)
	}{
		{"no tenant", func(c *Claims) { c.TenantID = "" }},
		{"no licence id", func(c *Claims) { c.LicenseID = "" }},
		{"no products", func(c *Claims) { c.AllowedProducts = nil }},
		{"no sports", func(c *Claims) { c.Sports = nil }},
		{"no expiry", func(c *Claims) { c.ExpiresAt = time.Time{} }},
		{"expiry before not_before", func(c *Claims) {
			c.NotBefore = now.Add(72 * time.Hour)
			c.ExpiresAt = now.Add(time.Hour)
		}},
		{"bad tier", func(c *Claims) { c.Tier = Tier{Name: "gold"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := validClaims(now)
			tc.mut(&c)
			if err := c.Validate(); err == nil {
				t.Error("want an error, got nil")
			}
		})
	}
}

// TestEmptySportListIsNotAWildcard pins that an omission denies rather than
// grants. This is the direction an entitlement bug has to fail in.
func TestEmptySportListIsNotAWildcard(t *testing.T) {
	c := Claims{}
	if c.AllowsSport("nfl") {
		t.Error("an empty sport list must entitle nothing")
	}
	if c.AllowsProduct(ProductIngest) {
		t.Error("an empty product list must entitle nothing")
	}
}

// --- validator ------------------------------------------------------------

// TestValidatorShutsDownWhenGraceLapses is the behaviour the whole package
// exists for, and the reason shutdown is injectable.
func TestValidatorShutsDownWhenGraceLapses(t *testing.T) {
	pub, priv := testKeys(t)
	dir := t.TempDir()
	issued := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	claims := validClaims(issued)
	claims.ExpiresAt = issued.Add(24 * time.Hour)
	lic, _ := Sign(claims, priv)
	path := writeLicense(t, dir, lic)

	clock := issued
	var exitCode = -1
	v, err := New(Config{
		Path: path, PublicKey: pub, Fingerprint: "any", Logger: quietLogger(),
		Now:      func() time.Time { return clock },
		Shutdown: func(code int) { exitCode = code },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Inside grace: a warning, but the process lives.
	clock = claims.ExpiresAt.Add(3 * 24 * time.Hour)
	v.Enforce()
	if exitCode != -1 {
		t.Fatalf("shut down at %v while still inside grace", clock)
	}
	if !v.Status().InGrace {
		t.Error("status should report InGrace")
	}

	// Past grace: the process must exit.
	clock = claims.ExpiresAt.Add(DefaultGrace + time.Minute)
	v.Enforce()
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1 once grace lapsed", exitCode)
	}
	if st := v.Status(); st.Valid {
		t.Error("status should be invalid once grace lapsed")
	}
}

func TestValidatorShutsDownOnTamperedFile(t *testing.T) {
	pub, priv := testKeys(t)
	dir := t.TempDir()
	now := time.Now()
	lic, _ := Sign(validClaims(now), priv)
	path := writeLicense(t, dir, lic)

	exitCode := -1
	v, err := New(Config{
		Path: path, PublicKey: pub, Fingerprint: "any", Logger: quietLogger(),
		Now: func() time.Time { return now }, Shutdown: func(c int) { exitCode = c },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Someone edits the licence under the running process.
	lic.Claims.Tier.Name = TierMega
	writeLicense(t, dir, lic)
	v.Enforce()
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1 after tampering", exitCode)
	}
}

func TestValidatorRefusesToStartWithoutALicense(t *testing.T) {
	pub, _ := testKeys(t)
	_, err := New(Config{
		Path: filepath.Join(t.TempDir(), "absent.key"), PublicKey: pub,
		Fingerprint: "any", Logger: quietLogger(),
		Shutdown: func(int) {},
	})
	if !errors.Is(err, ErrNoLicense) {
		t.Errorf("got %v, want ErrNoLicense", err)
	}
}

// TestWatchStopsWithContext keeps the ticker from outliving the process it
// guards, which would leak a goroutine per restart in a supervised container.
func TestWatchStopsWithContext(t *testing.T) {
	pub, priv := testKeys(t)
	dir := t.TempDir()
	now := time.Now()
	lic, _ := Sign(validClaims(now), priv)
	path := writeLicense(t, dir, lic)

	v, err := New(Config{
		Path: path, PublicKey: pub, Fingerprint: "any", Logger: quietLogger(),
		Now: func() time.Time { return now }, Shutdown: func(int) {},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	before := runtimeGoroutines()
	v.Watch(ctx)
	cancel()
	// Give the goroutine a moment to observe the cancellation.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtimeGoroutines() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("Watch goroutine outlived its context")
}

func TestEmbeddedPublicKeyFromEnv(t *testing.T) {
	pub, _ := testKeys(t)
	t.Setenv("OFFLOAD_LICENSE_PUBKEY", base64.StdEncoding.EncodeToString(pub))
	got, err := EmbeddedPublicKey()
	if err != nil {
		t.Fatalf("EmbeddedPublicKey: %v", err)
	}
	if !got.Equal(pub) {
		t.Error("key round-trip mismatch")
	}

	t.Setenv("OFFLOAD_LICENSE_PUBKEY", "not-base64!!")
	if _, err := EmbeddedPublicKey(); err == nil {
		t.Error("want an error for a malformed key")
	}
	// A build with no key must refuse rather than accept everything.
	t.Setenv("OFFLOAD_LICENSE_PUBKEY", "")
	if _, err := EmbeddedPublicKey(); err == nil {
		t.Error("a build with no public key must not verify anything")
	}
}

func TestFingerprintIsStableAndOpaque(t *testing.T) {
	a, err := Fingerprint()
	if err != nil {
		t.Skipf("no stable hardware identity here: %v", err)
	}
	b, _ := Fingerprint()
	if a != b {
		t.Errorf("fingerprint is not stable: %s vs %s", a, b)
	}
	if len(a) != 64 {
		t.Errorf("fingerprint is %d chars, want a 64-char sha256 hex", len(a))
	}
	// It must not leak a MAC address in the clear.
	for _, mac := range mustHardwareAddrs(t) {
		if len(mac) > 0 && contains(a, mac) {
			t.Errorf("fingerprint leaks the MAC %s", mac)
		}
	}
}

func mustHardwareAddrs(t *testing.T) []string {
	t.Helper()
	addrs, err := hardwareAddrs()
	if err != nil {
		return nil
	}
	return addrs
}

func contains(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) &&
		(func() bool {
			for i := 0; i+len(needle) <= len(haystack); i++ {
				if haystack[i:i+len(needle)] == needle {
					return true
				}
			}
			return false
		})()
}
