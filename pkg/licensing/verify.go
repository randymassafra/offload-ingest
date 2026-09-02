package licensing

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// DefaultGrace is how long the binary keeps serving past a licence's expiry
// when the licence does not state its own allowance.
//
// Seven days is a deliberate compromise. A venue's licence renewal can be
// delayed by a purchase order, a holiday, or a network the venue does not
// control; cutting a live site off at the stroke of midnight turns a billing
// problem into an outage. Seven days is long enough to survive a weekend and a
// business week, and short enough that it cannot be used as a free month.
const DefaultGrace = 7 * 24 * time.Hour

// Sentinel errors, so callers can distinguish "this venue needs to renew" from
// "this file has been tampered with". They warrant very different responses:
// one is a support ticket, the other is an incident.
var (
	ErrNoLicense        = errors.New("licensing: no license file")
	ErrMalformed        = errors.New("licensing: license file is malformed")
	ErrBadSignature     = errors.New("licensing: signature does not verify")
	ErrProductDenied    = errors.New("licensing: license does not cover this product")
	ErrNotYetValid      = errors.New("licensing: license is not yet valid")
	ErrExpired          = errors.New("licensing: license has expired and the grace period has lapsed")
	ErrFingerprint      = errors.New("licensing: license is not valid for this machine")
	ErrUnknownAlgorithm = errors.New("licensing: unsupported signature algorithm")
)

// AlgEd25519 is the only algorithm accepted.
//
// The field exists so a future migration has somewhere to go, but the verifier
// allows exactly one value. An "alg" field that a caller can set is how JWT
// implementations ended up accepting "none"; this one is checked against a
// constant before the key is ever touched.
const AlgEd25519 = "ed25519"

// Load reads and parses a licence file without verifying it.
func Load(path string) (*License, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w at %s", ErrNoLicense, path)
		}
		return nil, err
	}
	var lic License
	if err := json.Unmarshal(raw, &lic); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	return &lic, nil
}

// VerifySignature checks the Ed25519 signature over the canonical claims.
func (l *License) VerifySignature(pub ed25519.PublicKey) error {
	if !strings.EqualFold(l.Algorithm, AlgEd25519) {
		return fmt.Errorf("%w: %q", ErrUnknownAlgorithm, l.Algorithm)
	}
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("licensing: public key is %d bytes, want %d",
			len(pub), ed25519.PublicKeySize)
	}
	sig, err := base64.StdEncoding.DecodeString(l.Signature)
	if err != nil {
		return fmt.Errorf("%w: signature is not base64: %v", ErrMalformed, err)
	}
	if len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("%w: signature is %d bytes, want %d",
			ErrBadSignature, len(sig), ed25519.SignatureSize)
	}
	body, err := l.Claims.Canonical()
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, body, sig) {
		return ErrBadSignature
	}
	return nil
}

// Sign produces a signed licence from claims. Used by cmd/licensetool; it is in
// the package so signing and verifying can never drift apart on what exactly
// gets covered by the signature.
func Sign(claims Claims, priv ed25519.PrivateKey) (*License, error) {
	if err := claims.Validate(); err != nil {
		return nil, err
	}
	body, err := claims.Canonical()
	if err != nil {
		return nil, err
	}
	return &License{
		Claims:    claims,
		Algorithm: AlgEd25519,
		Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(priv, body)),
	}, nil
}

// Options controls a verification.
type Options struct {
	// Product is the token that must appear in allowed_products.
	Product string
	// Now is the clock. Injected so tests do not depend on wall time.
	Now time.Time
	// Fingerprint is this machine's identity. Empty skips the hardware check,
	// which is only ever right in tests.
	Fingerprint string
}

// Verify performs the full check: signature, then coherence, then entitlement,
// then clock, then hardware.
//
// Order matters. The signature is checked FIRST and everything else is read
// only after it passes, so no unauthenticated field ever influences a decision.
// A tampered licence that claims a generous tier must not get as far as having
// that tier parsed.
func (l *License) Verify(pub ed25519.PublicKey, opt Options) error {
	if err := l.VerifySignature(pub); err != nil {
		return err
	}
	if err := l.Claims.Validate(); err != nil {
		return err
	}
	if !l.Claims.AllowsProduct(opt.Product) {
		return fmt.Errorf("%w: %q is not in %v",
			ErrProductDenied, opt.Product, l.Claims.AllowedProducts)
	}

	now := opt.Now
	if now.IsZero() {
		now = time.Now()
	}
	if nb := l.Claims.NotBefore; !nb.IsZero() && now.Before(nb) {
		return fmt.Errorf("%w until %s", ErrNotYetValid, nb.Format(time.RFC3339))
	}
	if deadline := l.Claims.Deadline(); now.After(deadline) {
		return fmt.Errorf("%w: expired %s, grace ended %s",
			ErrExpired,
			l.Claims.ExpiresAt.Format(time.RFC3339),
			deadline.Format(time.RFC3339))
	}

	// An empty fingerprint list means the licence floats across machines. That
	// is a real issuing choice for a venue that re-images hardware, so it is
	// allowed — but only when the licence says so, never as a fallback for a
	// machine whose fingerprint could not be read.
	if len(l.Claims.Fingerprints) > 0 {
		if opt.Fingerprint == "" {
			return fmt.Errorf("%w: license is pinned but no machine fingerprint was available",
				ErrFingerprint)
		}
		if !containsFold(l.Claims.Fingerprints, opt.Fingerprint) {
			return fmt.Errorf("%w: this machine is %s", ErrFingerprint, opt.Fingerprint)
		}
	}
	return nil
}

// InGrace reports whether the licence is past expiry but still inside its
// grace period, which the dashboard surfaces as a warning rather than a fault.
func (l *License) InGrace(now time.Time) bool {
	return now.After(l.Claims.ExpiresAt) && !now.After(l.Claims.Deadline())
}
