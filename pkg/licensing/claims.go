package licensing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ProductIngest is this binary's product token. A licence that does not name it
// in AllowedProducts does not authorise this process, however valid it is for
// something else in the suite.
const ProductIngest = "offload-ingest"

// Claims is the signed body of a licence. Every field here is covered by the
// signature: changing one byte invalidates the file.
//
// The JSON tags are part of the wire contract. Signing is done over a canonical
// encoding of this struct, so renaming a tag breaks every licence in the field
// — treat them as append-only.
type Claims struct {
	// LicenseID identifies this document, for support and revocation lists.
	LicenseID string `json:"license_id"`
	// TenantID is the venue operator. The pipeline stamps it on every message
	// so a multi-tenant Kafka cluster stays attributable.
	TenantID string `json:"tenant_id"`
	// VenueName is display-only, shown on the dashboard.
	VenueName string `json:"venue_name,omitempty"`

	// AllowedProducts lists the binaries this licence authorises.
	AllowedProducts []string `json:"allowed_products"`
	// Fingerprints pins the licence to specific machines. An empty list means
	// the licence floats, which is a deliberate issuing decision rather than an
	// oversight — see Verify.
	Fingerprints []string `json:"fingerprints,omitempty"`

	// Regions and Sports are the content entitlements. A region is a bundle
	// token that maps onto API-Sports hosts and league filters.
	Regions []string `json:"regions,omitempty"`
	Sports  []string `json:"sports"`

	// Tier is the API throughput this venue paid for.
	Tier Tier `json:"tier"`

	IssuedAt  time.Time `json:"issued_at"`
	NotBefore time.Time `json:"not_before,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`

	// GraceDays is how long the binary keeps running past ExpiresAt. It is
	// inside the signed body so a venue cannot extend its own grace by editing
	// a config file. Zero means the package default applies.
	GraceDays int `json:"grace_days,omitempty"`
}

// License is the on-disk document: claims plus the signature over them.
type License struct {
	Claims    Claims `json:"claims"`
	Algorithm string `json:"alg"`
	// Signature is base64 (std, padded) Ed25519 over Claims.Canonical().
	Signature string `json:"sig"`
}

// Canonical renders the claims as the exact bytes that get signed.
//
// Signing raw json.Marshal output would be fragile: Go orders struct fields by
// declaration, but a licence re-serialised by any other tool — a support script,
// a Python issuer, a hand edit — would reorder or re-space them and the
// signature would stop verifying for no real reason. Canonicalising through a
// sorted, compact generic encoding makes the signature depend on the *content*
// rather than on which library happened to write the file.
func (c Claims) Canonical() ([]byte, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	var generic any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := writeCanonical(&buf, generic); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// writeCanonical emits JSON with object keys sorted and no insignificant space.
func writeCanonical(buf *bytes.Buffer, v any) error {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			ek, err := json.Marshal(k)
			if err != nil {
				return err
			}
			buf.Write(ek)
			buf.WriteByte(':')
			if err := writeCanonical(buf, t[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	case []any:
		buf.WriteByte('[')
		for i, el := range t {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, el); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	default:
		enc, err := json.Marshal(t)
		if err != nil {
			return err
		}
		buf.Write(enc)
	}
	return nil
}

// Grace is the offline allowance past expiry.
func (c Claims) Grace() time.Duration {
	if c.GraceDays > 0 {
		return time.Duration(c.GraceDays) * 24 * time.Hour
	}
	return DefaultGrace
}

// Deadline is the instant the binary must stop: expiry plus grace.
func (c Claims) Deadline() time.Time { return c.ExpiresAt.Add(c.Grace()) }

// AllowsProduct reports whether the licence authorises a product token.
func (c Claims) AllowsProduct(product string) bool {
	return containsFold(c.AllowedProducts, product)
}

// AllowsSport reports whether a sport is entitled. A licence with no sports
// listed entitles none — an empty list is not a wildcard, because defaulting an
// omission to "everything" is how entitlement checks quietly stop working.
func (c Claims) AllowsSport(sport string) bool { return containsFold(c.Sports, sport) }

// AllowsRegion reports whether a regional bundle is entitled.
func (c Claims) AllowsRegion(region string) bool { return containsFold(c.Regions, region) }

func containsFold(haystack []string, needle string) bool {
	for _, h := range haystack {
		if strings.EqualFold(strings.TrimSpace(h), strings.TrimSpace(needle)) {
			return true
		}
	}
	return false
}

// Validate checks the claims are internally coherent, before any signature or
// clock comparison. A licence that fails here was mis-issued.
func (c Claims) Validate() error {
	switch {
	case strings.TrimSpace(c.LicenseID) == "":
		return fmt.Errorf("licensing: license_id is empty")
	case strings.TrimSpace(c.TenantID) == "":
		return fmt.Errorf("licensing: tenant_id is empty")
	case len(c.AllowedProducts) == 0:
		return fmt.Errorf("licensing: allowed_products is empty")
	case len(c.Sports) == 0:
		return fmt.Errorf("licensing: sports is empty")
	case c.ExpiresAt.IsZero():
		return fmt.Errorf("licensing: expires_at is unset")
	case !c.NotBefore.IsZero() && c.ExpiresAt.Before(c.NotBefore):
		return fmt.Errorf("licensing: expires_at precedes not_before")
	}
	if _, err := c.Tier.Resolve(); err != nil {
		return err
	}
	return nil
}
