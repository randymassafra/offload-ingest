// Command licensetool issues and inspects offload-ingest licences.
//
// It is the private half of the licensing story: keygen and sign run on the
// build system and never ship to a venue. `verify` and `fingerprint` are safe
// to run anywhere and exist so support can diagnose a venue's licence without
// guessing.
//
//	licensetool keygen -out keys/            # once, per signing identity
//	licensetool fingerprint                  # run ON the venue machine
//	licensetool sign -key keys/license.priv -tenant acme -tier free \
//	    -sports nfl,nba,soccer -days 365 -fingerprint <fp> -out license.key
//	licensetool verify -license license.key -pub keys/license.pub
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/offloadintelligence/offload-ingest/pkg/licensing"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "keygen":
		err = runKeygen(os.Args[2:])
	case "sign":
		err = runSign(os.Args[2:])
	case "verify":
		err = runVerify(os.Args[2:])
	case "fingerprint":
		err = runFingerprint()
	case "-h", "--help", "help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "licensetool:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `licensetool issues and inspects offload-ingest licences.

Usage:
  licensetool keygen      [-out DIR]        generate an Ed25519 signing pair
  licensetool sign        [flags]           issue a signed licence
  licensetool verify      [flags]           check a licence the way the binary does
  licensetool fingerprint                   print this machine's fingerprint

Run a subcommand with -h for its flags.
`)
}

func runKeygen(args []string) error {
	fs := flag.NewFlagSet("keygen", flag.ExitOnError)
	out := fs.String("out", "keys", "directory to write license.pub and license.priv into")
	_ = fs.Parse(args)

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0o700); err != nil {
		return err
	}
	pubPath := filepath.Join(*out, "license.pub")
	privPath := filepath.Join(*out, "license.priv")
	pubB64 := base64.StdEncoding.EncodeToString(pub)

	if err := os.WriteFile(pubPath, []byte(pubB64+"\n"), 0o644); err != nil {
		return err
	}
	// 0600: the private key is the only thing standing between a customer and
	// an unlimited self-issued licence.
	if err := os.WriteFile(privPath,
		[]byte(base64.StdEncoding.EncodeToString(priv)+"\n"), 0o600); err != nil {
		return err
	}

	fmt.Printf("public key   %s\n", pubPath)
	fmt.Printf("private key  %s  (0600 — never ship this)\n", privPath)
	fmt.Printf("\nBuild the binary against this key:\n")
	fmt.Printf("  go build -ldflags \"-X %s.publicKeyB64=%s\" ./cmd/loadtest\n",
		"github.com/offloadintelligence/offload-ingest/pkg/licensing", pubB64)
	fmt.Printf("\nOr for development, export it:\n  export OFFLOAD_LICENSE_PUBKEY=%s\n", pubB64)
	return nil
}

func runSign(args []string) error {
	fs := flag.NewFlagSet("sign", flag.ExitOnError)
	keyPath := fs.String("key", "keys/license.priv", "Ed25519 private key file")
	out := fs.String("out", "license.key", "licence file to write")
	tenant := fs.String("tenant", "", "tenant id (required)")
	venue := fs.String("venue", "", "venue display name")
	id := fs.String("id", "", "licence id (default: generated)")
	tier := fs.String("tier", "free", "free|pro|ultra|mega|custom")
	rpm := fs.Int("rpm", 0, "requests/minute (required for custom, else caps the plan)")
	rpd := fs.Int("rpd", 0, "requests/day per host (required for custom, else caps the plan)")
	rpmonth := fs.Int("rpmonth", 0, "contractual requests/month, 0 for none")
	sports := fs.String("sports", "", "comma-separated sports (required)")
	regions := fs.String("regions", "", "comma-separated regional bundles")
	products := fs.String("products", licensing.ProductIngest, "comma-separated product tokens")
	fingerprints := fs.String("fingerprint", "", "comma-separated machine fingerprints; empty floats the licence")
	days := fs.Int("days", 365, "days until expiry")
	grace := fs.Int("grace", 0, "offline grace days, 0 for the 7-day default")
	_ = fs.Parse(args)

	if *tenant == "" || *sports == "" {
		return fmt.Errorf("-tenant and -sports are required")
	}
	priv, err := loadPrivate(*keyPath)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Truncate(time.Second)
	licenseID := *id
	if licenseID == "" {
		licenseID = fmt.Sprintf("lic-%s-%d", strings.ToLower(*tenant), now.Unix())
	}

	claims := licensing.Claims{
		LicenseID:       licenseID,
		TenantID:        *tenant,
		VenueName:       *venue,
		AllowedProducts: splitList(*products),
		Fingerprints:    splitList(*fingerprints),
		Regions:         splitList(*regions),
		Sports:          splitList(*sports),
		Tier: licensing.Tier{
			Name:              licensing.TierName(*tier),
			RequestsPerMinute: *rpm,
			RequestsPerDay:    *rpd,
			RequestsPerMonth:  *rpmonth,
		},
		IssuedAt:  now,
		ExpiresAt: now.AddDate(0, 0, *days),
		GraceDays: *grace,
	}
	lic, err := licensing.Sign(claims, priv)
	if err != nil {
		return err
	}
	body, err := json.MarshalIndent(lic, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*out, append(body, '\n'), 0o644); err != nil {
		return err
	}
	resolved, _ := claims.Tier.Resolve()
	fmt.Printf("wrote %s\n  tenant   %s\n  tier     %s\n  sports   %s\n  expires  %s (grace %s)\n",
		*out, claims.TenantID, resolved, strings.Join(claims.Sports, ","),
		claims.ExpiresAt.Format(time.RFC3339), claims.Grace())
	if len(claims.Fingerprints) == 0 {
		fmt.Println("  NOTE: no fingerprint pinned — this licence runs on any machine")
	}
	return nil
}

func runVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	licPath := fs.String("license", "license.key", "licence file")
	pubPath := fs.String("pub", "", "public key file; defaults to the embedded/env key")
	product := fs.String("product", licensing.ProductIngest, "product token to check")
	fp := fs.String("fingerprint", "", "machine fingerprint; defaults to this machine's")
	_ = fs.Parse(args)

	pub, err := resolvePublic(*pubPath)
	if err != nil {
		return err
	}
	lic, err := licensing.Load(*licPath)
	if err != nil {
		return err
	}
	fingerprint := *fp
	if fingerprint == "" {
		fingerprint, _ = licensing.Fingerprint()
	}
	now := time.Now()
	err = lic.Verify(pub, licensing.Options{Product: *product, Now: now, Fingerprint: fingerprint})

	tier, _ := lic.Claims.Tier.Resolve()
	fmt.Printf("licence  %s\ntenant   %s\ntier     %s\nsports   %s\nexpires  %s\ndeadline %s\nmachine  %s\n",
		lic.Claims.LicenseID, lic.Claims.TenantID, tier,
		strings.Join(lic.Claims.Sports, ","),
		lic.Claims.ExpiresAt.Format(time.RFC3339),
		lic.Claims.Deadline().Format(time.RFC3339), fingerprint)
	if err != nil {
		fmt.Printf("status   INVALID — %v\n", err)
		return err
	}
	if lic.InGrace(now) {
		fmt.Printf("status   VALID (IN GRACE — expired, shuts down %s)\n",
			lic.Claims.Deadline().Format(time.RFC3339))
		return nil
	}
	fmt.Println("status   VALID")
	return nil
}

func runFingerprint() error {
	fp, err := licensing.Fingerprint()
	if err != nil {
		return err
	}
	fmt.Println(fp)
	return nil
}

func loadPrivate(path string) (ed25519.PrivateKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("private key is not base64: %w", err)
	}
	if len(key) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("private key is %d bytes, want %d", len(key), ed25519.PrivateKeySize)
	}
	return ed25519.PrivateKey(key), nil
}

func resolvePublic(path string) (ed25519.PublicKey, error) {
	if path == "" {
		return licensing.EmbeddedPublicKey()
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("public key is not base64: %w", err)
	}
	if len(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("public key is %d bytes, want %d", len(key), ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(key), nil
}

func splitList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
