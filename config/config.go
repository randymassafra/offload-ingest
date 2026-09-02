// Package config loads process configuration from the environment, including a
// local .env file when one is present.
//
// This package sits at the module root rather than under internal/, which is a
// deliberate API decision and not just a layout one: anything under internal/
// cannot be imported by another module, so being here makes it part of this
// module's public surface. The other Offload Intelligence products load
// credentials the same way, and a second product importing
// github.com/offloadintelligence/offload-ingest/config is now possible where it
// was not before.
//
// # The centralized pattern
//
// Every environment variable the process understands is read once, at startup,
// by Load, into a single Config value that is then passed down. Nothing below
// this package calls os.Getenv.
//
// That matters for more than tidiness. Scattered os.Getenv calls mean the set
// of things a deployment must supply is only discoverable by grepping, a typo
// in a variable name fails silently at the moment the credential is first used
// — often hours into a run — and a component cannot be tested without mutating
// process-global state. Reading everything up front turns all three into a
// startup error with a list of what is missing.
//
// # Required fields
//
// Requirements are contextual, and stated by the caller:
//
//	cfg := config.MustLoad(config.RequireAPISports)
//
// A credential is required when the work about to be done needs it, not
// whenever the binary starts. Simulation mode contacts no provider and needs no
// key at all; a venue not licensed for golf has no reason to hold a golf key.
// Requiring everything unconditionally would make the load test — the thing
// that is supposed to run with no credentials — refuse to start.
package config

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/joho/godotenv"
)

// EnvFile is the default filename searched for.
const EnvFile = ".env"

// Config is every environment variable this process understands.
//
// Grouped by the concern that owns them, because the grouping is the
// documentation: a deployment engineer reading this struct can see exactly what
// a venue appliance has to be given.
type Config struct {
	// Source is the .env file that was loaded, empty when none was found.
	Source string

	// --- mode and licensing ---

	// Mode is simulation or production. Empty means simulation; see the note
	// on Load about why that default is deliberate.
	Mode string
	// LicensePath is the licence file. Defaults to license.key.
	LicensePath string
	// LicensePublicKey is the base64 Ed25519 verification key. Release builds
	// embed it with -ldflags; this is the development path.
	LicensePublicKey string

	// --- providers ---

	// APISportsKey authenticates all twelve API-Sports hosts. This is the
	// primary provider and covers ten of the fourteen sports.
	APISportsKey string
	// GolfAPIKey is the credential the golf feed uses.
	//
	// It reads GOLF_API_KEY when set and otherwise falls back to RapidAPIKey,
	// because golf is served by live-golf-data, which is RapidAPI-hosted.
	//
	// The fallback has to match whichever vendor actually serves the feed: it
	// once pointed at a SportsDataIO key and a live run returned HTTP 403,
	// because live-golf-data is RapidAPI-hosted. The dedicated name exists so a
	// separately-metered golf subscription is a configuration change rather
	// than a code one.
	GolfAPIKey string
	// RapidAPIKey authenticates the RapidAPI-fronted providers.
	RapidAPIKey string
	// RapidAPICricketHost and RapidAPIAllScoresHost are the per-provider hosts
	// that key is used against.
	RapidAPICricketHost   string
	RapidAPIAllScoresHost string

	// GolfCachePath is where the golf provider caches its leaderboard.
	GolfCachePath string

	// --- observability ---

	DashboardAddr string
	MetricsAddr   string
	// FlinkAddr optionally enables the downstream state scraper. Empty leaves
	// it off, which is the recommended architecture.
	FlinkAddr string

	// --- kafka ---

	KafkaBrokers      string
	KafkaSASLPassword string
}

// Field identifies a configuration field for requirement checks.
type Field string

// The fields a caller can require.
const (
	RequireAPISports  Field = "APISPORTS_KEY"
	RequireGolf       Field = "GOLF_API_KEY"
	RequireRapidAPI   Field = "RAPIDAPI_KEY"
	RequireLicenseKey Field = "OFFLOAD_LICENSE_PUBKEY"
)

// value returns the loaded value for a field.
func (c *Config) value(f Field) string {
	switch f {
	case RequireAPISports:
		return c.APISportsKey
	case RequireGolf:
		return c.GolfAPIKey
	case RequireRapidAPI:
		return c.RapidAPIKey
	case RequireLicenseKey:
		return c.LicensePublicKey
	default:
		return ""
	}
}

// hint explains where a missing value comes from, so the error tells an
// operator what to do rather than only what is wrong.
func hint(f Field) string {
	switch f {
	case RequireAPISports:
		return "the primary provider key, from api-sports.io"
	case RequireGolf:
		return "the golf feed's key for live-golf-data; falls back to RAPIDAPI_KEY when unset"
	case RequireRapidAPI:
		return "serves cricket and tennis, from rapidapi.com"
	case RequireLicenseKey:
		return "base64 Ed25519 public key; release builds embed it instead"
	default:
		return ""
	}
}

// LoadConfig reads the whole configuration, searching for a .env file in the
// working directory and its parents.
//
// This is the entry point for a normal startup. Load takes an explicit path for
// the cases that need one — a test, or a deployment that pins the file.
func LoadConfig() (*Config, error) { return Load("") }

// Load reads the .env file, then the environment, into a Config.
//
// path may be empty, in which case the file is searched for in the working
// directory and then each parent. That means the binary behaves the same
// whether it is run from the module root or from cmd/loadtest.
//
// A missing .env is not an error: production runs inject real environment
// variables and have no file at all. Only a malformed file fails the load.
//
// Values already present in the environment win over the file, which is what
// lets a container's injected credentials take precedence over a stray .env
// baked into an image.
func Load(path string) (*Config, error) {
	source, err := loadEnvFile(path)
	if err != nil {
		return nil, err
	}

	c := &Config{
		Source: source,

		Mode:             os.Getenv("OFFLOAD_MODE"),
		LicensePath:      firstSet("OFFLOAD_LICENSE_PATH"),
		LicensePublicKey: firstSet("OFFLOAD_LICENSE_PUBKEY"),

		APISportsKey:          firstSet("APISPORTS_KEY"),
		RapidAPIKey:           firstSet("RAPIDAPI_KEY"),
		RapidAPICricketHost:   firstSet("RAPIDAPI_CRICKET_HOST"),
		RapidAPIAllScoresHost: firstSet("RAPIDAPI_ALLSCORES_HOST"),
		GolfCachePath:         firstSet("GOLF_CACHE_PATH"),

		DashboardAddr: firstSet("OFFLOAD_DASHBOARD_ADDR"),
		MetricsAddr:   firstSet("OFFLOAD_METRICS_ADDR"),
		FlinkAddr:     firstSet("OFFLOAD_FLINK_ADDR"),

		KafkaBrokers:      firstSet("KAFKA_BROKERS"),
		KafkaSASLPassword: firstSet("KAFKA_SASL_PASSWORD"),
	}

	// Golf takes its own key when one is provisioned and otherwise rides on the
	// RapidAPI subscription that serves live-golf-data.
	c.GolfAPIKey = firstSet("GOLF_API_KEY")
	if c.GolfAPIKey == "" {
		c.GolfAPIKey = c.RapidAPIKey
	}

	if c.LicensePath == "" {
		c.LicensePath = "license.key"
	}
	if c.GolfCachePath == "" {
		c.GolfCachePath = "testdata/golf_cache.json"
	}
	return c, nil
}

// Validate reports every required field that is missing, in one error.
//
// All of them, not the first: an operator setting up an appliance should learn
// about three missing credentials once, rather than discovering them one
// restart at a time.
func (c *Config) Validate(required ...Field) error {
	var missing []Field
	for _, f := range required {
		if strings.TrimSpace(c.value(f)) == "" {
			missing = append(missing, f)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })

	var b strings.Builder
	b.WriteString("config: missing required environment ")
	if len(missing) == 1 {
		b.WriteString("variable:\n")
	} else {
		fmt.Fprintf(&b, "variables (%d):\n", len(missing))
	}
	for _, f := range missing {
		fmt.Fprintf(&b, "  %-24s %s\n", string(f), hint(f))
	}
	if c.Source == "" {
		b.WriteString("\nNo .env file was found. Copy .env.example to .env, or export these directly.")
	} else {
		fmt.Fprintf(&b, "\nLoaded %s; add the missing entries there, or export them directly.", c.Source)
	}
	return fmt.Errorf("%s", b.String())
}

// MustLoad loads the configuration and terminates the process if a required
// field is missing.
//
// This is the startup gate for a binary that cannot do anything useful without
// its credentials. It is deliberately the only place in the codebase that
// calls log.Fatalf: a library that exits on its own behalf is untestable and
// impossible to embed, so the decision to terminate belongs at the top of a
// command, once, where the operator sees a single clear message.
func MustLoad(required ...Field) *Config {
	cfg, err := Load("")
	if err != nil {
		log.Fatalf("%v", err)
	}
	if err := cfg.Validate(required...); err != nil {
		log.Fatalf("%v", err)
	}
	return cfg
}

// loadEnvFile loads a .env, returning the path that was used.
func loadEnvFile(path string) (string, error) {
	if path != "" {
		if err := godotenv.Load(path); err != nil {
			return "", fmt.Errorf("config: load %s: %w", path, err)
		}
		return path, nil
	}
	found, err := findUp(EnvFile)
	if err != nil || found == "" {
		return "", err
	}
	if err := godotenv.Load(found); err != nil {
		return "", fmt.Errorf("config: load %s: %w", found, err)
	}
	return found, nil
}

// firstSet returns the first of names that is set and non-empty.
func firstSet(names ...string) string {
	for _, n := range names {
		if v := strings.TrimSpace(os.Getenv(n)); v != "" {
			return v
		}
	}
	return ""
}

// findUp walks from the working directory towards the filesystem root looking
// for name, returning the first match, or an empty string when there is none.
func findUp(name string) (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("config: working directory: %w", err)
	}
	for {
		candidate := filepath.Join(dir, name)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", nil // reached the root
		}
		dir = parent
	}
}

// Redact renders a secret for logging: enough to identify which key is loaded,
// never enough to use it. Nothing should ever log the raw value.
func Redact(secret string) string {
	if secret == "" {
		return "(unset)"
	}
	if len(secret) <= 8 {
		return "****"
	}
	return secret[:4] + "…" + secret[len(secret)-4:]
}

// --- compatibility -----------------------------------------------------------

// LoadEnv loads a .env file and returns the path used.
//
// Retained because this package is now public API and callers outside the
// module may depend on it. New code should use Load, which reads every variable
// once instead of leaving each caller to fetch its own.
//
// Deprecated: use Load.
func LoadEnv(path string) (string, error) { return loadEnvFile(path) }

// APIKey returns the primary provider key.
//
// Deprecated: use Load and read Config.APISportsKey.
func APIKey() string { return firstSet("APISPORTS_KEY") }
