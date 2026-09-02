package main

import (
	"log/slog"
	"strings"

	"github.com/offloadintelligence/offload-ingest/config"
	volleyprovider "github.com/offloadintelligence/offload-ingest/internal/provider/apisports"
	golfprovider "github.com/offloadintelligence/offload-ingest/internal/provider/golf"
)

// Providers holds the directly-constructed upstream clients.
//
// These are threaded from configuration rather than reaching for the
// environment themselves — the constructors take a key, and the key comes from
// the single Config the process loaded at startup. Nothing below cmd/ calls
// os.Getenv.
type Providers struct {
	Golf       *golfprovider.Client
	Volleyball *volleyprovider.Client
}

// buildProviders constructs the providers a run needs.
//
// A provider is only built when its credential is present, and the caller
// decides whether a missing one is fatal. That split matters: simulation
// contacts no upstream and must run with no credentials at all, so a
// constructor that terminated the process on a missing key would make the load
// test — the thing designed to run without credentials — refuse to start.
//
// Where a credential IS required, the failure is a fatal at startup with a
// message naming the variable; see requireProviders.
func buildProviders(cfg *config.Config, log *slog.Logger) *Providers {
	p := &Providers{}

	if strings.TrimSpace(cfg.GolfAPIKey) != "" {
		p.Golf = golfprovider.New(cfg.GolfAPIKey).Configure(
			golfprovider.WithCachePath(cfg.GolfCachePath),
		)
		log.Debug("golf provider ready",
			"host", golfprovider.Host,
			"cache", p.Golf.CachePath(),
			"key", config.Redact(cfg.GolfAPIKey))
	}

	if strings.TrimSpace(cfg.APISportsKey) != "" {
		p.Volleyball = volleyprovider.New(cfg.APISportsKey)
		log.Debug("volleyball provider ready",
			"vertical", string(volleyprovider.Vertical),
			"key", config.Redact(cfg.APISportsKey))
	}
	return p
}

// requireProviders terminates the process when a provider a run depends on
// could not be constructed.
//
// The fatal lives here, at the top of the command, rather than inside a
// constructor. A library that exits on its own behalf cannot be tested or
// embedded, and the operator is better served by one message naming every
// missing variable than by whichever one happened to be reached first.
func requireProviders(cfg *config.Config, need ...config.Field) error {
	return cfg.Validate(need...)
}

// golfHost and volleyballVertical keep the provider package names out of
// main.go's import list for one-line log statements.
func golfHost() string           { return golfprovider.Host }
func volleyballVertical() string { return string(volleyprovider.Vertical) }
