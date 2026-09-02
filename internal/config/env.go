// Package config loads process configuration from the environment, including a
// local .env file when one is present.
package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

// EnvFile is the default filename searched for.
const EnvFile = ".env"

// LoadEnv loads key/value pairs from a .env file into the process environment.
//
// Values already present in the environment win: godotenv.Load does not
// overwrite, which is what lets a container's real environment take precedence
// over a stray .env baked into an image.
//
// path may be empty, in which case the file is searched for in the working
// directory and then in each parent directory. That means the binary behaves
// the same whether it is run from the module root or from cmd/loadtest.
//
// A missing file is not an error: production runs inject real environment
// variables and have no .env at all. Only a malformed file fails the load.
func LoadEnv(path string) (string, error) {
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

// APIKey returns the SportsDataIO subscription key from the environment.
//
// SPORTS_DATA_IO_API_KEY is the documented name; SPORTSDATAIO_API_KEY is
// accepted as an alias so earlier deployments keep working.
func APIKey() string {
	for _, name := range []string{"SPORTS_DATA_IO_API_KEY", "SPORTSDATAIO_API_KEY"} {
		if v := os.Getenv(name); v != "" {
			return v
		}
	}
	return ""
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
