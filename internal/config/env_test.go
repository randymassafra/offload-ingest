package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadEnvReadsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("SPORTS_DATA_IO_API_KEY=abcd1234efgh5678\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SPORTS_DATA_IO_API_KEY", "")
	os.Unsetenv("SPORTS_DATA_IO_API_KEY")

	got, err := LoadEnv(path)
	if err != nil {
		t.Fatalf("LoadEnv: %v", err)
	}
	if got != path {
		t.Errorf("loaded %q, want %q", got, path)
	}
	if APIKey() != "abcd1234efgh5678" {
		t.Errorf("APIKey = %q", APIKey())
	}
}

// TestRealEnvironmentWins is the property that keeps a stray .env in an image
// from overriding the credentials a deployment actually injects.
func TestRealEnvironmentWins(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("SPORTS_DATA_IO_API_KEY=from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SPORTS_DATA_IO_API_KEY", "from-environment")

	if _, err := LoadEnv(path); err != nil {
		t.Fatalf("LoadEnv: %v", err)
	}
	if got := APIKey(); got != "from-environment" {
		t.Errorf("APIKey = %q, want the real environment to win", got)
	}
}

func TestMissingFileIsNotAnError(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	path, err := LoadEnv("")
	if err != nil {
		t.Errorf("a missing .env must not fail: %v", err)
	}
	if path != "" {
		t.Errorf("found %q in an empty tree", path)
	}
}

func TestLoadEnvSearchesParents(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte("SPORTS_DATA_IO_API_KEY=parent-key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(root, "cmd", "loadtest")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(nested)
	t.Setenv("SPORTS_DATA_IO_API_KEY", "")
	os.Unsetenv("SPORTS_DATA_IO_API_KEY")

	got, err := LoadEnv("")
	if err != nil {
		t.Fatalf("LoadEnv: %v", err)
	}
	if got == "" {
		t.Fatal("did not find .env in a parent directory")
	}
	if APIKey() != "parent-key" {
		t.Errorf("APIKey = %q", APIKey())
	}
}

func TestAliasIsAccepted(t *testing.T) {
	t.Setenv("SPORTS_DATA_IO_API_KEY", "")
	os.Unsetenv("SPORTS_DATA_IO_API_KEY")
	t.Setenv("SPORTSDATAIO_API_KEY", "legacy-name")
	if got := APIKey(); got != "legacy-name" {
		t.Errorf("APIKey = %q, want the alias to be honoured", got)
	}
}

// TestMalformedFileFails covers the inputs godotenv actually rejects: a line
// with no assignment, and an unterminated quoted value. A silently ignored
// syntax error would leave the process running without the credential it
// thinks it loaded, so these must surface rather than being swallowed.
func TestMalformedFileFails(t *testing.T) {
	cases := map[string]string{
		"line with no assignment": "notakeyvalueline\n",
		"unterminated quote":      "SPORTS_DATA_IO_API_KEY=\"unterminated\n",
	}
	for name, body := range cases {
		dir := t.TempDir()
		path := filepath.Join(dir, ".env")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadEnv(path); err == nil {
			t.Errorf("%s: expected a load error", name)
		}
	}
}

// TestRedactNeverLeaksTheSecret guards the one place a key could reach a log.
func TestRedactNeverLeaksTheSecret(t *testing.T) {
	// A synthetic key of the same shape as a real one. Never put a live
	// credential in source, even as a test fixture.
	secret := "0123456789abcdef0123456789abcdef"
	got := Redact(secret)
	if strings.Contains(got, secret) {
		t.Fatal("Redact returned the full secret")
	}
	if len(got) > 12 {
		t.Errorf("Redact = %q, too much of the key is shown", got)
	}
	if Redact("") != "(unset)" {
		t.Errorf("Redact(\"\") = %q", Redact(""))
	}
	if Redact("short") != "****" {
		t.Errorf("Redact(short) = %q", Redact("short"))
	}
}
