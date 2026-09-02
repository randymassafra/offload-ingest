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
	if err := os.WriteFile(path, []byte("APISPORTS_KEY=abcd1234efgh5678\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("APISPORTS_KEY", "")
	os.Unsetenv("APISPORTS_KEY")

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
	if err := os.WriteFile(path, []byte("APISPORTS_KEY=from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("APISPORTS_KEY", "from-environment")

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
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte("APISPORTS_KEY=parent-key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(root, "cmd", "loadtest")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(nested)
	t.Setenv("APISPORTS_KEY", "")
	os.Unsetenv("APISPORTS_KEY")

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

// TestMalformedFileFails covers the inputs godotenv actually rejects: a line
// with no assignment, and an unterminated quoted value. A silently ignored
// syntax error would leave the process running without the credential it
// thinks it loaded, so these must surface rather than being swallowed.
func TestMalformedFileFails(t *testing.T) {
	cases := map[string]string{
		"line with no assignment": "notakeyvalueline\n",
		"unterminated quote":      "APISPORTS_KEY=\"unterminated\n",
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

// --- centralized Load --------------------------------------------------------

// TestLoadReadsEveryVariableOnce is the centralization contract: one call
// produces the whole configuration, so a deployment's requirements are readable
// from a single struct rather than by grepping for os.Getenv.
func TestLoadReadsEveryVariableOnce(t *testing.T) {
	for k, v := range map[string]string{
		"OFFLOAD_MODE":            "production",
		"OFFLOAD_LICENSE_PATH":    "/etc/offload/license.key",
		"OFFLOAD_LICENSE_PUBKEY":  "cHVibGljLWtleQ==",
		"APISPORTS_KEY":           "api-sports-key",
		"GOLF_API_KEY":            "golf-key",
		"RAPIDAPI_KEY":            "rapid-key",
		"RAPIDAPI_CRICKET_HOST":   "cricket.example",
		"RAPIDAPI_ALLSCORES_HOST": "allscores.example",
		"OFFLOAD_DASHBOARD_ADDR":  ":8090",
		"OFFLOAD_METRICS_ADDR":    ":9102",
		"OFFLOAD_FLINK_ADDR":      "http://flink:8081",
		"KAFKA_BROKERS":           "kafka:9092",
		"KAFKA_SASL_PASSWORD":     "hunter2",
	} {
		t.Setenv(k, v)
	}
	cfg, err := Load(filepath.Join(t.TempDir(), "absent.env"))
	if err == nil {
		// A named-but-absent path is an error; use the search form instead.
		_ = cfg
	}
	cfg, err = Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for name, got := range map[string]string{
		"Mode": cfg.Mode, "LicensePath": cfg.LicensePath,
		"LicensePublicKey": cfg.LicensePublicKey, "APISportsKey": cfg.APISportsKey,
		"GolfAPIKey":  cfg.GolfAPIKey,
		"RapidAPIKey": cfg.RapidAPIKey, "RapidAPICricketHost": cfg.RapidAPICricketHost,
		"RapidAPIAllScoresHost": cfg.RapidAPIAllScoresHost,
		"DashboardAddr":         cfg.DashboardAddr, "MetricsAddr": cfg.MetricsAddr,
		"FlinkAddr": cfg.FlinkAddr, "KafkaBrokers": cfg.KafkaBrokers,
		"KafkaSASLPassword": cfg.KafkaSASLPassword,
	} {
		if got == "" {
			t.Errorf("%s was not populated by Load", name)
		}
	}
	if cfg.GolfAPIKey != "golf-key" {
		t.Errorf("GolfAPIKey = %q, want the dedicated GOLF_API_KEY", cfg.GolfAPIKey)
	}
}

// TestGolfKeyFallsBackToRapidAPI. Golf is served by live-golf-data, which is
// RapidAPI-hosted, so the fallback must be the RapidAPI key.
//
// This is a regression test for a real failure: the fallback was the
// key for a different vendor, and a live run sent it to RapidAPI and got 403. A
// credential fallback has to follow whichever vendor actually serves the feed.
func TestGolfKeyFallsBackToRapidAPI(t *testing.T) {
	t.Setenv("GOLF_API_KEY", "")
	t.Setenv("APISPORTS_KEY", "wrong-vendor-key")
	t.Setenv("RAPIDAPI_KEY", "rapid-key")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.GolfAPIKey != "rapid-key" {
		t.Errorf("GolfAPIKey = %q, want the RapidAPI key as fallback", cfg.GolfAPIKey)
	}
	if cfg.GolfAPIKey == "wrong-vendor-key" {
		t.Error("golf must not receive another vendor's key; live-golf-data is RapidAPI-hosted")
	}

	// And a dedicated key takes precedence when provisioned.
	t.Setenv("GOLF_API_KEY", "dedicated")
	cfg, _ = Load("")
	if cfg.GolfAPIKey != "dedicated" {
		t.Errorf("GolfAPIKey = %q, want the dedicated key to win", cfg.GolfAPIKey)
	}
}

// TestValidateReportsEveryMissingFieldAtOnce: an operator setting up an
// appliance should learn about three missing credentials once, not discover
// them one restart at a time.
func TestValidateReportsEveryMissingFieldAtOnce(t *testing.T) {
	for _, k := range []string{
		"APISPORTS_KEY", "APISPORTS_KEY", "GOLF_API_KEY", "RAPIDAPI_KEY",
	} {
		t.Setenv(k, "")
	}
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	err = cfg.Validate(RequireAPISports, RequireGolf, RequireRapidAPI)
	if err == nil {
		t.Fatal("want an error when required fields are missing")
	}
	for _, want := range []string{"APISPORTS_KEY", "GOLF_API_KEY", "RAPIDAPI_KEY"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not name the missing %s:\n%s", want, err)
		}
	}
	// The message must tell an operator what to do, not only what is wrong.
	if !strings.Contains(err.Error(), ".env") {
		t.Errorf("error gives no remedy:\n%s", err)
	}
}

func TestValidatePassesWhenSatisfied(t *testing.T) {
	t.Setenv("APISPORTS_KEY", "present")
	t.Setenv("GOLF_API_KEY", "present")
	cfg, _ := Load("")
	if err := cfg.Validate(RequireAPISports, RequireGolf); err != nil {
		t.Errorf("Validate: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate with no requirements: %v", err)
	}
}

// TestWhitespaceOnlyValueCountsAsMissing. A variable set to spaces in a .env is
// a typo, not a credential, and treating it as present fails later and further
// from the cause.
func TestWhitespaceOnlyValueCountsAsMissing(t *testing.T) {
	t.Setenv("APISPORTS_KEY", "   ")
	cfg, _ := Load("")
	if cfg.APISportsKey != "" {
		t.Errorf("APISportsKey = %q, want it treated as unset", cfg.APISportsKey)
	}
	if err := cfg.Validate(RequireAPISports); err == nil {
		t.Error("a whitespace-only value should not satisfy a requirement")
	}
}

// TestLicensePathDefaults keeps a deployment from having to set every variable.
func TestLicensePathDefaults(t *testing.T) {
	t.Setenv("OFFLOAD_LICENSE_PATH", "")
	cfg, _ := Load("")
	if cfg.LicensePath != "license.key" {
		t.Errorf("LicensePath = %q, want the license.key default", cfg.LicensePath)
	}
}

// TestSourceReportsWhichFileWasLoaded, so a support engineer can tell whether a
// venue's values came from the file they think they edited.
func TestSourceReportsWhichFileWasLoaded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("APISPORTS_KEY=from-file\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Source != path {
		t.Errorf("Source = %q, want %q", cfg.Source, path)
	}
}

// TestRealEnvironmentBeatsTheFile is the precedence a container relies on: an
// injected credential must win over a stray .env baked into an image.
func TestRealEnvironmentBeatsTheFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("APISPORTS_KEY=from-file\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Setenv("APISPORTS_KEY", "from-environment")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.APISportsKey != "from-environment" {
		t.Errorf("APISportsKey = %q, want the real environment to win", cfg.APISportsKey)
	}
}

// TestValidateNeverLeaksASecret. The error names variables and is likely to be
// pasted into a support ticket; it must never carry a value.
func TestValidateNeverLeaksASecret(t *testing.T) {
	t.Setenv("APISPORTS_KEY", "super-secret-value")
	// Golf falls back to the RapidAPI key, so that has to be cleared too for
	// the field to actually be missing.
	t.Setenv("GOLF_API_KEY", "")
	t.Setenv("RAPIDAPI_KEY", "")
	cfg, _ := Load("")
	err := cfg.Validate(RequireAPISports, RequireGolf)
	if err == nil {
		t.Fatal("want an error")
	}
	if strings.Contains(err.Error(), "super-secret-value") {
		t.Errorf("the error leaked a credential:\n%s", err)
	}
}

// --- authorised scopes -------------------------------------------------------

// TestAuthorizedScopesAggregatesBothClaims. The licence carries two claims and
// both must reach the enforced list.
func TestAuthorizedScopesAggregatesBothClaims(t *testing.T) {
	authorized, unconstrained := AuthorizedScopes(
		[]string{"nfl", "soccer", "afl"}, []string{"us", "eu", "apac"})

	bySport := map[string][]int{}
	for _, a := range authorized {
		bySport[a.Sport] = append(bySport[a.Sport], a.ID)
	}
	if len(bySport["nfl"]) == 0 {
		t.Error("the NFL claim produced no authorised league")
	}
	if len(bySport["soccer"]) < 5 {
		t.Errorf("soccer authorised %d leagues, expected the licensed set", len(bySport["soccer"]))
	}
	// AFL's host serves one competition, so it carries no league restriction.
	var aflUnconstrained bool
	for _, s := range unconstrained {
		if s == "afl" {
			aflUnconstrained = true
		}
	}
	if !aflUnconstrained && len(bySport["afl"]) == 0 {
		t.Error("afl is neither constrained nor unconstrained; it was dropped entirely")
	}
}

// TestRegionCannotWidenTheAuthorizedList. Regions describe a package; they must
// never become a second, looser path to content.
func TestRegionCannotWidenTheAuthorizedList(t *testing.T) {
	authorized, unconstrained := AuthorizedScopes([]string{"nfl"}, []string{"global"})
	for _, a := range authorized {
		if a.Sport != "nfl" {
			t.Errorf("region 'global' widened a single-sport licence to %s", a.Sport)
		}
	}
	for _, s := range unconstrained {
		if s != "nfl" {
			t.Errorf("region 'global' unconstrained %s on an NFL-only licence", s)
		}
	}
}

// TestNoSportClaimAuthorizesNothing: an omission must not widen entitlement.
func TestNoSportClaimAuthorizesNothing(t *testing.T) {
	authorized, unconstrained := AuthorizedScopes(nil, []string{"global"})
	if len(authorized) != 0 || len(unconstrained) != 0 {
		t.Errorf("a licence with no sports authorised %d scopes and %d sports",
			len(authorized), len(unconstrained))
	}
}

// TestClaimSourceDistinguishesSportFromRegion, so a licence audit can say how a
// competition was granted.
func TestClaimSourceDistinguishesSportFromRegion(t *testing.T) {
	authorized, _ := AuthorizedScopes([]string{"soccer"}, []string{"eu"})
	if len(authorized) == 0 {
		t.Fatal("no scopes authorised")
	}
	for _, a := range authorized {
		if a.Source != "region:eu" {
			t.Errorf("%s source = %q, want it attributed to the eu bundle", a, a.Source)
		}
	}

	// With no region claim, the sports list is the grant.
	authorized, _ = AuthorizedScopes([]string{"soccer"}, nil)
	for _, a := range authorized {
		if a.Source != "sport" {
			t.Errorf("%s source = %q, want \"sport\"", a, a.Source)
		}
	}
}

// TestAuthorizedScopesAreNamed. A drop reported as "league 481 is not licensed"
// is unactionable; one naming the competition is not.
func TestAuthorizedScopesAreNamed(t *testing.T) {
	authorized, _ := AuthorizedScopes([]string{"soccer"}, nil)
	for _, a := range authorized {
		if a.Name == "" {
			t.Errorf("league %d carries no competition name", a.ID)
		}
	}
}
