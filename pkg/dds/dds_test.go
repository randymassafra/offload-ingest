package dds

import (
	"strings"
	"testing"
)

// TestAssetsAreSelfContained is the design system's hard rule. A venue
// appliance may have no outbound internet beyond its data provider, and a
// dashboard that renders blank because a CDN is unreachable fails at exactly
// the moment someone needs it.
func TestAssetsAreSelfContained(t *testing.T) {
	for name, asset := range map[string]string{"dds.css": CSS(), "dds.js": JS()} {
		for _, forbidden := range []string{
			"http://", "https://", "//cdn", "fonts.googleapis", "unpkg", "jsdelivr",
		} {
			if strings.Contains(asset, forbidden) {
				t.Errorf("%s references an external resource (%q)", name, forbidden)
			}
		}
	}
	// The logo is the one asset allowed a URL, because SVG's xmlns is a
	// namespace identifier and is never fetched.
	logo := string(Logo())
	if strings.Contains(logo, "<image") || strings.Contains(logo, "xlink:href") {
		t.Error("logo pulls in an external image")
	}
}

// TestMandatedPaletteIsUsed guards the three tokens the DDS specifies by name.
func TestMandatedPaletteIsUsed(t *testing.T) {
	css := CSS()
	for name, token := range map[string]string{
		"background": Background, "highlight": Highlight, "label": Label,
	} {
		if !strings.Contains(css, token) {
			t.Errorf("the %s token %s does not appear in the stylesheet", name, token)
		}
	}
	if Background != "#0A192F" || Highlight != "#64FFDA" || Label != "#8892B0" {
		t.Error("the mandated palette has been altered")
	}
}

// TestGridIsTwelveColumns pins the layout standard.
func TestGridIsTwelveColumns(t *testing.T) {
	css := CSS()
	if !strings.Contains(css, "repeat(12, minmax(0, 1fr))") {
		t.Error("the grid is not twelve columns")
	}
	for _, span := range []string{".dds-col-3", ".dds-col-4", ".dds-col-6", ".dds-col-12"} {
		if !strings.Contains(css, span) {
			t.Errorf("column span %s is missing", span)
		}
	}
}

// TestPulseRespectsReducedMotion. Motion is an enhancement, never the only
// signal: a viewer who has asked for reduced motion must still see the alert.
func TestPulseRespectsReducedMotion(t *testing.T) {
	css := CSS()
	if !strings.Contains(css, "@keyframes dds-pulse") {
		t.Fatal("no pulse animation defined")
	}
	i := strings.Index(css, "prefers-reduced-motion")
	if i < 0 {
		t.Fatal("the pulse has no reduced-motion guard")
	}
	guard := css[i:]
	if !strings.Contains(guard, "animation: none") {
		t.Error("reduced motion does not disable the animation")
	}
	// The colour must survive, or the information is lost with the motion.
	if !strings.Contains(guard, "border-color: var(--bad)") {
		t.Error("reduced motion drops the alert colour along with the animation")
	}
}

// TestShellRendersOneCompleteDocument.
func TestShellRendersOneCompleteDocument(t *testing.T) {
	out := Shell(ShellOptions{
		Product: Product{Name: "Offload Test", Version: "1.2.3"},
		Body:    "<div id=cards></div>",
		Script:  "function render(d){}",
	})
	for _, want := range []string{
		"<!doctype html>", "Offload Test", "1.2.3", Version,
		"dds-header", "dds-sidebar", "dds-grid", "DDS.poll(",
		`data-dds="status-lamp"`, `data-dds="updated"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("shell is missing %q", want)
		}
	}
	if strings.Contains(out, "<script src=") || strings.Contains(out, `<link rel="stylesheet"`) {
		t.Error("shell pulls in an external asset")
	}
}

// TestShellEscapesProductName keeps a product name out of the markup grammar.
func TestShellEscapesProductName(t *testing.T) {
	out := Shell(ShellOptions{Product: Product{Name: `Ingest<script>alert(1)</script>`}})
	if strings.Contains(out, "<script>alert(1)</script>") {
		t.Error("product name was injected unescaped")
	}
}

// TestStateURLCannotBreakOutOfTheScript covers the one value interpolated into
// the inline script.
func TestStateURLCannotBreakOutOfTheScript(t *testing.T) {
	out := Shell(ShellOptions{
		Product:  Product{Name: "X"},
		StateURL: `/api/state"</script><script>alert(1)//`,
	})
	if strings.Contains(out, "</script><script>alert(1)") {
		t.Error("the state URL escaped its string literal")
	}
}

func TestClassifyLatency(t *testing.T) {
	for _, tc := range []struct {
		ms   float64
		want Health
	}{
		{0, HealthUnknown},
		{50, HealthOK},
		{999, HealthOK},
		{1000, HealthWarn},
		{1999, HealthWarn},
		{2000, HealthBad}, // the DDS threshold
		{9000, HealthBad},
	} {
		if got := ClassifyLatency(tc.ms); got != tc.want {
			t.Errorf("ClassifyLatency(%v) = %s, want %s", tc.ms, got, tc.want)
		}
	}
}

func TestClassifyErrorRate(t *testing.T) {
	for _, tc := range []struct {
		rate float64
		want Health
	}{
		{0, HealthOK},
		{0.01, HealthOK},
		{0.025, HealthWarn},
		{0.05, HealthBad}, // the DDS threshold
		{0.5, HealthBad},
	} {
		if got := ClassifyErrorRate(tc.rate); got != tc.want {
			t.Errorf("ClassifyErrorRate(%v) = %s, want %s", tc.rate, got, tc.want)
		}
	}
}

func TestClassifyRatio(t *testing.T) {
	if got := ClassifyRatio(0.5, 0.75, 0.9); got != HealthOK {
		t.Errorf("below both bands = %s, want ok", got)
	}
	if got := ClassifyRatio(0.8, 0.75, 0.9); got != HealthWarn {
		t.Errorf("between bands = %s, want warn", got)
	}
	if got := ClassifyRatio(0.95, 0.75, 0.9); got != HealthBad {
		t.Errorf("above both = %s, want bad", got)
	}
}

// TestThresholdsMatchTheSpecification pins the two numbers the DDS names.
func TestThresholdsMatchTheSpecification(t *testing.T) {
	if LatencyAlertMS != 2000 {
		t.Errorf("latency threshold = %d, the DDS specifies 2000ms", LatencyAlertMS)
	}
	if ErrorRateAlert != 0.05 {
		t.Errorf("error-rate threshold = %v, the DDS specifies 5%%", ErrorRateAlert)
	}
}
