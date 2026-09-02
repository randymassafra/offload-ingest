// Package dds is the Offload Intelligence Dashboard Design System.
//
// It exists because four products — Ingest, LiveMesh, Relic and Atmos — each
// need an operator dashboard, and four independently-styled dashboards is how a
// suite stops looking like one product. The palette, the grid, the card anatomy
// and the alert behaviour live here once and are consumed by all of them.
//
// # What is shared, and what is not
//
// Shared: visual tokens, the twelve-column grid, the card component, the
// sparkline and health-lamp primitives, and the alert pulse. Those are design
// decisions and must not drift per product.
//
// Not shared: what the cards contain. Each product owns its own JSON state
// endpoint and decides which metrics matter. The design system renders; it does
// not model.
//
// # No external assets, ever
//
// The assets are compiled into the binary with go:embed and served from the
// product's own listener. A venue appliance may have no outbound internet
// beyond its data provider, and a dashboard that renders blank because a CDN is
// unreachable fails at exactly the moment someone needs it. A test in this
// package asserts the CSS and JS reference nothing external.
package dds

// Version is the design system revision. Products report it on their dashboard
// so a support engineer can tell at a glance whether a venue is running current
// styling, and so a breaking token change can be rolled out deliberately.
const Version = "1.0.0"

// Palette is the Offload Intelligence colour system.
//
// The three mandated tokens are Background, Highlight and Label. The rest are
// derived companions chosen to sit correctly on deep navy: raising a panel
// above the page needs a lighter navy rather than a grey, and the status
// colours must stay distinguishable from the electric-blue highlight — a red
// alert next to a highlight that is also a bright cyan-green reads as two
// kinds of "good" unless the status green is pulled away from it.
const (
	// Mandated by the DDS.
	Background = "#0A192F" // deep navy — the page ground
	Highlight  = "#64FFDA" // electric blue — primary values, accents, the logo
	Label      = "#8892B0" // slate gray — secondary labels and axis text

	// Derived companions.
	Panel  = "#112240" // raised surface for cards
	Line   = "#1D3557" // hairline borders
	Text   = "#CCD6F6" // primary body text
	Subtle = "#0D2137" // recessed wells, sparkline backing

	// Status. Deliberately distinct in hue from Highlight so a healthy card and
	// a highlighted value are not the same green.
	StatusOK   = "#3DDC97" // green
	StatusWarn = "#FFC857" // amber
	StatusBad  = "#FF5C5C" // red
)

// Health is a card's status lamp.
type Health string

const (
	HealthOK      Health = "ok"
	HealthWarn    Health = "warn"
	HealthBad     Health = "bad"
	HealthUnknown Health = "unknown"
)

// Product identifies the dashboard's owner in the shared header.
type Product struct {
	// Name is the product, e.g. "Offload Ingest".
	Name string `json:"name"`
	// Version is the product build, not the design system's.
	Version string `json:"version"`
	// DDSVersion is stamped by Shell so a page always reports the design
	// system it was actually rendered with.
	DDSVersion string `json:"dds_version"`
}

// Thresholds are the DDS-wide alerting rules.
//
// They live in the design system rather than in each product because "when
// does a card pulse" is a design decision: an operator learns that a pulsing
// card means the same thing in Ingest as it does in Atmos, and that only holds
// if the rule is defined once.
const (
	// LatencyAlertMS is the poll-to-publish delta above which a latency card
	// alerts. Two seconds is the point at which a live score is visibly stale
	// on a screen behind a bar.
	LatencyAlertMS = 2000
	// ErrorRateAlert is the fraction of requests failing above which an error
	// card alerts.
	ErrorRateAlert = 0.05
)

// ClassifyRatio maps a 0..1 utilisation-style value onto a health lamp, using
// the DDS's standard two-stage thresholds.
//
// Shared so that "amber" means the same proportion everywhere. A product that
// wants different bands for a specific card should say so explicitly rather
// than quietly passing a differently-scaled number.
func ClassifyRatio(v, warnAt, badAt float64) Health {
	switch {
	case v >= badAt:
		return HealthBad
	case v >= warnAt:
		return HealthWarn
	default:
		return HealthOK
	}
}

// ClassifyLatency maps a millisecond latency onto a lamp.
func ClassifyLatency(ms float64) Health {
	switch {
	case ms <= 0:
		return HealthUnknown
	case ms >= LatencyAlertMS:
		return HealthBad
	case ms >= LatencyAlertMS/2:
		return HealthWarn
	default:
		return HealthOK
	}
}

// ClassifyErrorRate maps a 0..1 error rate onto a lamp.
func ClassifyErrorRate(rate float64) Health {
	switch {
	case rate >= ErrorRateAlert:
		return HealthBad
	case rate >= ErrorRateAlert/2:
		return HealthWarn
	default:
		return HealthOK
	}
}
