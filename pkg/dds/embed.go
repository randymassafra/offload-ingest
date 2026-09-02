package dds

import (
	_ "embed"
	"html/template"
	"strings"
)

// The design system's assets are compiled into every product binary.
//
// go:embed rather than a file path because a venue appliance is shipped as one
// executable; a dashboard that depends on a sibling directory being present is
// a dashboard that breaks the first time someone copies the binary somewhere.
var (
	//go:embed assets/dds.css
	styleSheet string
	//go:embed assets/dds.js
	script string
	//go:embed assets/logo.svg
	logoSVG string
)

// CSS returns the design system stylesheet.
func CSS() string { return styleSheet }

// JS returns the shared rendering primitives.
func JS() string { return script }

// Logo returns the Offload motif as an inline SVG.
func Logo() template.HTML { return template.HTML(logoSVG) }

// ShellOptions configures the page shell.
type ShellOptions struct {
	Product Product
	// StateURL is the product's own JSON endpoint, polled by the page.
	StateURL string
	// Sidebar is the markup for the left rail; products render their own.
	Sidebar template.HTML
	// Body is the main region — normally a .dds-grid of cards.
	Body template.HTML
	// Script is the product's render function, appended after the DDS
	// primitives. It receives the decoded state object.
	Script template.JS
}

// Shell renders the complete page: one document, no external references.
//
// The header, the sidebar rail and the grid are the design system's; everything
// inside them is the product's. That split is what keeps four dashboards
// recognisably one suite while letting each measure entirely different things.
func Shell(o ShellOptions) string {
	o.Product.DDSVersion = Version
	if o.StateURL == "" {
		o.StateURL = "/api/state"
	}
	var b strings.Builder
	b.WriteString(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="dark">
<title>`)
	b.WriteString(template.HTMLEscapeString(o.Product.Name))
	b.WriteString(`</title>
<style>`)
	b.WriteString(styleSheet)
	b.WriteString(`</style>
</head>
<body>
<header class="dds-header" id="dds-header">
  `)
	b.WriteString(logoSVG)
	b.WriteString(`
  <h1 class="dds-product">`)
	b.WriteString(template.HTMLEscapeString(o.Product.Name))
	b.WriteString(`<small>Offload Intelligence</small></h1>
  <span class="dds-mode" data-dds="mode" data-mode="simulation">—</span>
  <span class="dds-header-spacer"></span>
  <div class="dds-header-meta">
    <div class="dds-status">
      <span class="dds-lamp" data-dds="status-lamp" data-health="unknown"></span>
      <span data-dds="status-text">connecting…</span>
    </div>
    <div>Last updated<b data-dds="updated">—</b></div>
    <div>Build<b>`)
	b.WriteString(template.HTMLEscapeString(o.Product.Version))
	b.WriteString(`</b></div>
    <div>DDS<b>`)
	b.WriteString(template.HTMLEscapeString(Version))
	b.WriteString(`</b></div>
  </div>
</header>
<div class="dds-shell">
  <aside class="dds-sidebar" id="dds-sidebar">`)
	b.WriteString(string(o.Sidebar))
	b.WriteString(`</aside>
  <main class="dds-main">`)
	b.WriteString(string(o.Body))
	b.WriteString(`</main>
</div>
<script>`)
	b.WriteString(script)
	b.WriteString("\n")
	b.WriteString(string(o.Script))
	b.WriteString(`
DDS.poll(` + jsString(o.StateURL) + `, render, 2000);
</script>
</body>
</html>
`)
	return b.String()
}

// jsString quotes a value for embedding in the inline script.
func jsString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\n':
			b.WriteString("\\n")
		case '<':
			// Prevents a value ending the script element early.
			b.WriteString("\\u003c")
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
