// Command schematool verifies the generated feeds against the provider.
//
// Two checks, catching different classes of defect:
//
//	schemas   Does a generated payload have the same SHAPE as a captured
//	          provider response? Compares JSON path sets.
//	routes    Is each registered route real and callable? Substitutes real
//	          parameters from the captures and calls the live API.
//
// Both are needed, and neither subsumes the other. The captures were fetched by
// hand with correct URLs, so a shape comparison cannot see a wrong route: eight
// endpoints once passed the schema check at 100% while returning 404 in
// practice. Conversely a route can return 200 while the payload has drifted.
//
// This replaced a pair of Python scripts. Beyond removing a language from the
// build, being in Go lets the tool call generators.Endpoints() directly instead
// of parsing the loadtest CLI's text output, which was fragile and silently
// wrong when a column widened.
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	fixtures := flag.String("fixtures", "fixtures", "root directory of captured provider responses")
	verbose := flag.Bool("v", false, "print the field-level diff for every feed")
	fullPaths := flag.Bool("paths", false, "schemas: print whole JSON paths in the diff, not leaf names")
	providers := flag.String("providers", "all", "comma-separated providers to capture, or \"all\"")
	inferFile := flag.String("file", "", "infer: the capture to read")
	inferType := flag.String("type", "Model", "infer: the Go type name to emit")
	inferPath := flag.String("path", "", "infer/inspect: dotted path into the document, e.g. \"scorecard.batsman\"")
	inspectDepth := flag.Int("depth", 0, "inspect: list JSON paths up to this nesting depth")
	inspectSample := flag.Bool("sample", false, "inspect: print a truncated sample of the document")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(),
			"schematool verifies generated feeds against captured and live provider data.\n\n"+
				"Usage:\n"+
				"  schematool capture [flags]   refresh the captures from the live APIs\n"+
				"  schematool schemas [flags]   compare payload shape against captures\n"+
				"  schematool routes  [flags]   call every route against the live API\n"+
				"  schematool inspect [flags]   explore a capture: fields, paths, samples\n"+
				"  schematool infer   [flags]   emit Go structs inferred from a capture\n\nFlags:\n")
		flag.PrintDefaults()
	}

	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	_ = flag.CommandLine.Parse(os.Args[2:])

	var err error
	switch cmd {
	case "capture":
		err = runCapture(*fixtures, *providers)
	case "schemas":
		err = runSchemas(*fixtures, *verbose, *fullPaths)
	case "routes":
		err = runRoutes(*fixtures)
	case "infer":
		err = runInfer(*inferFile, *fixtures, *inferType, *inferPath)
	case "inspect":
		err = runInspect(*inferFile, *fixtures, *inferPath, *inspectDepth, *inspectSample)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "schematool:", err)
		os.Exit(1)
	}
}
