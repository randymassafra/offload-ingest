package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// runInspect explores a captured document: its keys, its JSON paths, array
// sizes and a sample record at a given path.
//
// Onboarding a provider means a lot of "what shape is this?" poking. Doing that
// with throwaway scripts in another language leaves nothing behind and makes
// the workflow depend on a toolchain the project does not otherwise need, so it
// lives here instead — the same place the capture, inference and comparison do.
func runInspect(file, root, path string, depth int, showSample bool) error {
	raw, err := readCapture(file, root)
	if err != nil {
		return err
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("%s: %w", file, err)
	}

	target := doc
	if path != "" {
		recs := records(doc, path)
		if len(recs) == 0 {
			// The path may point at a scalar or array rather than objects.
			target = navigate(doc, path)
			if target == nil {
				return fmt.Errorf("nothing found at path %q", path)
			}
		} else {
			fmt.Printf("%d object(s) at %q\n\n", len(recs), path)
			target = recs[0]
		}
	}

	fmt.Printf("%-9s %s\n", "file:", file)
	fmt.Printf("%-9s %d bytes\n", "size:", len(raw))
	if path != "" {
		fmt.Printf("%-9s %s\n", "path:", path)
	}

	switch t := target.(type) {
	case map[string]any:
		fmt.Printf("%-9s object, %d field(s)\n\n", "type:", len(t))
		printFields(t)
	case []any:
		fmt.Printf("%-9s array, %d element(s)\n\n", "type:", len(t))
		if len(t) > 0 {
			if obj, ok := t[0].(map[string]any); ok {
				fmt.Println("first element:")
				printFields(obj)
			} else {
				fmt.Printf("first element: %v\n", t[0])
			}
		}
	default:
		fmt.Printf("%-9s scalar: %v\n", "type:", t)
	}

	all := pathsOf(target)
	fmt.Printf("\n%d JSON path(s)", len(all))
	if depth > 0 {
		shown := 0
		fmt.Println(", to depth", depth)
		for _, p := range sortedStrings(all) {
			if strings.Count(p, ".") < depth {
				fmt.Println("  ", p)
				shown++
			}
		}
		if shown == 0 {
			fmt.Println("  (none within that depth)")
		}
	} else {
		fmt.Println()
	}

	if showSample {
		pretty, _ := json.MarshalIndent(target, "", " ")
		if len(pretty) > 2000 {
			pretty = append(pretty[:2000], []byte("\n… truncated")...)
		}
		fmt.Printf("\nsample:\n%s\n", pretty)
	}
	return nil
}

// printFields lists an object's fields with their inferred Go-ish types, which
// is the first thing worth knowing about an unfamiliar document.
func printFields(obj map[string]any) {
	keys := sortedStrings(setOf(obj))
	width := 0
	for _, k := range keys {
		if len(k) > width {
			width = len(k)
		}
	}
	for _, k := range keys {
		fmt.Printf("  %-*s  %s\n", width, k, describeValue(obj[k]))
	}
}

func describeValue(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case bool:
		return fmt.Sprintf("bool (%v)", t)
	case float64:
		if t == float64(int64(t)) {
			return fmt.Sprintf("int (%d)", int64(t))
		}
		return fmt.Sprintf("float (%g)", t)
	case string:
		if len(t) > 40 {
			t = t[:40] + "…"
		}
		return fmt.Sprintf("string (%q)", t)
	case []any:
		if len(t) == 0 {
			return "array (empty)"
		}
		inner := "scalar"
		if _, ok := t[0].(map[string]any); ok {
			inner = "object"
		} else if _, ok := t[0].([]any); ok {
			inner = "array"
		}
		return fmt.Sprintf("array of %s (%d)", inner, len(t))
	case map[string]any:
		return fmt.Sprintf("object (%d field(s))", len(t))
	}
	return "?"
}

// navigate walks a dotted path, descending into the first element of any array
// it meets, and returns whatever is there.
func navigate(doc any, path string) any {
	cur := doc
	for _, seg := range strings.Split(path, ".") {
		if arr, ok := cur.([]any); ok {
			if len(arr) == 0 {
				return nil
			}
			cur = arr[0]
		}
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur, ok = obj[seg]
		if !ok {
			return nil
		}
	}
	return cur
}

// readCapture reads a path, falling back to one relative to the fixtures root.
func readCapture(file, root string) ([]byte, error) {
	if raw, err := os.ReadFile(file); err == nil {
		return raw, nil
	}
	return os.ReadFile(filepath.Join(root, file))
}

func setOf(m map[string]any) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}

func sortedStrings(s map[string]bool) []string {
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
