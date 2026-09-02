package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"unicode"
)

// runInfer prints Go struct definitions inferred from a captured response.
//
// Adding a provider means writing wire models for a schema nobody has
// documented in Go. Doing that by hand is slow and, worse, quietly lossy — the
// NCAA models were originally transcribed from documentation and turned out to
// describe 37-49% of what the API actually returns. Inferring them from a real
// response instead makes the field list exhaustive by construction.
//
// Nullability is inferred across every record in the sample: a field is a
// pointer if it is null anywhere. A field that is null in EVERY record has no
// inferable type and is flagged rather than guessed at.
func runInfer(file, root, typeName, path string) error {
	raw, err := os.ReadFile(file)
	if err != nil {
		// Allow a path relative to the fixtures root for convenience.
		raw, err = os.ReadFile(root + "/" + file)
		if err != nil {
			return err
		}
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("%s: %w", file, err)
	}

	recs := records(doc, path)
	if len(recs) == 0 {
		return fmt.Errorf("no objects found at path %q", path)
	}
	fmt.Println(inferStruct(typeName, recs))
	return nil
}

// records collects the objects at a dotted path, descending through arrays.
func records(doc any, path string) []map[string]any {
	var out []map[string]any
	var walk func(any, string)
	walk = func(cur any, rest string) {
		switch t := cur.(type) {
		case []any:
			for _, item := range t {
				walk(item, rest)
			}
		case map[string]any:
			if rest == "" {
				out = append(out, t)
				return
			}
			head, tail, _ := strings.Cut(rest, ".")
			if v, ok := t[head]; ok {
				walk(v, tail)
			}
		}
	}
	walk(doc, path)
	return out
}

// inferStruct builds the Go definition from a set of observed records.
func inferStruct(name string, recs []map[string]any) string {
	types := map[string]any{}
	nullable := map[string]bool{}
	var order []string

	// Record keys are visited in sorted order: Go randomises map iteration, so
	// without this the generated struct's field order changes run to run and
	// the output cannot be diffed or regenerated reproducibly.
	for _, r := range recs {
		for _, k := range sortedKeys(r) {
			v := r[k]
			if _, seen := types[k]; !seen {
				order = append(order, k)
				types[k] = nil
			}
			if v == nil {
				nullable[k] = true
			} else if types[k] == nil {
				types[k] = v
			}
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "type %s struct {\n", name)
	for _, k := range order {
		goName := exported(k)
		t := goType(types[k], nullable[k])
		note := ""
		if types[k] == nil {
			note = "  // always null in the capture; type unconfirmed"
		}
		tag := k
		fmt.Fprintf(&b, "\t%s %s `json:%q`%s\n", goName, t, tag, note)
	}
	b.WriteString("}")
	return b.String()
}

func goType(sample any, nullable bool) string {
	base := "string"
	switch v := sample.(type) {
	case bool:
		base = "bool"
	case float64:
		if v == float64(int64(v)) {
			base = "int"
		} else {
			base = "float64"
		}
	case string:
		base = "string"
	case []any:
		if len(v) == 0 {
			return "[]any"
		}
		return "[]" + strings.TrimPrefix(goType(v[0], false), "*")
	case map[string]any:
		return "map[string]any"
	case nil:
		base = "string"
	}
	if nullable {
		return "*" + base
	}
	return base
}

// exported turns a provider's field name into an exported Go identifier.
// Cricbuzz uses lowercase keys where other providers use PascalCase, so this has
// to cope with both without mangling the already-correct ones.
func exported(key string) string {
	if key == "" {
		return "Field"
	}
	parts := strings.FieldsFunc(key, func(r rune) bool {
		return r == '_' || r == '-' || r == '.'
	})
	var b strings.Builder
	for _, p := range parts {
		r := []rune(p)
		r[0] = unicode.ToUpper(r[0])
		b.WriteString(string(r))
	}
	out := b.String()
	// Go convention for common initialisms, so the generated names read the
	// way hand-written ones would.
	for _, init := range []string{"Id", "Url", "Api"} {
		if strings.HasSuffix(out, init) {
			out = strings.TrimSuffix(out, init) + strings.ToUpper(init)
		}
	}
	return out
}

// sortedKeys is used by callers that want a stable field order.
func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
