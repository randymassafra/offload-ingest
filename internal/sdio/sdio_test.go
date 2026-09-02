package sdio

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestDateTimeWireFormat pins SportsDataIO's zone-less US Eastern timestamp.
// A trailing Z or an offset here would break any consumer parsing with the
// documented format.
func TestDateTimeWireFormat(t *testing.T) {
	utc := time.Date(2026, 8, 30, 17, 5, 0, 0, time.UTC)

	b, err := json.Marshal(DateTime(utc))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := strings.Trim(string(b), `"`)
	if strings.HasSuffix(got, "Z") || strings.Contains(got, "+") {
		t.Errorf("DateTime = %q, want no zone suffix", got)
	}
	if len(got) != len("2006-01-02T15:04:05") {
		t.Errorf("DateTime = %q, want the %q layout", got, DateTimeLayout)
	}

	var back DateTime
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !back.Time().Equal(utc) {
		t.Errorf("round trip gave %v, want %v", back.Time(), utc)
	}
}

func TestDateAndUTCWireFormats(t *testing.T) {
	when := time.Date(2026, 8, 30, 17, 5, 0, 0, time.UTC)

	b, _ := json.Marshal(Date(when))
	if got := strings.Trim(string(b), `"`); len(got) != len("2006-01-02") {
		t.Errorf("Date = %q, want the %q layout", got, DateLayout)
	}

	b, _ = json.Marshal(DateTimeUTC(when))
	if got := strings.Trim(string(b), `"`); !strings.HasSuffix(got, "Z") {
		t.Errorf("DateTimeUTC = %q, want a trailing Z", got)
	}
}

func TestNullTimestampsDoNotError(t *testing.T) {
	var d DateTime
	if err := json.Unmarshal([]byte(`null`), &d); err != nil {
		t.Errorf("DateTime null: %v", err)
	}
	var day Date
	if err := json.Unmarshal([]byte(`""`), &day); err != nil {
		t.Errorf("Date empty: %v", err)
	}
}

// TestNullableScalarsAreOmitted checks that a nil pointer marshals as JSON
// null rather than a zero. A consumer must be able to tell "no score yet" from
// "a score of zero", and that distinction only survives if the field is a
// pointer without omitempty.
func TestEasternZoneResolves(t *testing.T) {
	if Eastern == nil {
		t.Fatal("Eastern is nil")
	}
	// Either the real zone or the documented fallback is acceptable; a UTC
	// zone would silently shift every timestamp by five hours.
	_, offset := time.Date(2026, 1, 15, 12, 0, 0, 0, Eastern).Zone()
	if offset != -5*3600 {
		t.Errorf("Eastern winter offset = %ds, want -18000", offset)
	}
}

// jsonFields returns the JSON field names a struct marshals to.
func jsonFields(t *testing.T, v any) map[string]bool {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %T: %v", v, err)
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("unmarshal %T: %v", v, err)
	}
	out := make(map[string]bool, len(doc))
	for k := range doc {
		out[k] = true
	}
	return out
}

// TestBoxScoreRoots pins what each box score wraps its match in. The root
// differs per API — NFL nests a Score, NBA and the college APIs nest a Game —
// and a consumer unwrapping the wrong key gets nothing.

// TestNullableScalarsRoundTripAsNull checks that a nil pointer marshals as JSON
// null rather than a zero. A consumer must be able to tell "no score yet" from
// "a score of zero", and that distinction only survives if the field is a
// pointer without omitempty.
//
// Golf and NASCAR are what is left on this provider — everything else moved to
// API-Sports — so they are what this invariant is pinned against now.
func TestNullableScalarsRoundTripAsNull(t *testing.T) {
	var round GolfPlayerRound // zero value: every nullable field nil
	b, err := json.Marshal(round)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var checked int
	for k, v := range doc {
		if v != nil {
			continue
		}
		checked++
		_ = k
	}
	if checked == 0 {
		t.Error("no field marshalled as null; nullable scalars must be pointers")
	}
}

// TestNoOmitemptyOnNullableScalars catches a refactor that adds omitempty to a
// nullable field. SportsDataIO always sends the key; dropping it changes the
// document shape a schema registry has already accepted.
func TestNoOmitemptyOnNullableScalars(t *testing.T) {
	for _, v := range []any{
		GolfPlayerRound{}, GolfPlayerHole{}, GolfPlayerTournament{},
		NASCARDriverRace{}, NASCARRace{}, NASCARDriver{},
	} {
		rt := reflect.TypeOf(v)
		for i := 0; i < rt.NumField(); i++ {
			fld := rt.Field(i)
			if fld.Type.Kind() != reflect.Ptr {
				continue
			}
			if strings.Contains(fld.Tag.Get("json"), ",omitempty") {
				t.Errorf("%s.%s is a nullable scalar with omitempty; the provider always sends the key",
					rt.Name(), fld.Name)
			}
		}
	}
}
