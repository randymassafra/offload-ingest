// Package golf is the client for live-golf-data, the golf feed's upstream.
//
// Golf is one of four sports API-Sports has no host for, so it needs its own
// provider. This one is reached through RapidAPI and covers the PGA and LIV
// tours.
//
// # The wire format is MongoDB extended JSON
//
// This is the quirk that shapes the whole package. Integers do not arrive as
// integers:
//
//	"currentHole": {"$numberInt": "18"}
//	"strokes":     {"$numberInt": "72"}
//
// The upstream is evidently serving documents straight out of MongoDB without
// collapsing them to plain JSON. Unmarshalling those into an int field fails
// outright, so every numeric field here is a MongoInt, which accepts the
// wrapped form, a bare number and a quoted number.
//
// Scores are a separate trap: "total", "scoreToPar" and "currentRoundScore" are
// strings carrying a sign — "-9", "+2", "E" for even. They are kept as strings
// because that is what the provider sends and because "E" has no integer
// spelling; ParScore is provided for callers that need a number.
package golf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// MongoInt is an integer that may arrive wrapped in MongoDB's extended JSON.
//
// Three forms are accepted — {"$numberInt":"18"}, 18, and "18" — because the
// upstream has been observed sending the first and there is no guarantee it
// will not normalise some fields and not others. Being liberal here costs
// nothing; being strict would mean a single unwrapped field breaks a whole
// leaderboard.
type MongoInt int

// UnmarshalJSON implements json.Unmarshaler.
func (m *MongoInt) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || string(b) == "null" {
		*m = 0
		return nil
	}

	// The wrapped form.
	if b[0] == '{' {
		var wrapper struct {
			NumberInt    string `json:"$numberInt"`
			NumberLong   string `json:"$numberLong"`
			NumberDouble string `json:"$numberDouble"`
		}
		if err := json.Unmarshal(b, &wrapper); err != nil {
			return fmt.Errorf("golf: decoding extended-JSON number %s: %w", b, err)
		}
		for _, raw := range []string{wrapper.NumberInt, wrapper.NumberLong, wrapper.NumberDouble} {
			if raw == "" {
				continue
			}
			f, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				return fmt.Errorf("golf: %q is not a number: %w", raw, err)
			}
			*m = MongoInt(int(f))
			return nil
		}
		// An empty wrapper is not an error; the provider omits values this way.
		*m = 0
		return nil
	}

	// A quoted number.
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		if strings.TrimSpace(s) == "" {
			*m = 0
			return nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return fmt.Errorf("golf: %q is not a number: %w", s, err)
		}
		*m = MongoInt(int(f))
		return nil
	}

	// A bare number.
	var f float64
	if err := json.Unmarshal(b, &f); err != nil {
		return fmt.Errorf("golf: decoding number %s: %w", b, err)
	}
	*m = MongoInt(int(f))
	return nil
}

// MarshalJSON emits a plain number.
//
// Deliberately NOT the extended form. The pipeline's contract is that Kafka
// carries the provider's document, but an extended-JSON integer is a MongoDB
// serialisation artifact rather than meaningful provider data, and forcing
// every downstream consumer to unwrap it would propagate the upstream's
// accident into our schema.
func (m MongoInt) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Itoa(int(m))), nil
}

// Int returns the value.
func (m MongoInt) Int() int { return int(m) }

// MongoDate is a timestamp in MongoDB's extended JSON, as the schedule serves
// them:
//
//	"start": {"$date": {"$numberLong": "1735776000000"}}
//
// The inner value is MILLISECONDS since the epoch, not seconds. Reading it as
// seconds silently yields a date in 56000 AD, which sorts and compares without
// erroring — exactly the kind of wrong that a window comparison would accept
// and then never match anything.
//
// The plain forms are accepted too: {"$date": "2025-01-02T00:00:00Z"}, a bare
// RFC 3339 string, and a bare number. Being liberal costs nothing here, and a
// provider that normalises one field and not another has already been observed
// on this API.
type MongoDate struct{ time.Time }

// UnmarshalJSON implements json.Unmarshaler.
func (d *MongoDate) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || string(b) == "null" {
		d.Time = time.Time{}
		return nil
	}

	if b[0] == '{' {
		// Two nestings are in play. The outer one is {"$date": ...}; the inner
		// value is itself sometimes {"$numberLong": "..."} rather than a bare
		// number, so both keys are unwrapped here instead of recursing on
		// $date alone — recursion on the inner object would look for a $date
		// key that is not there and silently yield the zero time.
		var wrapper struct {
			Date       json.RawMessage `json:"$date"`
			NumberLong string          `json:"$numberLong"`
			NumberInt  string          `json:"$numberInt"`
		}
		if err := json.Unmarshal(b, &wrapper); err != nil {
			return fmt.Errorf("golf: decoding extended-JSON date %s: %w", b, err)
		}
		for _, raw := range []string{wrapper.NumberLong, wrapper.NumberInt} {
			if raw == "" {
				continue
			}
			ms, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				return fmt.Errorf("golf: %q is not an epoch: %w", raw, err)
			}
			d.Time = time.UnixMilli(ms).UTC()
			return nil
		}
		if len(wrapper.Date) == 0 {
			d.Time = time.Time{}
			return nil
		}
		return d.UnmarshalJSON(wrapper.Date)
	}

	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		if strings.TrimSpace(s) == "" {
			d.Time = time.Time{}
			return nil
		}
		// A quoted epoch, which is how $numberLong arrives.
		if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
			d.Time = time.UnixMilli(ms).UTC()
			return nil
		}
		// The same endpoint has been observed returning both the extended-JSON
		// epoch and a bare, zone-less timestamp — apparently depending on how
		// the request is made. Both are accepted rather than assuming one.
		for _, layout := range []string{
			time.RFC3339,
			"2006-01-02T15:04:05Z0700",
			"2006-01-02T15:04:05", // zone-less; treated as UTC
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, s); err == nil {
				d.Time = t.UTC()
				return nil
			}
		}
		return fmt.Errorf("golf: %q is not a recognisable date", s)
	}

	var ms int64
	if err := json.Unmarshal(b, &ms); err != nil {
		return fmt.Errorf("golf: decoding date %s: %w", b, err)
	}
	d.Time = time.UnixMilli(ms).UTC()
	return nil
}

// MarshalJSON emits RFC 3339, so a consumer of our envelope is not handed the
// upstream's serialisation artifact.
func (d MongoDate) MarshalJSON() ([]byte, error) {
	if d.Time.IsZero() {
		return []byte("null"), nil
	}
	return json.Marshal(d.Time.UTC().Format(time.RFC3339))
}

// IsZero reports whether the date was absent.
func (d MongoDate) IsZero() bool { return d.Time.IsZero() }

// Leaderboard is the /leaderboard response.
type Leaderboard struct {
	OrgID       string    `json:"orgId"`
	Year        string    `json:"year"`
	TournID     string    `json:"tournId"`
	Status      string    `json:"status"`
	RoundID     MongoInt  `json:"roundId"`
	RoundStatus string    `json:"roundStatus"`
	LastUpdated string    `json:"lastUpdated"`
	Timestamp   string    `json:"timestamp"`
	CutLines    []CutLine `json:"cutLines"`
	Rows        []Row     `json:"leaderboardRows"`
}

// CutLine is the score at which the field is cut.
type CutLine struct {
	CutCount     MongoInt `json:"cutCount"`
	CutScore     string   `json:"cutScore"`
	CutPaidCount MongoInt `json:"cutPaidCount"`
}

// Row is one player's position on the leaderboard.
type Row struct {
	PlayerID  string `json:"playerId"`
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	IsAmateur bool   `json:"isAmateur"`
	CourseID  string `json:"courseId"`
	Status    string `json:"status"`
	Position  string `json:"position"`
	// Total, CurrentRoundScore and the round scores are signed strings —
	// "-9", "+2", "E". See the package comment.
	Total                           string   `json:"total"`
	CurrentRoundScore               string   `json:"currentRoundScore"`
	TotalStrokesFromCompletedRounds string   `json:"totalStrokesFromCompletedRounds"`
	CurrentHole                     MongoInt `json:"currentHole"`
	StartingHole                    MongoInt `json:"startingHole"`
	RoundComplete                   bool     `json:"roundComplete"`
	Rounds                          []Round  `json:"rounds"`
	Teetime                         string   `json:"teeTime,omitempty"`
}

// Round is one round's result for a player.
type Round struct {
	RoundID    MongoInt `json:"roundId"`
	Strokes    MongoInt `json:"strokes"`
	ScoreToPar string   `json:"scoreToPar"`
	CourseID   string   `json:"courseId"`
	CourseName string   `json:"courseName"`
}

// Name is the player's display name.
func (r Row) Name() string {
	return strings.TrimSpace(r.FirstName + " " + r.LastName)
}

// ParScore converts a signed score string to an integer relative to par.
//
// "E" is even, which has no sign and would otherwise parse as an error. The ok
// return distinguishes a genuine zero from an unparseable value — a caller
// summing scores must not silently treat "WD" as level par.
func ParScore(s string) (int, bool) {
	s = strings.TrimSpace(s)
	switch {
	case s == "":
		return 0, false
	case strings.EqualFold(s, "E"):
		return 0, true
	}
	n, err := strconv.Atoi(strings.TrimPrefix(s, "+"))
	if err != nil {
		return 0, false
	}
	return n, true
}
