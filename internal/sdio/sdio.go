// Package sdio holds Go structs mirroring SportsDataIO's wire format. These are
// transport models, not simulation state: field names, casing and JSON shape are
// copied from SportsDataIO so that anything downstream — Kafka consumers, Flink
// jobs, schema registries — sees the same bytes it will see in production.
//
// # Provenance
//
// Each model file records where its schema came from. Three tiers:
//
//   - VERIFIED (OpenAPI): field lists extracted from SportsDataIO's published
//     OpenAPI specifications. NFL, NBA and Soccer.
//   - VERIFIED (data dictionary): field lists taken from the public data
//     dictionary pages. NCAAF, NCAAB, Golf, MMA/UFC, F1.
//   - MODELED: SportsDataIO does not publish a schema we could reach. The model
//     follows their house conventions but the field names are NOT authoritative.
//     Tennis, Cricket, AFL and Rugby. See each file's header.
//
// # Conventions
//
// SportsDataIO is consistent about a few things, and the models reproduce them
// rather than "improving" them:
//
//   - Timestamps are US Eastern, formatted without a zone offset
//     ("2026-08-30T13:05:00"). UTC variants carry a Utc/UTC suffix.
//   - Nullable scalars are pointers so that a real JSON null round-trips; a
//     zero and an absent value are different things to a downstream consumer.
//   - Casing is per-API, not global. NFL and NBA use "GameID"; Soccer v4 uses
//     "GameId"; MMA uses "FightId". These inconsistencies are deliberate.
package sdio

import (
	"strings"
	"time"
)

// Layouts used across the SportsDataIO APIs.
const (
	// DateTimeLayout is the US Eastern timestamp format with no zone offset.
	DateTimeLayout = "2006-01-02T15:04:05"
	// DateLayout is the day-granularity format used by Day, StartDay and friends.
	DateLayout = "2006-01-02"
	// DateTimeUTCLayout is used by the *Utc / *UTC fields.
	DateTimeUTCLayout = "2006-01-02T15:04:05Z"
)

// Eastern is the wire timezone for every non-UTC SportsDataIO timestamp.
var Eastern = mustLoadEastern()

func mustLoadEastern() *time.Location {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		// Alpine and distroless images may ship without tzdata; a fixed -05:00
		// keeps the wire format plausible rather than failing the load test.
		return time.FixedZone("EST", -5*60*60)
	}
	return loc
}

// DateTime marshals as a bare US Eastern timestamp, matching the API.
type DateTime time.Time

func (d DateTime) MarshalJSON() ([]byte, error) {
	return []byte(`"` + time.Time(d).In(Eastern).Format(DateTimeLayout) + `"`), nil
}

func (d *DateTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		return nil
	}
	t, err := time.ParseInLocation(DateTimeLayout, s, Eastern)
	if err != nil {
		return err
	}
	*d = DateTime(t)
	return nil
}

func (d DateTime) Time() time.Time { return time.Time(d) }

// Date marshals as a bare day, matching Day/StartDay/EndDay fields.
type Date time.Time

func (d Date) MarshalJSON() ([]byte, error) {
	return []byte(`"` + time.Time(d).In(Eastern).Format(DateLayout) + `"`), nil
}

func (d *Date) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		return nil
	}
	t, err := time.ParseInLocation(DateLayout, s, Eastern)
	if err != nil {
		return err
	}
	*d = Date(t)
	return nil
}

func (d Date) Time() time.Time { return time.Time(d) }

// DateTimeUTC marshals with a trailing Z, matching the *Utc / *UTC fields.
type DateTimeUTC time.Time

func (d DateTimeUTC) MarshalJSON() ([]byte, error) {
	return []byte(`"` + time.Time(d).UTC().Format(DateTimeUTCLayout) + `"`), nil
}

func (d *DateTimeUTC) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		return nil
	}
	t, err := time.Parse(DateTimeUTCLayout, s)
	if err != nil {
		return err
	}
	*d = DateTimeUTC(t)
	return nil
}

func (d DateTimeUTC) Time() time.Time { return time.Time(d) }

// SeasonType values, shared across the SportsDataIO APIs.
const (
	SeasonTypeRegular    = 1
	SeasonTypePreseason  = 2
	SeasonTypePostseason = 3
	SeasonTypeOffseason  = 4
	SeasonTypeAllStar    = 5
)

// Status values seen on game and event models.
const (
	StatusScheduled  = "Scheduled"
	StatusInProgress = "InProgress"
	StatusFinal      = "Final"
	StatusF_OT       = "F/OT"
	StatusSuspended  = "Suspended"
	StatusPostponed  = "Postponed"
	StatusCanceled   = "Canceled"
)

// Ptr returns a pointer to v, for the nullable scalar fields.
func Ptr[T any](v T) *T { return &v }
