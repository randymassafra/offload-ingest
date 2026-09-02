package sdio

// NASCAR models.
//
// GENERATED FROM CAPTURED PROVIDER RESPONSES — DO NOT HAND-EDIT FIELD LISTS.
//
// NASCAR replaces Formula 1 in the sport line-up. SportsDataIO markets F1 as a
// product but no route for it is reachable: /f1/v1, /f1/v2, /v3/f1,
// /formula1/*, /motorsports/* and /api/f1/* all return 404, not the 401 that a
// merely unsubscribed product returns. NASCAR is served at /nascar/v2/json and
// responds on the same key, so the motorsport slot is filled by a feed whose
// schema can actually be verified.
//
// The shapes below come from live captures under fixtures/sportsdataio/nascar.
// Note the provider's own quirks, reproduced rather than corrected:
//
//   - DriverRace reports Laps, LapsLed, FinalPosition and the rest as DECIMALS,
//     not integers, because the same model carries season averages.
//   - RaceResult is a two-field envelope: {Race, DriverRaces}.
//   - Day and DateTime are separate fields, both full timestamps.

type NASCARSeries struct {
	SeriesID int    `json:"SeriesID"`
	Name     string `json:"Name"`
}

type NASCARDriver struct {
	DriverID      int     `json:"DriverID"`
	FirstName     string  `json:"FirstName"`
	LastName      string  `json:"LastName"`
	Number        *int    `json:"Number"`
	NumberDisplay *string `json:"NumberDisplay"`
	Team          *string `json:"Team"`
	BirthDate     *string `json:"BirthDate"`
	BirthPlace    *string `json:"BirthPlace"`
	Gender        *string `json:"Gender"` // always null in the capture; type unconfirmed
	Height        *int    `json:"Height"`
	Weight        *int    `json:"Weight"`
	Manufacturer  *string `json:"Manufacturer"`
	Engine        *string `json:"Engine"`
	Chassis       *string `json:"Chassis"`
	Sponsors      *string `json:"Sponsors"`
	CrewChief     *string `json:"CrewChief"`
	PhotoUrl      string  `json:"PhotoUrl"`
	Updated       string  `json:"Updated"`
	Created       *string `json:"Created"`
}

type NASCARRace struct {
	RaceID              int     `json:"RaceID"`
	SeriesID            int     `json:"SeriesID"`
	SeriesName          string  `json:"SeriesName"`
	Season              int     `json:"Season"`
	Name                string  `json:"Name"`
	Day                 string  `json:"Day"`
	DateTime            string  `json:"DateTime"`
	Track               string  `json:"Track"`
	Broadcast           string  `json:"Broadcast"`
	WinnerID            int     `json:"WinnerID"`
	PoleWinnerID        int     `json:"PoleWinnerID"`
	IsInProgress        bool    `json:"IsInProgress"`
	IsOver              bool    `json:"IsOver"`
	Updated             string  `json:"Updated"`
	Created             string  `json:"Created"`
	RescheduledDay      *string `json:"RescheduledDay"`
	RescheduledDateTime *string `json:"RescheduledDateTime"`
	Canceled            bool    `json:"Canceled"`
	ScheduledLaps       int     `json:"ScheduledLaps"`
	ActualLaps          int     `json:"ActualLaps"`
}

type NASCARDriverRace struct {
	StatID                  int      `json:"StatID"`
	DriverID                int      `json:"DriverID"`
	Season                  int      `json:"Season"`
	Name                    string   `json:"Name"`
	Number                  *int     `json:"Number"`
	NumberDisplay           *string  `json:"NumberDisplay"`
	Manufacturer            string   `json:"Manufacturer"`
	DraftKingsSalary        int      `json:"DraftKingsSalary"`
	RaceID                  int      `json:"RaceID"`
	Day                     string   `json:"Day"`
	DateTime                string   `json:"DateTime"`
	Updated                 string   `json:"Updated"`
	Created                 string   `json:"Created"`
	FantasyPoints           float64  `json:"FantasyPoints"`
	FantasyPointsDraftKings float64  `json:"FantasyPointsDraftKings"`
	QualifyingSpeed         *float64 `json:"QualifyingSpeed"`
	PoleFinalPosition       float64  `json:"PoleFinalPosition"`
	StartPosition           float64  `json:"StartPosition"`
	FinalPosition           float64  `json:"FinalPosition"`
	PositionDifferential    float64  `json:"PositionDifferential"`
	Laps                    float64  `json:"Laps"`
	LapsLed                 float64  `json:"LapsLed"`
	FastestLaps             float64  `json:"FastestLaps"`
	Points                  float64  `json:"Points"`
	Bonus                   float64  `json:"Bonus"`
	Penalty                 float64  `json:"Penalty"`
	Wins                    float64  `json:"Wins"`
	Poles                   float64  `json:"Poles"`
	CurrentPosition         float64  `json:"CurrentPosition"`
}

// NASCARRaceResult is the /nascar/v2/json/RaceResult/{raceid} response: the
// race document plus one row per driver.
type NASCARRaceResult struct {
	Race        NASCARRace         `json:"Race"`
	DriverRaces []NASCARDriverRace `json:"DriverRaces"`
}
