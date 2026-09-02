// Package cricbuzz holds Go structs mirroring the Cricbuzz API's wire format,
// as served through RapidAPI.
//
// Cricket has no API-Sports host, so it is served here. The primary provider covers every other
// sport but offers no cricket at all — /v3/cricket, /cricket/v1 and every other
// prefix return 404, not the 401 an unsubscribed-but-real product returns — so
// cricket comes from a different vendor with entirely different conventions:
//
//   - Field names are lowercase and unspaced: "batteamname", "strkrate".
//   - Numeric-looking values are often strings: overs, economy and strike rate
//     all arrive quoted, because the API formats them for display.
//   - The document is a scorecard of innings, each carrying its own batsman,
//     bowler, fall-of-wicket and partnership collections, rather than the flat
//     per-player stat arrays the other providers return.
//
// GENERATED FROM CAPTURED RESPONSES — regenerate with:
//
//	go run ./cmd/schematool infer -file cricbuzz/scorecard.json \
//	    -path scorecard.batsman -type CricbuzzBatsman
//
// Source: fixtures/cricbuzz, captured from mcenter/v1/{matchId}/hscard.
package cricbuzz

type Batsman struct {
	Balls           int    `json:"balls"`
	Fours           int    `json:"fours"`
	ID              int    `json:"id"`
	Imageid         int    `json:"imageid"`
	Inmatchchange   string `json:"inmatchchange"`
	Iscaptain       bool   `json:"iscaptain"`
	Iscbplusfree    bool   `json:"iscbplusfree"`
	Iskeeper        bool   `json:"iskeeper"`
	Isoverseas      bool   `json:"isoverseas"`
	Ispremiumfree   bool   `json:"ispremiumfree"`
	Name            string `json:"name"`
	Nickname        string `json:"nickname"`
	Outdec          string `json:"outdec"`
	Planid          int    `json:"planid"`
	Playingxichange string `json:"playingxichange"`
	Premiumvideourl string `json:"premiumvideourl"`
	Runs            int    `json:"runs"`
	Sixes           int    `json:"sixes"`
	Strkrate        string `json:"strkrate"`
	Videoid         int    `json:"videoid"`
	Videotype       string `json:"videotype"`
	Videourl        string `json:"videourl"`
}

type Bowler struct {
	Balls           int     `json:"balls"`
	Dots            int     `json:"dots"`
	Economy         string  `json:"economy"`
	ID              int     `json:"id"`
	Imageid         int     `json:"imageid"`
	Inmatchchange   string  `json:"inmatchchange"`
	Iscaptain       bool    `json:"iscaptain"`
	Iskeeper        bool    `json:"iskeeper"`
	Isoverseas      bool    `json:"isoverseas"`
	Ispremiumfree   bool    `json:"ispremiumfree"`
	Maidens         int     `json:"maidens"`
	Name            string  `json:"name"`
	Nickname        string  `json:"nickname"`
	Overs           string  `json:"overs"`
	Planid          int     `json:"planid"`
	Playingxichange string  `json:"playingxichange"`
	Premiumvideourl string  `json:"premiumvideourl"`
	Rpb             float64 `json:"rpb"`
	Runs            int     `json:"runs"`
	Videoid         int     `json:"videoid"`
	Videotype       string  `json:"videotype"`
	Videourl        string  `json:"videourl"`
	Wickets         int     `json:"wickets"`
}

type Extras struct {
	Byes    int `json:"byes"`
	Legbyes int `json:"legbyes"`
	Noballs int `json:"noballs"`
	Penalty int `json:"penalty"`
	Total   int `json:"total"`
	Wides   int `json:"wides"`
}

type FallOfWicket struct {
	Ballnbr     int     `json:"ballnbr"`
	Batsmanid   int     `json:"batsmanid"`
	Batsmanname string  `json:"batsmanname"`
	Overnbr     float64 `json:"overnbr"`
	Runs        int     `json:"runs"`
}

type Partnership struct {
	Bat1balls      int    `json:"bat1balls"`
	Bat1boundaries int    `json:"bat1boundaries"`
	Bat1fives      int    `json:"bat1fives"`
	Bat1fours      int    `json:"bat1fours"`
	Bat1id         int    `json:"bat1id"`
	Bat1name       string `json:"bat1name"`
	Bat1ones       int    `json:"bat1ones"`
	Bat1runs       int    `json:"bat1runs"`
	Bat1sixers     int    `json:"bat1sixers"`
	Bat1sixes      int    `json:"bat1sixes"`
	Bat1threes     int    `json:"bat1threes"`
	Bat1twos       int    `json:"bat1twos"`
	Bat2balls      int    `json:"bat2balls"`
	Bat2boundaries int    `json:"bat2boundaries"`
	Bat2fives      int    `json:"bat2fives"`
	Bat2fours      int    `json:"bat2fours"`
	Bat2id         int    `json:"bat2id"`
	Bat2name       string `json:"bat2name"`
	Bat2ones       int    `json:"bat2ones"`
	Bat2runs       int    `json:"bat2runs"`
	Bat2sixers     int    `json:"bat2sixers"`
	Bat2sixes      int    `json:"bat2sixes"`
	Bat2threes     int    `json:"bat2threes"`
	Bat2twos       int    `json:"bat2twos"`
	ID             int    `json:"id"`
	Teamid         int    `json:"teamid"`
	Teamname       string `json:"teamname"`
	Totalballs     int    `json:"totalballs"`
	Totalruns      int    `json:"totalruns"`
}

// Fow and PartnershipList are the single-field wrappers the API puts around
// its fall-of-wicket and partnership collections.
type Fow struct {
	Fow []FallOfWicket `json:"fow"`
}

type PartnershipList struct {
	Partnership []Partnership `json:"partnership"`
}

// Innings is one innings of the scorecard.
type Innings struct {
	Inningsid    int             `json:"inningsid"`
	Batsman      []Batsman       `json:"batsman"`
	Bowler       []Bowler        `json:"bowler"`
	Fow          Fow             `json:"fow"`
	Extras       Extras          `json:"extras"`
	Score        int             `json:"score"`
	Wickets      int             `json:"wickets"`
	Overs        float64         `json:"overs"`
	Runrate      float64         `json:"runrate"`
	Batteamname  string          `json:"batteamname"`
	Batteamsname string          `json:"batteamsname"`
	Isdeclared   bool            `json:"isdeclared"`
	Isfollowon   bool            `json:"isfollowon"`
	Ballnbr      int             `json:"ballnbr"`
	Rpb          float64         `json:"rpb"`
	Partnership  PartnershipList `json:"partnership"`
}

// Scorecard is the /mcenter/v1/{matchId}/hscard response. It is the closest
// thing this API has to a box score.
type Scorecard struct {
	Scorecard           []Innings `json:"scorecard"`
	Ismatchcomplete     bool      `json:"ismatchcomplete"`
	Status              string    `json:"status"`
	Responselastupdated int       `json:"responselastupdated"`
	Appindex            *AppIndex `json:"appindex"`
}

// AppIndex is presentation metadata the API attaches to most documents. It is
// carried so the payload matches the wire, not because the pipeline uses it.
type AppIndex struct {
	Seotitle string `json:"seotitle"`
	Weburl   string `json:"weburl"`
}
