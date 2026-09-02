package sdio

// Cricket models.
//
// ┌─────────────────────────────────────────────────────────────────────────┐
// │ PROVENANCE: MODELED — SportsDataIO DOES NOT OFFER A CRICKET API.         │
// │                                                                          │
// │ Cricket is not in SportsDataIO's product line. There is no schema to     │
// │ conform to. The models below follow SportsDataIO's house conventions so  │
// │ the payloads sit consistently alongside the real feeds in Kafka, but     │
// │ they describe no real endpoint.                                          │
// │                                                                          │
// │ If cricket ingest is genuinely on the roadmap, pick the provider first   │
// │ (Sportradar, SportMonks and Entity Sport all cover it) and regenerate    │
// │ this file from that provider's schema. Load testing against an invented  │
// │ shape only proves the pipeline can move bytes.                           │
// └─────────────────────────────────────────────────────────────────────────┘

// CricketDelivery is the ball-by-ball telemetry record.
type CricketDelivery struct {
	DeliveryId        int     `json:"DeliveryId"`
	MatchId           int     `json:"MatchId"`
	InningsId         int     `json:"InningsId"`
	Sequence          int     `json:"Sequence"`
	Over              int     `json:"Over"`
	Ball              int     `json:"Ball"`
	BattingTeamId     int     `json:"BattingTeamId"`
	BowlingTeamId     int     `json:"BowlingTeamId"`
	StrikerId         int     `json:"StrikerId"`
	NonStrikerId      int     `json:"NonStrikerId"`
	BowlerId          int     `json:"BowlerId"`
	RunsScored        int     `json:"RunsScored"`
	ExtraRuns         int     `json:"ExtraRuns"`
	ExtraType         *string `json:"ExtraType"`
	IsWicket          bool    `json:"IsWicket"`
	DismissalType     *string `json:"DismissalType"`
	DismissedPlayerId *int    `json:"DismissedPlayerId"`
	IsBoundary        bool    `json:"IsBoundary"`
	Description       *string `json:"Description"`
	Updated           *string `json:"Updated"`
}

// CricketInnings is one innings of a match.
type CricketInnings struct {
	InningsId           int     `json:"InningsId"`
	MatchId             int     `json:"MatchId"`
	Number              int     `json:"Number"`
	BattingTeamId       int     `json:"BattingTeamId"`
	BowlingTeamId       int     `json:"BowlingTeamId"`
	Runs                int     `json:"Runs"`
	Wickets             int     `json:"Wickets"`
	Overs               float64 `json:"Overs"`
	Extras              int     `json:"Extras"`
	RunRate             float64 `json:"RunRate"`
	IsComplete          bool    `json:"IsComplete"`
	DeclarationDeclared bool    `json:"DeclarationDeclared"`
}

// CricketPlayerMatch is one player's match stat line.
type CricketPlayerMatch struct {
	StatId   int     `json:"StatId"`
	MatchId  int     `json:"MatchId"`
	PlayerId int     `json:"PlayerId"`
	TeamId   int     `json:"TeamId"`
	Name     *string `json:"Name"`
	Role     *string `json:"Role"`

	RunsScored    float64 `json:"RunsScored"`
	BallsFaced    float64 `json:"BallsFaced"`
	Fours         float64 `json:"Fours"`
	Sixes         float64 `json:"Sixes"`
	StrikeRate    float64 `json:"StrikeRate"`
	IsOut         bool    `json:"IsOut"`
	DismissalType *string `json:"DismissalType"`

	OversBowled  float64 `json:"OversBowled"`
	RunsConceded float64 `json:"RunsConceded"`
	WicketsTaken float64 `json:"WicketsTaken"`
	Maidens      float64 `json:"Maidens"`
	EconomyRate  float64 `json:"EconomyRate"`

	Catches       float64 `json:"Catches"`
	Stumpings     float64 `json:"Stumpings"`
	RunOuts       float64 `json:"RunOuts"`
	FantasyPoints float64 `json:"FantasyPoints"`
	Updated       *string `json:"Updated"`
}

// CricketMatch is the match-level model.
type CricketMatch struct {
	MatchId  int     `json:"MatchId"`
	SeriesId int     `json:"SeriesId"`
	Season   int     `json:"Season"`
	Format   *string `json:"Format"`
	Status   *string `json:"Status"`
	Day      *string `json:"Day"`
	DateTime *string `json:"DateTime"`
	Venue    *string `json:"Venue"`

	HomeTeamId   int     `json:"HomeTeamId"`
	HomeTeamKey  *string `json:"HomeTeamKey"`
	HomeTeamName *string `json:"HomeTeamName"`
	AwayTeamId   int     `json:"AwayTeamId"`
	AwayTeamKey  *string `json:"AwayTeamKey"`
	AwayTeamName *string `json:"AwayTeamName"`

	TossWinnerTeamId *int    `json:"TossWinnerTeamId"`
	TossDecision     *string `json:"TossDecision"`

	CurrentInnings    *int    `json:"CurrentInnings"`
	CurrentOver       *int    `json:"CurrentOver"`
	ScoreDisplay      *string `json:"ScoreDisplay"`
	WinnerTeamId      *int    `json:"WinnerTeamId"`
	ResultDescription *string `json:"ResultDescription"`

	Updated       *string `json:"Updated"`
	IsClosed      bool    `json:"IsClosed"`
	GlobalMatchId int     `json:"GlobalMatchId"`
}

// CricketBoxScore is the match snapshot.
type CricketBoxScore struct {
	Match         CricketMatch         `json:"Match"`
	Innings       []CricketInnings     `json:"Innings"`
	PlayerMatches []CricketPlayerMatch `json:"PlayerMatches"`
}
