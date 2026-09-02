package sdio

// Golf models.
//
// Provenance: VERIFIED against SportsDataIO's public Golf data dictionary.
//
// Endpoints these satisfy:
//
//	GET /golf/v2/json/Tournaments                  -> []GolfTournament
//	GET /golf/v2/json/Leaderboard/{tournamentid}   -> GolfLeaderboard
//	GET /golf/v2/json/Player/{playerid}            -> GolfPlayerTournament
//
// Golf is the clearest example of why the feed kinds are split. The
// Leaderboard is a whole-field snapshot that is expensive and slow-moving;
// the hole-by-hole PlayerHole records underneath it are the high-frequency
// stream. A Flink job windowing on scoring changes wants the latter.
//
// Note the wire model's use of decimal for counting stats (Birdies, Pars,
// Eagles are all decimal, not integer). That is SportsDataIO's choice and is
// reproduced here rather than corrected.

// GolfRound is a tournament round.
type GolfRound struct {
	TournamentID int     `json:"TournamentID"`
	RoundID      int     `json:"RoundID"`
	Number       int     `json:"Number"`
	Day          *string `json:"Day"`
	IsRoundOver  bool    `json:"IsRoundOver"`
}

// GolfTournament is the event-level model.
type GolfTournament struct {
	TournamentID           int         `json:"TournamentID"`
	Name                   *string     `json:"Name"`
	StartDate              *string     `json:"StartDate"`
	EndDate                *string     `json:"EndDate"`
	IsOver                 bool        `json:"IsOver"`
	IsInProgress           bool        `json:"IsInProgress"`
	Venue                  *string     `json:"Venue"`
	Location               *string     `json:"Location"`
	Par                    *int        `json:"Par"`
	Yards                  *int        `json:"Yards"`
	Purse                  *float64    `json:"Purse"`
	StartDateTime          *string     `json:"StartDateTime"`
	Canceled               bool        `json:"Canceled"`
	Covered                bool        `json:"Covered"`
	City                   *string     `json:"City"`
	State                  *string     `json:"State"`
	ZipCode                *string     `json:"ZipCode"`
	Country                *string     `json:"Country"`
	TimeZone               *string     `json:"TimeZone"`
	Format                 *string     `json:"Format"`
	Rounds                 []GolfRound `json:"Rounds"`
	SportRadarTournamentID *string     `json:"SportRadarTournamentID"`
	OddsCoverage           *string     `json:"OddsCoverage"`
}

// GolfPlayerHole is the hole-by-hole telemetry record.
type GolfPlayerHole struct {
	PlayerRoundID        int  `json:"PlayerRoundID"`
	Number               int  `json:"Number"`
	Par                  int  `json:"Par"`
	Score                *int `json:"Score"`
	ToPar                *int `json:"ToPar"`
	HoleInOne            bool `json:"HoleInOne"`
	DoubleEagle          bool `json:"DoubleEagle"`
	Eagle                bool `json:"Eagle"`
	Birdie               bool `json:"Birdie"`
	IsPar                bool `json:"IsPar"`
	Bogey                bool `json:"Bogey"`
	DoubleBogey          bool `json:"DoubleBogey"`
	WorseThanDoubleBogey bool `json:"WorseThanDoubleBogey"`
}

// GolfPlayerRound is one player's round, carrying its holes.
type GolfPlayerRound struct {
	PlayerRoundID                        int              `json:"PlayerRoundID"`
	PlayerTournamentID                   int              `json:"PlayerTournamentID"`
	Number                               int              `json:"Number"`
	Day                                  *string          `json:"Day"`
	Par                                  *int             `json:"Par"`
	Score                                *int             `json:"Score"`
	BogeyFree                            bool             `json:"BogeyFree"`
	IncludesStreakOfThreeBirdiesOrBetter bool             `json:"IncludesStreakOfThreeBirdiesOrBetter"`
	DoubleEagles                         int              `json:"DoubleEagles"`
	Eagles                               int              `json:"Eagles"`
	Birdies                              int              `json:"Birdies"`
	Pars                                 int              `json:"Pars"`
	Bogeys                               int              `json:"Bogeys"`
	DoubleBogeys                         int              `json:"DoubleBogeys"`
	WorseThanDoubleBogey                 int              `json:"WorseThanDoubleBogey"`
	HoleInOnes                           int              `json:"HoleInOnes"`
	TripleBogeys                         int              `json:"TripleBogeys"`
	WorseThanTripleBogey                 int              `json:"WorseThanTripleBogey"`
	Holes                                []GolfPlayerHole `json:"Holes"`
	LongestBirdieOrBetterStreak          float64          `json:"LongestBirdieOrBetterStreak"`
	ConsecutiveBirdieOrBetterCount       float64          `json:"ConsecutiveBirdieOrBetterCount"`
	BounceBackCount                      float64          `json:"BounceBackCount"`
	IncludesStreakOfFourBirdiesOrBetter  bool             `json:"IncludesStreakOfFourBirdiesOrBetter"`
	IncludesStreakOfFiveBirdiesOrBetter  bool             `json:"IncludesStreakOfFiveBirdiesOrBetter"`
	IncludesFiveOrMoreBirdiesOrBetter    bool             `json:"IncludesFiveOrMoreBirdiesOrBetter"`
	IncludesStreakOfSixBirdiesOrBetter   bool             `json:"IncludesStreakOfSixBirdiesOrBetter"`
	TeeTime                              *string          `json:"TeeTime"`
	BackNineStart                        bool             `json:"BackNineStart"`
}

// GolfPlayerTournament is one player's tournament line, carrying their rounds.
type GolfPlayerTournament struct {
	PlayerTournamentID int      `json:"PlayerTournamentID"`
	PlayerID           int      `json:"PlayerID"`
	TournamentID       int      `json:"TournamentID"`
	Name               *string  `json:"Name"`
	Rank               *int     `json:"Rank"`
	Country            *string  `json:"Country"`
	TotalScore         *float64 `json:"TotalScore"`
	TotalStrokes       *float64 `json:"TotalStrokes"`
	TotalThrough       *int     `json:"TotalThrough"`
	Earnings           *float64 `json:"Earnings"`
	FedExPoints        *int     `json:"FedExPoints"`

	FantasyPoints             float64 `json:"FantasyPoints"`
	FantasyPointsDraftKings   float64 `json:"FantasyPointsDraftKings"`
	FantasyPointsFanDuel      float64 `json:"FantasyPointsFanDuel"`
	FantasyPointsYahoo        float64 `json:"FantasyPointsYahoo"`
	FantasyPointsFantasyDraft float64 `json:"FantasyPointsFantasyDraft"`
	DraftKingsSalary          *int    `json:"DraftKingsSalary"`
	FanDuelSalary             *int    `json:"FanDuelSalary"`
	FantasyDraftSalary        *int    `json:"FantasyDraftSalary"`

	DoubleEagles         float64 `json:"DoubleEagles"`
	Eagles               float64 `json:"Eagles"`
	Birdies              float64 `json:"Birdies"`
	Pars                 float64 `json:"Pars"`
	Bogeys               float64 `json:"Bogeys"`
	DoubleBogeys         float64 `json:"DoubleBogeys"`
	WorseThanDoubleBogey float64 `json:"WorseThanDoubleBogey"`
	HoleInOnes           float64 `json:"HoleInOnes"`
	TripleBogeys         float64 `json:"TripleBogeys"`
	WorseThanTripleBogey float64 `json:"WorseThanTripleBogey"`

	StreaksOfThreeBirdiesOrBetter       float64 `json:"StreaksOfThreeBirdiesOrBetter"`
	StreaksOfFourBirdiesOrBetter        float64 `json:"StreaksOfFourBirdiesOrBetter"`
	StreaksOfFiveBirdiesOrBetter        float64 `json:"StreaksOfFiveBirdiesOrBetter"`
	StreaksOfSixBirdiesOrBetter         float64 `json:"StreaksOfSixBirdiesOrBetter"`
	BogeyFreeRounds                     float64 `json:"BogeyFreeRounds"`
	RoundsUnderSeventy                  float64 `json:"RoundsUnderSeventy"`
	ConsecutiveBirdieOrBetterCount      float64 `json:"ConsecutiveBirdieOrBetterCount"`
	BounceBackCount                     float64 `json:"BounceBackCount"`
	RoundsWithFiveOrMoreBirdiesOrBetter float64 `json:"RoundsWithFiveOrMoreBirdiesOrBetter"`

	TeeTime              *string  `json:"TeeTime"`
	MadeCut              float64  `json:"MadeCut"`
	Win                  float64  `json:"Win"`
	TournamentStatus     *string  `json:"TournamentStatus"`
	IsAlternate          bool     `json:"IsAlternate"`
	MadeCutDidNotFinish  bool     `json:"MadeCutDidNotFinish"`
	IsWithdrawn          bool     `json:"IsWithdrawn"`
	OddsToWin            *float64 `json:"OddsToWin"`
	OddsToWinDescription *string  `json:"OddsToWinDescription"`

	Rounds []GolfPlayerRound `json:"Rounds"`
}

// GolfLeaderboard is the /Leaderboard/{tournamentid} response.
type GolfLeaderboard struct {
	Tournament GolfTournament         `json:"Tournament"`
	Players    []GolfPlayerTournament `json:"Players"`
}
