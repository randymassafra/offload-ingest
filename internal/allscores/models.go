// Package allscores holds Go structs mirroring the AllScores API's wire format,
// as served through RapidAPI.
//
// This is the pipeline's third provider, carrying tennis. SportsDataIO sells a
// tennis feed but this key is not scoped to it (401 Unauthorized Endpoint), so
// the tennis models were previously invented. AllScores serves real tennis
// match data and these structs are generated from it.
//
// What it does and does not give us:
//
//   - Set-by-set scores, including tiebreak columns, on game.stages.
//   - Player identity with live ATP/WTA ranking on the competitors.
//   - NO point-by-point. The document sets hasPointByPoint: true, but the API
//     exposes no route serving it — /point-by-point, /game-point-by-point and
//     every other spelling return 404, and the published tool list has none.
//     The flag describes what the upstream app renders, not what this API
//     returns, so the tennis telemetry feed carries set-score updates instead.
//
// GENERATED FROM CAPTURED RESPONSES — regenerate with:
//
//	go run ./cmd/schematool infer -file allscores/tennis_game_details.json \
//	    -path game.stages -type Stage
//
// Source: fixtures/allscores, captured from /api/allscores/game-details.
package allscores

// Stage is one set of a tennis match, or the match-level set tally.
//
// The optional fields really are omitted rather than sent as zero or null: an
// unplayed set carries only its id, name and -1 scores, and the extra-score
// columns appear only when a set went to a tiebreak. omitempty reproduces that,
// which is the opposite of the SportsDataIO models where a nullable field is
// always present.
type Stage struct {
	ID                       int    `json:"id"`
	Name                     string `json:"name"`
	ShortName                string `json:"shortName"`
	HomeCompetitorScore      int    `json:"homeCompetitorScore"`
	AwayCompetitorScore      int    `json:"awayCompetitorScore"`
	HomeCompetitorExtraScore int    `json:"homeCompetitorExtraScore,omitempty"`
	AwayCompetitorExtraScore int    `json:"awayCompetitorExtraScore,omitempty"`
	Time                     string `json:"time,omitempty"`
	IsEnded                  bool   `json:"isEnded,omitempty"`
	IsCurrent                bool   `json:"isCurrent,omitempty"`
}

// DELIBERATELY NOT MODELLED: promotedPredictions.
//
// The upstream attaches a bookmaker promo to tennis matches — roughly 40 JSON
// paths of odds, bookmaker branding and vote tallies. It is betting content,
// not match data, and the pipeline does not ingest betting content, so it is
// omitted here and excluded from the schema comparison on both sides rather
// than left to look like an accidental gap.
//
// If odds ever become in scope, regenerate from the capture:
//
//	go run ./cmd/schematool infer -file allscores/tennis_game_details.json \
//	    -path game.promotedPredictions.predictions -type Prediction

type Competitor struct {
	Color             string    `json:"color"`
	CountryID         int       `json:"countryId"`
	CreatedAt         string    `json:"createdAt,omitempty"`
	ID                int       `json:"id"`
	ImageVersion      int       `json:"imageVersion"`
	IsQualified       bool      `json:"isQualified"`
	IsWinner          bool      `json:"isWinner"`
	MainCompetitionID int       `json:"mainCompetitionId"`
	Name              string    `json:"name"`
	NameForURL        string    `json:"nameForURL"`
	Rankings          []Ranking `json:"rankings,omitempty"`
	RecentMatches     []int     `json:"recentMatches"`
	Score             int       `json:"score"`
	SportID           int       `json:"sportId"`
	ToQualify         bool      `json:"toQualify"`
	Type              int       `json:"type"`
	// A club carries a long name, a short name, an away kit colour and a
	// lineup. All four are conditional: a tennis competitor has no kit and no
	// teamsheet, and the provider sends the name variants only where they
	// differ from name — an unabbreviated club has no shortName.
	LongName  string  `json:"longName,omitempty"`
	ShortName string  `json:"shortName,omitempty"`
	AwayColor string  `json:"awayColor,omitempty"`
	Lineups   *Lineup `json:"lineups,omitempty"`
}

// ScoreboardCompetitor is a club as the games-scores lookup table describes it.
//
// It is a catalog entry, not a participant: it carries the squad and transfer
// flags a directory needs and none of the per-match state — no score, no
// winner, no recent form. Modelling it apart from Competitor keeps both exact.
type ScoreboardCompetitor struct {
	ID                int    `json:"id"`
	CountryID         int    `json:"countryId"`
	SportID           int    `json:"sportId"`
	Name              string `json:"name"`
	LongName          string `json:"longName"`
	SymbolicName      string `json:"symbolicName"`
	NameForURL        string `json:"nameForURL"`
	Type              int    `json:"type"`
	PopularityRank    int    `json:"popularityRank"`
	ImageVersion      int    `json:"imageVersion"`
	Color             string `json:"color"`
	AwayColor         string `json:"awayColor"`
	MainCompetitionID int    `json:"mainCompetitionId"`
	ShortName         string `json:"shortName"`
	CreatedAt         string `json:"createdAt"`
	HasSquad          bool   `json:"hasSquad"`
	HasTransfers      bool   `json:"hasTransfers"`
	CompetitorNum     int    `json:"competitorNum"`
	HideOnSearch      bool   `json:"hideOnSearch"`
	HideOnCatalog     bool   `json:"hideOnCatalog"`

	// Per-fixture state, carried only where the competitor is embedded in a
	// fixture row; the standalone catalog entry omits all of it. These are
	// pointers rather than plain values because the distinction that matters
	// is present-and-false against absent, and omitempty on a bool erases it.
	Score       *int  `json:"score,omitempty"`
	IsQualified *bool `json:"isQualified,omitempty"`
	IsWinner    *bool `json:"isWinner,omitempty"`
	ToQualify   *bool `json:"toQualify,omitempty"`
	RedCards    int   `json:"redCards,omitempty"`
}

type Venue struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	ShortName string `json:"shortName"`
	// Soccer venues carry a gate, a capacity and a Places lookup; a tennis
	// court has none of them.
	Attendance    int    `json:"attendance,omitempty"`
	Capacity      int    `json:"capacity,omitempty"`
	GooglePlaceID string `json:"googlePlaceId,omitempty"`
}

// Ranking is a tour ranking carried on a tennis competitor.
type Ranking struct {
	Name     string `json:"name"`
	Position int    `json:"position"`
}

// Game is the match document returned by /api/allscores/game-details.
type Game struct {
	ID                           int        `json:"id"`
	SportID                      int        `json:"sportId"`
	CompetitionID                int        `json:"competitionId"`
	StatusID                     int        `json:"statusId"`
	SeasonNum                    int        `json:"seasonNum"`
	StageNum                     int        `json:"stageNum"`
	StageName                    string     `json:"stageName,omitempty"`
	CompetitionDisplayName       string     `json:"competitionDisplayName"`
	StartTime                    string     `json:"startTime"`
	StatusGroup                  int        `json:"statusGroup"`
	StatusText                   string     `json:"statusText"`
	ShortStatusText              string     `json:"shortStatusText"`
	GameTimeAndStatusDisplayType int        `json:"gameTimeAndStatusDisplayType"`
	JustEnded                    bool       `json:"justEnded"`
	GameTime                     float64    `json:"gameTime"`
	GameTimeDisplay              string     `json:"gameTimeDisplay"`
	WinDescription               string     `json:"winDescription"`
	HomeCompetitor               Competitor `json:"homeCompetitor"`
	AwayCompetitor               Competitor `json:"awayCompetitor"`
	Stages                       []Stage    `json:"stages"`
	IsHomeAwayInverted           bool       `json:"isHomeAwayInverted"`
	Venue                        Venue      `json:"venue"`
	GameStageHasTable            bool       `json:"gameStageHasTable"`
	LineTypesIds                 []int      `json:"lineTypesIds"`

	// Soccer-only blocks. Present when the sport has them, omitted otherwise,
	// which is how one model covers both a tennis match and a league fixture.
	RoundName       string           `json:"roundName,omitempty"`
	RoundNum        int              `json:"roundNum,omitempty"`
	StandingsName   string           `json:"standingsName,omitempty"`
	Members         []Member         `json:"members,omitempty"`
	Events          []Event          `json:"events,omitempty"`
	Officials       []Official       `json:"officials,omitempty"`
	PlayByPlay      *PlayByPlayFeed  `json:"playByPlay,omitempty"`
	TVNetworks      []TVNetwork      `json:"tvNetworks,omitempty"`
	ActualPlayTime  *ActualPlayTime  `json:"actualPlayTime,omitempty"`
	PreciseGameTime *PreciseGameTime `json:"preciseGameTime,omitempty"`
	ChartEvents     *ChartEvents     `json:"chartEvents,omitempty"`
	TopPerformers   *TopPerformers   `json:"topPerformers,omitempty"`
	Widgets         []Widget         `json:"widgets,omitempty"`
	HasVideo        bool             `json:"hasVideo,omitempty"`
	Video           *Video           `json:"video,omitempty"`

	HasFieldPositions bool `json:"hasFieldPositions,omitempty"`
	HasLineups        bool `json:"hasLineups,omitempty"`
	HasMissingPlayers bool `json:"hasMissingPlayers,omitempty"`
	HasNews           bool `json:"hasNews,omitempty"`
	HasPlayerBets     bool `json:"hasPlayerBets,omitempty"`
	HasShotChart      bool `json:"hasShotChart,omitempty"`

	// Capability flags. hasPointByPoint is reproduced because the upstream
	// sends it, but no route serves that data — see the package comment.
	HasBets             bool `json:"hasBets"`
	HasBetsTeaser       bool `json:"hasBetsTeaser"`
	HasBrackets         bool `json:"hasBrackets"`
	HasPointByPoint     bool `json:"hasPointByPoint"`
	HasRecentMatches    bool `json:"hasRecentMatches"`
	HasStandings        bool `json:"hasStandings"`
	HasStats            bool `json:"hasStats"`
	HasTrends           bool `json:"hasTrends"`
	HasPreviousMeetings bool `json:"hasPreviousMeetings"`
	HasTopTrends        bool `json:"hasTopTrends"`
}

// GameDetails is the /api/allscores/game-details response envelope.
type GameDetails struct {
	LastUpdateID      int  `json:"lastUpdateId"`
	RequestedUpdateID int  `json:"requestedUpdateId"`
	TTL               int  `json:"ttl"`
	Game              Game `json:"game"`
}

// --- Soccer ---------------------------------------------------------------
//
// AllScores serves one schema across sports, with the richer blocks present
// only where the sport has them. Everything below is soccer-only and carries
// omitempty so a tennis payload — which has none of it — stays byte-identical
// to the tennis wire.
//
// This is why soccer moved here from SportsDataIO: that key is scoped to the
// UEFA Champions League alone, while this feed serves the Premier League,
// LaLiga, Serie A, the Bundesliga, Ligue 1, MLS and the Champions League, and
// carries squads, an event timeline, lineups with per-player statistics and
// match officials.

// Member is one squad entry on the match document.
type Member struct {
	ID           int    `json:"id"`
	AthleteID    int    `json:"athleteId"`
	CompetitorID int    `json:"competitorId"`
	Name         string `json:"name"`
	ShortName    string `json:"shortName"`
	NameForURL   string `json:"nameForURL"`
	JerseyNumber int    `json:"jerseyNumber"`
	ImageVersion int    `json:"imageVersion"`
	CreatedAt    string `json:"createdAt"`
}

// EventType classifies a timeline event: goal, card, substitution and so on.
type EventType struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	SubTypeID   int    `json:"subTypeId"`
	SubTypeName string `json:"subTypeName,omitempty"`
}

// Event is one entry on the match timeline.
type Event struct {
	CompetitorID                 int     `json:"competitorId"`
	StatusID                     int     `json:"statusId"`
	StageID                      int     `json:"stageId"`
	Order                        int     `json:"order"`
	Num                          int     `json:"num"`
	GameTime                     float64 `json:"gameTime"`
	AddedTime                    int     `json:"addedTime"`
	GameTimeDisplay              string  `json:"gameTimeDisplay"`
	GameTimeAndStatusDisplayType int     `json:"gameTimeAndStatusDisplayType"`
	PlayerID                     int     `json:"playerId"`
	IsMajor                      bool    `json:"isMajor"`
	// extraPlayers is the substitution's other half, or a goal's assister.
	ExtraPlayers []int     `json:"extraPlayers"`
	EventType    EventType `json:"eventType"`
}

// Official is a match official.
type Official struct {
	ID         int    `json:"id"`
	AthleteID  int    `json:"athleteId"`
	CountryID  int    `json:"countryId"`
	Name       string `json:"name"`
	NameForURL string `json:"nameForURL"`
	Status     int    `json:"status"`
}

// Position, Formation and YardFormation place a player in the lineup.
type Position struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type Formation struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	ShortName string `json:"shortName"`
}

type YardFormation struct {
	Line          int `json:"line"`
	FieldLine     int `json:"fieldLine"`
	FieldPosition int `json:"fieldPosition"`
	FieldSide     int `json:"fieldSide"`
}

// LineupStat is one per-player statistic. As with the rugby feed, the value
// arrives as a string even when it is a count.
type LineupStat struct {
	CategoryID int    `json:"categoryId"`
	Name       string `json:"name"`
	ShortName  string `json:"shortName"`
	Value      string `json:"value"`
	Type       int    `json:"type"`
	Order      int    `json:"order"`
	ImageID    int    `json:"imageId"`
	IsTop      bool   `json:"isTop"`
}

// Substitution records a player coming on or off.
type Substitution struct {
	PlayerID int `json:"playerId"`
	Time     int `json:"time"`
	// AddedTime is the stoppage minute the change was made in, sent only for a
	// substitution that happened during added time.
	AddedTime  int `json:"addedTime,omitempty"`
	Type       int `json:"type"`
	Status     int `json:"status"`
	EventOrder int `json:"eventOrder"`
}

// SeasonStat is a season-to-date note the provider shows against a player,
// already rendered for display: "Appearances (1/2)".
type SeasonStat struct {
	Text string `json:"text"`
	Type int    `json:"type"`
}

// LineupMember is one player in a starting XI or on the bench.
type LineupMember struct {
	ID                int           `json:"id"`
	CompetitorID      int           `json:"competitorId"`
	NationalID        int           `json:"nationalId"`
	Status            int           `json:"status"`
	StatusText        string        `json:"statusText"`
	Position          Position      `json:"position"`
	Formation         Formation     `json:"formation"`
	YardFormation     YardFormation `json:"yardFormation"`
	Stats             []LineupStat  `json:"stats"`
	HasStats          bool          `json:"hasStats"`
	Ranking           float64       `json:"ranking"`
	PopularityRank    int           `json:"popularityRank"`
	HeatMap           string        `json:"heatMap"`
	CreatedAt         string        `json:"createdAt"`
	HasShotChart      bool          `json:"hasShotChart"`
	HasHighestRanking bool          `json:"hasHighestRanking"`
	Substitution      *Substitution `json:"substitution,omitempty"`
	// A player listed as Missing carries the reason: an injury with an
	// expected return, or a suspension.
	Injury     *Injury     `json:"injury,omitempty"`
	Suspension *Suspension `json:"suspension,omitempty"`
	// SeasonStats is season-to-date context, carried for some players only.
	SeasonStats []SeasonStat `json:"seasonStats,omitempty"`
}

// Injury is why a squad member is unavailable. imageId is the provider's icon
// key rather than a number, despite the name.
type Injury struct {
	CategoryID     int    `json:"categoryId"`
	Reason         string `json:"reason"`
	ExpectedReturn string `json:"expectedReturn"`
	ImageID        string `json:"imageId"`
	ImageVersion   int    `json:"imageVersion"`
}

// Suspension is a ban keeping a player out, named by the offence that earned it.
type Suspension struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// StatsCategory names a grouping of the per-player statistics.
type StatsCategory struct {
	ID              int             `json:"id"`
	Name            string          `json:"name"`
	OrderLevel      int             `json:"orderLevel"`
	OrderByPosition []PositionOrder `json:"orderByPosition"`
}

// PositionOrder ranks a statistic differently per playing position, which is
// how the upstream shows a keeper saves before it shows them shots.
type PositionOrder struct {
	Position      int `json:"position"`
	PositionOrder int `json:"positionOrder"`
}

// Lineup is one side's team sheet.
type Lineup struct {
	Status            string          `json:"status"`
	Formation         string          `json:"formation"`
	HasFieldPositions bool            `json:"hasFieldPositions"`
	Members           []LineupMember  `json:"members"`
	StatsCategory     []StatsCategory `json:"statsCategory"`
}

// PlayByPlayFeed is a pair of URLs pointing at a separate generator service.
// It carries no play data itself, which is why there is no soccer play-by-play
// feed sourced from it — the timeline comes from Events instead.
type PlayByPlayFeed struct {
	FeedURL        string `json:"feedURL"`
	PreviewFeedURL string `json:"previewFeedUrl"`
}

// TVNetwork is a broadcaster carrying the fixture. The bookmakerId field is the
// provider's, not ours — a network row reuses the bookmaker table's id space.
type TVNetwork struct {
	BookmakerID  int    `json:"bookmakerId"`
	CountryID    int    `json:"countryId"`
	ID           int    `json:"id"`
	ImageVersion int    `json:"imageVersion"`
	Name         string `json:"name"`
	Type         int    `json:"type"`
	Website      string `json:"website"`
}

// PlayClock is one half of the actual-play-time split: a label and the share of
// the match it accounts for.
type PlayClock struct {
	Name     string  `json:"name"`
	Progress float64 `json:"progress"`
}

// ActualPlayTime is the ball-in-play breakdown soccer fixtures carry.
type ActualPlayTime struct {
	ActualTime PlayClock `json:"actualTime"`
	Title      string    `json:"title"`
	TotalTime  PlayClock `json:"totalTime"`
}

// PreciseGameTime is the match clock to the second, with the direction it runs.
type PreciseGameTime struct {
	AutoProgress   bool `json:"autoProgress"`
	ClockDirection int  `json:"clockDirection"`
	Minutes        int  `json:"minutes"`
	Seconds        int  `json:"seconds"`
}

// ChartEventType is one legend entry of the shot chart, used for both the type
// and sub-type dimensions — the provider gives them the same shape.
type ChartEventType struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Value int    `json:"value"`
}

// ChartOutcome is where a shot ended up. x/y locate it on the pitch, z on the
// goalmouth, so a blocked shot and a top-corner finish differ in z alone.
type ChartOutcome struct {
	ID   int     `json:"id"`
	Name string  `json:"name"`
	Y    float64 `json:"y"`
	Z    float64 `json:"z,omitempty"`
	X    float64 `json:"x"`
}

// ChartEvent is one shot. Xg is a string on the wire despite being a number,
// which is reproduced rather than corrected.
type ChartEvent struct {
	BodyPart      string       `json:"bodyPart"`
	CompetitorNum int          `json:"competitorNum"`
	GameID        int          `json:"gameId"`
	Key           string       `json:"key"`
	Line          float64      `json:"line"`
	Outcome       ChartOutcome `json:"outcome"`
	PlayerID      int          `json:"playerId"`
	Side          float64      `json:"side"`
	Status        int          `json:"status"`
	SubType       int          `json:"subType"`
	Time          string       `json:"time"`
	Type          int          `json:"type"`
	Xg            string       `json:"xg"`
	Xgot          string       `json:"xgot"`
	// goalDescription names where in the goal the ball finished. Only a goal
	// carries it.
	GoalDescription string `json:"goalDescription,omitempty"`
}

// ChartStatus is a match-status row the chart legend filters on.
type ChartStatus struct {
	AliasName         string `json:"aliasName"`
	SymbolName        string `json:"symbolName"`
	AutonomicTime     bool   `json:"autonomicTime"`
	GameTimeForStatus bool   `json:"gameTimeForStatus"`
	HasEvents         bool   `json:"hasEvents"`
	ID                int    `json:"id"`
	IsAbnormal        bool   `json:"isAbnormal"`
	IsActive          bool   `json:"isActive"`
	IsExtraTime       bool   `json:"isExtraTime"`
	IsFinished        bool   `json:"isFinished"`
	IsNotStarted      bool   `json:"isNotStarted"`
	IsPenalties       bool   `json:"isPenalties"`
	Name              string `json:"name"`
	SportTypeID       int    `json:"sportTypeId"`
}

// ChartEvents is the shot chart: the events plus the legend needed to plot them.
type ChartEvents struct {
	EventSubTypes []ChartEventType `json:"eventSubTypes"`
	EventTypes    []ChartEventType `json:"eventTypes"`
	Events        []ChartEvent     `json:"events"`
	Statuses      []ChartStatus    `json:"statuses"`
}

// TopPerformer is the standout player on one side of a category.
//
// DELIBERATELY NOT MODELLED: relatedLines. As with promotedPredictions, the
// provider hangs bookmaker odds off each performer — bookmaker branding, line
// types and priced options. Betting content, excluded on both sides.
type TopPerformer struct {
	AthleteID         int          `json:"athleteId"`
	CreatedAt         string       `json:"createdAt"`
	ID                int          `json:"id"`
	ImageVersion      int          `json:"imageVersion"`
	Name              string       `json:"name"`
	NameForURL        string       `json:"nameForURL"`
	PositionName      string       `json:"positionName"`
	PositionShortName string       `json:"positionShortName"`
	ShortName         string       `json:"shortName"`
	Stats             []LineupStat `json:"stats"`
}

// TopPerformerCategory pairs the two sides' leaders in one statistic.
type TopPerformerCategory struct {
	AwayPlayer TopPerformer `json:"awayPlayer"`
	HomePlayer TopPerformer `json:"homePlayer"`
	Name       string       `json:"name"`
}

// TopPerformers is the match's leader board, one entry per statistic.
type TopPerformers struct {
	Categories []TopPerformerCategory `json:"categories"`
}

// Widget is the third-party match-tracker embed a soccer fixture carries. It is
// a pointer to someone else's renderer rather than data, but it is on the wire.
type Widget struct {
	PartnerID   string  `json:"partnerId"`
	Provider    string  `json:"provider"`
	WidgetRatio float64 `json:"widgetRatio"`
	WidgetType  string  `json:"widgetType"`
	WidgetURL   string  `json:"widgetUrl"`
}

// --- multi-league scoreboard ------------------------------------------------
//
// /api/allscores/games-scores returns every fixture in a date window across
// every competition the provider covers, with the lookup tables needed to
// resolve them. This is what makes soccer a multi-league feed rather than a
// single-competition one: 198 competitions across 115 countries in one call.

// Video is the highlights clip attached to a finished match. embedElement is a
// template with #w, #h and #id placeholders, sent as a string rather than
// pre-filled.
type Video struct {
	EmbedElement string `json:"embedElement"`
	ID           string `json:"id"`
	IsEmbedded   bool   `json:"isEmbedded"`
	Source       int    `json:"source"`
	Type         int    `json:"type"`
	URL          string `json:"url"`
}

// SportRef is the sport header on a scoreboard document.
type SportRef struct {
	ID           int    `json:"id"`
	Name         string `json:"name"`
	NameForURL   string `json:"nameForURL"`
	DrawSupport  bool   `json:"drawSupport"`
	ImageVersion int    `json:"imageVersion"`
}

// Country is one entry of the scoreboard's country lookup table.
type Country struct {
	ID           int    `json:"id"`
	Name         string `json:"name"`
	TotalGames   int    `json:"totalGames"`
	LiveGames    int    `json:"liveGames"`
	NameForURL   string `json:"nameForURL"`
	ImageVersion int    `json:"imageVersion"`
	// The board files continental and friendly competitions under a pseudo
	// country, id 54, which is the only row that carries this flag.
	IsInternational bool `json:"isInternational,omitempty"`
}

// Competition is one league or cup. The scoreboard resolves a game's
// competitionId against this table.
type Competition struct {
	ID                 int    `json:"id"`
	CountryID          int    `json:"countryId"`
	SportID            int    `json:"sportId"`
	Name               string `json:"name"`
	ShortName          string `json:"shortName"`
	NameForURL         string `json:"nameForURL"`
	PopularityRank     int    `json:"popularityRank"`
	Color              string `json:"color"`
	TotalGames         int    `json:"totalGames"`
	LiveGames          int    `json:"liveGames"`
	CompetitorsType    int    `json:"competitorsType"`
	CurrentPhaseNum    int    `json:"currentPhaseNum"`
	CurrentSeasonNum   int    `json:"currentSeasonNum"`
	CurrentStageNum    int    `json:"currentStageNum"`
	CurrentStageType   int    `json:"currentStageType"`
	HasActiveGames     bool   `json:"hasActiveGames"`
	HasBrackets        bool   `json:"hasBrackets"`
	HasLiveStandings   bool   `json:"hasLiveStandings"`
	HasStandings       bool   `json:"hasStandings"`
	HasStandingsGroups bool   `json:"hasStandingsGroups"`
	HideOnCatalog      bool   `json:"hideOnCatalog"`
	HideOnSearch       bool   `json:"hideOnSearch"`
	IsInternational    bool   `json:"isInternational"`
	ImageVersion       int    `json:"imageVersion"`
	LongName           string `json:"longName"`
	CreatedAt          string `json:"createdAt"`
	CurrentPhaseName   string `json:"currentPhaseName"`
}

// ScoreboardGame is one fixture row of the games-scores listing.
//
// It is a different document from Game, not a sparse one: it drops the venue,
// the stage breakdown and the status id, and adds the lineup-availability and
// broadcast flags a scoreboard needs to decide what to render. Modelling it
// separately keeps both honest.
//
// DELIBERATELY NOT MODELLED: odds. See GamesScores.
type ScoreboardGame struct {
	ID                           int                  `json:"id"`
	SportID                      int                  `json:"sportId"`
	CompetitionID                int                  `json:"competitionId"`
	SeasonNum                    int                  `json:"seasonNum"`
	StageNum                     int                  `json:"stageNum"`
	GroupNum                     int                  `json:"groupNum"`
	RoundNum                     int                  `json:"roundNum"`
	RoundName                    string               `json:"roundName"`
	StageName                    string               `json:"stageName"`
	StandingsName                string               `json:"standingsName"`
	CompetitionDisplayName       string               `json:"competitionDisplayName"`
	StartTime                    string               `json:"startTime"`
	StatusGroup                  int                  `json:"statusGroup"`
	StatusText                   string               `json:"statusText"`
	ShortStatusText              string               `json:"shortStatusText"`
	GameTimeAndStatusDisplayType int                  `json:"gameTimeAndStatusDisplayType"`
	JustEnded                    bool                 `json:"justEnded"`
	GameTime                     float64              `json:"gameTime"`
	GameTimeDisplay              string               `json:"gameTimeDisplay"`
	HasLineups                   bool                 `json:"hasLineups"`
	HasMissingPlayers            bool                 `json:"hasMissingPlayers"`
	HasFieldPositions            bool                 `json:"hasFieldPositions"`
	LineupsStatus                int                  `json:"lineupsStatus"`
	LineupsStatusText            string               `json:"lineupsStatusText"`
	HasTVNetworks                bool                 `json:"hasTVNetworks"`
	HasBetsTeaser                bool                 `json:"hasBetsTeaser"`
	WinDescription               string               `json:"winDescription"`
	HomeCompetitor               ScoreboardCompetitor `json:"homeCompetitor"`
	AwayCompetitor               ScoreboardCompetitor `json:"awayCompetitor"`
	HomeAwayTeamOrder            int                  `json:"homeAwayTeamOrder"`
	IsHomeAwayInverted           bool                 `json:"isHomeAwayInverted"`
	Winner                       int                  `json:"winner"`
	HasBets                      bool                 `json:"hasBets"`
	HasBrackets                  bool                 `json:"hasBrackets"`
	HasPlayerBets                bool                 `json:"hasPlayerBets"`
	HasPointByPoint              bool                 `json:"hasPointByPoint"`
	HasPreviousMeetings          bool                 `json:"hasPreviousMeetings"`
	HasRecentMatches             bool                 `json:"hasRecentMatches"`
	HasStandings                 bool                 `json:"hasStandings"`
	HasStats                     bool                 `json:"hasStats"`
	HasVideo                     bool                 `json:"hasVideo"`
	HasNews                      bool                 `json:"hasNews"`
	HasLiveStreaming             bool                 `json:"hasLiveStreaming"`
	// AddedTime is stoppage time in the half being played, and the group names
	// describe how a scoreboard should bucket the fixture.
	AddedTime              int    `json:"addedTime"`
	GroupName              string `json:"groupName"`
	CompetitionGroupByName string `json:"competitionGroupByName"`
}

// GamesScores is the /api/allscores/games-scores response.
//
// DELIBERATELY NOT MODELLED: bookmakers, and the odds block on each game.
// Same reasoning as promotedPredictions — betting content is out of scope, so
// it is omitted here and excluded from the schema comparison on both sides.
type GamesScores struct {
	LastUpdateID      int                    `json:"lastUpdateId"`
	RequestedUpdateID int                    `json:"requestedUpdateId"`
	TTL               int                    `json:"ttl"`
	Summary           struct{}               `json:"summary"`
	Sports            []SportRef             `json:"sports"`
	Countries         []Country              `json:"countries"`
	Competitions      []Competition          `json:"competitions"`
	Competitors       []ScoreboardCompetitor `json:"competitors"`
	Games             []ScoreboardGame       `json:"games"`
}
