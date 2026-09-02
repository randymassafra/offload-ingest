package generators

// competitor is a pool entry: a club, a national side, a player or a driver.
// ID and GlobalID mirror the convention of a per-API id alongside a
// cross-API global id.
type competitor struct {
	ID       int
	GlobalID int
	Key      string
	Name     string
	Country  string
}

// Pools are intentionally small. Load tests care about payload shape and
// partition-key cardinality, not roster completeness — widen a pool to raise
// the number of distinct fixtures a sport can produce.
var (
	nflTeams = []competitor{
		{1, 90000001, "KC", "Kansas City Chiefs", "USA"},
		{2, 90000002, "SF", "San Francisco 49ers", "USA"},
		{3, 90000003, "BUF", "Buffalo Bills", "USA"},
		{4, 90000004, "PHI", "Philadelphia Eagles", "USA"},
		{5, 90000005, "DAL", "Dallas Cowboys", "USA"},
		{6, 90000006, "BAL", "Baltimore Ravens", "USA"},
		{7, 90000007, "DET", "Detroit Lions", "USA"},
		{8, 90000008, "GB", "Green Bay Packers", "USA"},
	}

	ncaafTeams = []competitor{
		{101, 90000101, "UGA", "Georgia Bulldogs", "USA"},
		{102, 90000102, "MICH", "Michigan Wolverines", "USA"},
		{103, 90000103, "TEX", "Texas Longhorns", "USA"},
		{104, 90000104, "BAMA", "Alabama Crimson Tide", "USA"},
		{105, 90000105, "OSU", "Ohio State Buckeyes", "USA"},
		{106, 90000106, "ORE", "Oregon Ducks", "USA"},
		{107, 90000107, "LSU", "LSU Tigers", "USA"},
		{108, 90000108, "ND", "Notre Dame Fighting Irish", "USA"},
	}

	nbaTeams = []competitor{
		{201, 90000201, "BOS", "Boston Celtics", "USA"},
		{202, 90000202, "DEN", "Denver Nuggets", "USA"},
		{203, 90000203, "MIL", "Milwaukee Bucks", "USA"},
		{204, 90000204, "OKC", "Oklahoma City Thunder", "USA"},
		{205, 90000205, "NYK", "New York Knicks", "USA"},
		{206, 90000206, "LAL", "Los Angeles Lakers", "USA"},
		{207, 90000207, "DAL", "Dallas Mavericks", "USA"},
		{208, 90000208, "MIN", "Minnesota Timberwolves", "USA"},
	}

	ncaabTeams = []competitor{
		{301, 90000301, "CONN", "UConn Huskies", "USA"},
		{302, 90000302, "PUR", "Purdue Boilermakers", "USA"},
		{303, 90000303, "HOU", "Houston Cougars", "USA"},
		{304, 90000304, "DUKE", "Duke Blue Devils", "USA"},
		{305, 90000305, "KU", "Kansas Jayhawks", "USA"},
		{306, 90000306, "ARIZ", "Arizona Wildcats", "USA"},
		{307, 90000307, "UNC", "North Carolina Tar Heels", "USA"},
		{308, 90000308, "UK", "Kentucky Wildcats", "USA"},
	}

	eplTeams = []competitor{
		{401, 90000401, "MCI", "Manchester City", "ENG"},
		{402, 90000402, "ARS", "Arsenal", "ENG"},
		{403, 90000403, "LIV", "Liverpool", "ENG"},
		{404, 90000404, "AVL", "Aston Villa", "ENG"},
		{405, 90000405, "TOT", "Tottenham Hotspur", "ENG"},
		{406, 90000406, "CHE", "Chelsea", "ENG"},
		{407, 90000407, "NEW", "Newcastle United", "ENG"},
		{408, 90000408, "MUN", "Manchester United", "ENG"},
	}

	aflTeams = []competitor{
		{501, 90000501, "COLL", "Collingwood Magpies", "AUS"},
		{502, 90000502, "BL", "Brisbane Lions", "AUS"},
		{503, 90000503, "SYD", "Sydney Swans", "AUS"},
		{504, 90000504, "PORT", "Port Adelaide Power", "AUS"},
		{505, 90000505, "CARL", "Carlton Blues", "AUS"},
		{506, 90000506, "GEEL", "Geelong Cats", "AUS"},
		{507, 90000507, "MELB", "Melbourne Demons", "AUS"},
		{508, 90000508, "WB", "Western Bulldogs", "AUS"},
	}

	rugbyTeams = []competitor{
		{601, 90000601, "PEN", "Penrith Panthers", "AUS"},
		{602, 90000602, "MEL", "Melbourne Storm", "AUS"},
		{603, 90000603, "BRI", "Brisbane Broncos", "AUS"},
		{604, 90000604, "SYD", "Sydney Roosters", "AUS"},
		{605, 90000605, "LEIN", "Leinster", "IRL"},
		{606, 90000606, "TLS", "Toulouse", "FRA"},
		{607, 90000607, "SAR", "Saracens", "ENG"},
		{608, 90000608, "CRU", "Crusaders", "NZL"},
	}

	cricketTeams = []competitor{
		{701, 90000701, "IND", "India", "IND"},
		{702, 90000702, "AUS", "Australia", "AUS"},
		{703, 90000703, "ENG", "England", "ENG"},
		{704, 90000704, "SA", "South Africa", "RSA"},
		{705, 90000705, "NZ", "New Zealand", "NZL"},
		{706, 90000706, "PAK", "Pakistan", "PAK"},
		{707, 90000707, "SL", "Sri Lanka", "SRI"},
		{708, 90000708, "WI", "West Indies", "WIN"},
	}

	tennisPlayers = []competitor{
		{801, 90000801, "ALC", "Carlos Alcaraz", "ESP"},
		{802, 90000802, "SIN", "Jannik Sinner", "ITA"},
		{803, 90000803, "DJO", "Novak Djokovic", "SRB"},
		{804, 90000804, "MED", "Daniil Medvedev", "RUS"},
		{805, 90000805, "SWI", "Iga Swiatek", "POL"},
		{806, 90000806, "SAB", "Aryna Sabalenka", "BLR"},
		{807, 90000807, "GAU", "Coco Gauff", "USA"},
		{808, 90000808, "RYB", "Elena Rybakina", "KAZ"},
	}

	golfPlayers = []competitor{
		{901, 90000901, "SCH", "Scottie Scheffler", "USA"},
		{902, 90000902, "MCI", "Rory McIlroy", "NIR"},
		{903, 90000903, "RAH", "Jon Rahm", "ESP"},
		{904, 90000904, "SCA", "Xander Schauffele", "USA"},
		{905, 90000905, "HOV", "Viktor Hovland", "NOR"},
		{906, 90000906, "ABE", "Ludvig Aberg", "SWE"},
		{907, 90000907, "MOR", "Collin Morikawa", "USA"},
		{908, 90000908, "KOE", "Brooks Koepka", "USA"},
	}

	ufcFighters = []competitor{
		{1001, 90001001, "MAK", "Islam Makhachev", "RUS"},
		{1002, 90001002, "PER", "Alex Pereira", "BRA"},
		{1003, 90001003, "JON", "Jon Jones", "USA"},
		{1004, 90001004, "EDW", "Leon Edwards", "GBR"},
		{1005, 90001005, "OMA", "Sean O'Malley", "USA"},
		{1006, 90001006, "TOP", "Ilia Topuria", "ESP"},
		{1007, 90001007, "ZHA", "Zhang Weili", "CHN"},
		{1008, 90001008, "GRA", "Alexa Grasso", "MEX"},
	}

	mmaFighters = []competitor{
		{1101, 90001101, "PIT", "Patricio Freire", "BRA"},
		{1102, 90001102, "NUR", "Usman Nurmagomedov", "RUS"},
		{1103, 90001103, "EBL", "Johnny Eblen", "USA"},
		{1104, 90001104, "NEM", "Vadim Nemkov", "RUS"},
		{1105, 90001105, "CYB", "Cris Cyborg", "BRA"},
		{1106, 90001106, "CAR", "Liz Carmouche", "USA"},
		{1107, 90001107, "COL", "Clay Collard", "USA"},
		{1108, 90001108, "CAP", "Bruno Cappelozza", "BRA"},
	}
)

var (
	nflVenues     = []string{"Arrowhead Stadium", "Levi's Stadium", "Highmark Stadium", "Lincoln Financial Field"}
	ncaafVenues   = []string{"Sanford Stadium", "Michigan Stadium", "DKR Memorial", "Bryant-Denny Stadium"}
	nbaVenues     = []string{"TD Garden", "Ball Arena", "Fiserv Forum", "Madison Square Garden"}
	ncaabVenues   = []string{"Gampel Pavilion", "Mackey Arena", "Cameron Indoor", "Allen Fieldhouse"}
	eplVenues     = []string{"Etihad Stadium", "Emirates Stadium", "Anfield", "Villa Park"}
	aflVenues     = []string{"MCG", "Gabba", "SCG", "Adelaide Oval"}
	rugbyVenues   = []string{"BlueBet Stadium", "AAMI Park", "Aviva Stadium", "Twickenham"}
	cricketVenues = []string{"Eden Gardens", "MCG", "Lord's", "Newlands"}
	tennisVenues  = []string{"Centre Court", "Philippe-Chatrier", "Arthur Ashe", "Rod Laver Arena"}
	golfVenues    = []string{"Augusta National", "St Andrews", "Pebble Beach", "TPC Sawgrass"}
	cageVenues    = []string{"T-Mobile Arena", "UFC Apex", "Madison Square Garden", "Etihad Arena"}
)

// --- API-Sports pools -------------------------------------------------------

// apiSportsTeamsFor supplies competitors for the verticals whose capture came
// back empty (out of season on the day we captured, or gated by the plan). The
// ids are ours, not the provider's: using a real API-Sports team id here would
// imply we are replaying that club's real fixtures, which we are not.
func apiSportsTeamsFor(s Sport) []competitor {
	switch s {
	case SportNFL, SportNCAAF:
		return nflTeams
	case SportNBA, SportNCAAB:
		return nbaTeams
	case SportAFL:
		return aflTeams
	case SportRugby:
		return rugbyTeams
	case SportUFC, SportMMA:
		return mmaFighters
	case SportF1:
		return f1Drivers
	default:
		return nflTeams
	}
}

// apiSportsLeagueFor is the competition block the provider attaches to every
// document. The ids are the provider's own, read from its live /leagues
// endpoint, so a consumer can join our stream against the API-Sports catalog.
func apiSportsLeagueFor(s Sport) map[string]any {
	league := func(id int, name, typ, country string) map[string]any {
		return map[string]any{
			"id": id, "name": name, "type": typ, "season": seasonFor(now()),
			"country": country,
			"logo":    "https://media.api-sports.io/leagues/" + itoa(id) + ".png",
		}
	}
	switch s {
	case SportNFL:
		return league(1, "NFL", "league", "USA")
	case SportNCAAF:
		return league(2, "NCAA", "league", "USA")
	case SportNCAAB:
		return league(116, "NCAA", "league", "USA")
	case SportNBA:
		return league(12, "NBA", "league", "USA")
	case SportAFL:
		return league(1, "AFL Premiership", "league", "Australia")
	case SportRugby:
		return league(13, "Premiership Rugby", "league", "England")
	case SportUFC, SportMMA:
		return league(1, "UFC", "league", "World")
	case SportF1:
		return league(1, "Formula 1", "race", "World")
	default:
		return league(1, "League", "league", "World")
	}
}

// f1Drivers is a small driver pool for the Formula 1 feed.
var f1Drivers = []competitor{
	{701, 90000701, "VER", "Max Verstappen", "NED"},
	{702, 90000702, "NOR", "Lando Norris", "GBR"},
	{703, 90000703, "LEC", "Charles Leclerc", "MON"},
	{704, 90000704, "RUS", "George Russell", "GBR"},
	{705, 90000705, "PIA", "Oscar Piastri", "AUS"},
	{706, 90000706, "HAM", "Lewis Hamilton", "GBR"},
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
