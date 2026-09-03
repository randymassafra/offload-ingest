# offload-ingest

Licensed sports-data ingest for the offload-intelligence pipeline. It runs in
two modes against the same code path:

- **production** — live ingest from **API-Sports**, paced by the tier in a
  cryptographically signed licence
- **simulation** — the same document shapes generated locally, spending no
  upstream quota, for round-the-clock load testing

The message value on Kafka is the provider's document verbatim — no envelope of
ours around it. Routing metadata travels in Kafka headers instead, so a consumer
can deserialize with a schema generated from the provider's own models.

```
value    the provider document (fixture{}, game{}, Leaderboard, Scorecard, …)
key      the provider's fixture id — one partition per game, ordered
headers  sport, feed, endpoint, model, sequence, fixture
```

**13 sports, 29 endpoints, 4 provider adapters, 2 vendor accounts.** API-Sports
carries ten of the sports; cricket, tennis and golf have no host there and are
served over RapidAPI.

## Layout

```
cmd/loadtest/        CLI: flags, wiring, dual-mode execution, signal handling
cmd/licensetool/     keygen, sign, verify, fingerprint (build-system side)
cmd/schematool/      capture, compare shapes, validate routes, infer models

pkg/licensing/       Ed25519 licence verification, tiers, hardware fingerprint
pkg/ingest/          the licensed pipeline
  apisports/         unified client for the 12 API-Sports hosts
  ratelimit.go       per-host token buckets sized from the licence tier
  crowd.go           audience-weighted budget allocator
  usage.go           daily/monthly quota tracking and back-off thresholds
  schedule.go        state-aware adaptive cadence
  sweep.go           bulk fetching and state derivation
  streamer.go        the DataStreamer seam: simulation vs production
  runtime.go         assembly — licence -> tier -> limiter -> client -> streamer
pkg/metrics/         counters, gauges, one-hour series, histograms,
                     host sampling and the Prometheus exposition
pkg/dds/             the Dashboard Design System, shared by all four products
  assets/            palette, 12-column grid, card component, logo (go:embed)

config/              environment and .env loading — at the module root, not
                     under internal/, so other products can import it

internal/dashboard/  the local operator page and its JSON endpoint
internal/generators/ simulations, including the API-Sports capture replayer
internal/provider/    per-provider clients — golf (live-golf-data) and
                     volleyball (API-Sports)
internal/allscores/  AllScores wire models — tennis
internal/cricbuzz/   Cricbuzz wire models — cricket
internal/poller/     concurrent polling worker pool (simulation)
internal/webhook/    burst emitters and an inbound receiver
internal/producer/   Kafka client plus discard/stdout/fan-out sinks
deployments/         Dockerfile and the linux/amd64 Compose stack
```

The assembly order in `runtime.go` is the load-bearing part: nothing downstream
can widen what the licence granted, because each stage only receives what the
previous one produced. The client cannot call a vertical the limiter does not
know about; the limiter only knows verticals the entitlement resolved; the
entitlement comes from verified claims. There is no path from a config file to
more throughput.

Wire models live in a package per provider because the conventions genuinely
differ, and forcing them into one house style would mean the payloads no longer
match any wire.

## Sports and providers

`nfl`, `ncaaf`, `ncaab`, `nba`, `soccer`, `afl`, `rugby`, `cricket`, `tennis`,
`golf`, `ufc`, `mma`, `f1`.

| Provider | Endpoints | Sports |
| --- | --- | --- |
| **`apisports`** | 20 | soccer, nfl, ncaaf, ncaab, nba, afl, rugby, ufc, mma, f1 |
| `cricbuzz` | 3 | cricket |
| `allscores` | 3 | tennis |
| `livegolf` | 3 | golf |

### Why four providers and not one

The brief was 100% reliance on API-Sports. It covers ten of the fourteen sports
and, usefully, **closes the AFL gap** that no previous provider could. But it
has no host for **cricket, tennis or golf** — verified by probing every
plausible hostname, not by reading the documentation:

```
v1.afl.api-sports.io          200    ← the gap that was open all project
v1.formula-1.api-sports.io    200
v1.tennis.api-sports.io       NO SUCH HOST
v1.golf.api-sports.io         NO SUCH HOST
v1.cricket.api-sports.io      NO SUCH HOST
v1.nascar.api-sports.io       NO SUCH HOST
```

Consolidating those three onto API-Sports would have meant deleting three
working, capture-verified sports. They keep their providers; everything
API-Sports can serve moved to it.

Motorsport is **Formula 1 on API-Sports**, and only that. NASCAR was retired
rather than kept on a fifth vendor: API-Sports sells no NASCAR host at any
spelling, so carrying it meant carrying SportsDataIO — an entire provider,
its models and its key — for one sport. The category is served by F1.

### One API, twelve independently-metered hosts

API-Sports is not one service. Each sport is a separate host —
`v3.football.api-sports.io`, `v1.basketball.api-sports.io` — with **its own
quota**. One key authenticates all of them. A free venue polling six sports has
600 requests/day in total, not 100.

Treating the quota as global would leave five sixths of it unspent; treating it
as unlimited would throttle every sport at once. The limiter, the usage tracker
and the budget allocator all key on the host.

## Feed kinds

A sport is not one stream. Providers split a live event across endpoints
with very different shapes, sizes and update rates, and the pipeline has to
survive all of them at once:

| Kind | What it is | Typical size | Emitter |
| --- | --- | --- | --- |
| `boxscore` | Whole-event snapshot: game, line score, team and player stat lines | 5–40 KB | poller |
| `playbyplay` | The event timeline, append-only, strictly ordered | 3–170 KB | poller |
| `playerstats` | Flat array of per-player stat lines | 1–40 KB | poller |
| `telemetry` | One lap, hole, point or delivery | 0.2–1.3 KB | webhook bursts |

Event-driven feeds stay quiet when nothing happens rather than republishing the
last record. A soccer incident feed emits a goal, card or substitution exactly
once — byte-identical duplicates on a topic are indistinguishable from a replay,
so a renderer that has nothing new advances the clock instead.

That 186 B → 168 KB spread is the point: it is what stresses producer batching,
consumer deserialization budgets and backpressure. `loadtest -endpoints` prints
the full catalog.

Several feeds carry part of a response rather than a whole one: the per-driver
the leaderboard rows inside a golf tournament, the incident arrays inside a soccer box
score. Those are described by an explicit **projection** — a JSON path into the
response — rather than by inventing a route. `Path` is what a consumer would
call; `Projection` is what it would then select.

## Licensing

The binary will not run without a valid licence. The model is offline-first: a
venue appliance often sits behind a restrictive network, so there is no call
home — everything needed to decide whether this process may run is inside
`license.key`, signed with Ed25519 by a key that never leaves the build system.

What the licence decides:

| Claim | Enforces |
| --- | --- |
| `tenant_id` | who the venue is; stamped on every message |
| `allowed_products` | that this binary is covered, not just something in the suite |
| `fingerprints` | which machines may run it (SHA-256 of platform machine id + physical MACs) |
| `sports`, `regions` | what content is entitled |
| `tier` | **the API throughput the rate limiter is sized from** |
| `expires_at`, `grace_days` | when it stops |

That last one is why licensing is not a bolt-on. The API quota is the scarcest
resource in the system, and the tier in the licence is what tells the limiter
how much of it exists. A free venue and a custom-contract venue run the same
binary; the licence is the difference.

```bash
make keygen                      # once, on the build system
make license TIER=free           # issue, pinned to this machine
make verify-license              # check it the way the binary does
```

A background ticker re-verifies every 24 hours and calls `os.Exit(1)` the moment
the licence stops holding. That is blunt on purpose: this process publishes
licensed data into a venue's Kafka cluster, and continuing to publish on a
licence that no longer verifies is the one outcome licensing exists to prevent.

**Seven-day offline grace.** A renewal can be delayed by a purchase order or a
network the venue does not control; cutting a live site off at the stroke of
midnight turns a billing problem into an outage. Seven days survives a weekend
and a business week, and is too short to use as a free month. The grace is
inside the signed body, so a venue cannot extend its own.

Signing covers a **canonical** encoding of the claims — keys sorted, no
insignificant whitespace — so a licence that a support tool has round-tripped
through a different JSON writer still verifies. The `alg` field is checked
against a constant before the key is touched; the "alg: none" lesson from JWT
applies here too.

### Tiers

| Tier | Per minute | Per day, per host | Source |
| --- | --- | --- | --- |
| `free` | 10 | 100 | **verified live** |
| `pro` | 300 | 7,500 | transcribed from pricing |
| `ultra` | 450 | 75,000 | transcribed from pricing |
| `mega` | 900 | 150,000 | transcribed from pricing |
| `custom` | must state its own | must state its own | negotiated |

Only `free` has been exercised against a live key, and the table says so. A
licence may *tighten* a published plan but never loosen it — an issuer wanting
more has to say `custom` and own it, which keeps an over-generous number from
riding in on a typo. And the transcribed rows are safe to ship because the
provider's own response headers override them at runtime.

## Rate limiting is driven by the licence

```
licence tier  ->  per-host budget  ->  crowd-weighted share  ->  achievable cadence
```

Derived in that direction, never the reverse. A scheduler that picks a cadence
first and hopes the budget covers it will exhaust the day's quota before the
evening kick-off and go dark exactly when the venue cares most.

**The headers say what the tier cannot.** The real names, confirmed live, are
not what the documentation implies:

```
x-ratelimit-limit                 per-MINUTE ceiling      (10 on free)
x-ratelimit-remaining             per-MINUTE remaining
x-ratelimit-requests-limit        per-DAY ceiling         (100 on free)
x-ratelimit-requests-remaining    per-DAY remaining
```

The unqualified pair is the *minute* window; the `requests`-qualified pair is
the *day*. Reading `x-ratelimit-remaining` as the daily figure — the obvious
misreading — would have the limiter believe it has 9 requests left for the day
when it has 99, and throttle a venue to almost nothing.

**Crowd weighting.** A bar with forty people watching the NFL and nobody
watching handball should not spend equal quota on each. Weight is
`base × live × engagement`: configured venue interest, multiplied by whether
anything is actually in play (log-shaped, so the first live game matters more
than the tenth), multiplied by an optional real-time audience signal. An
engagement reading older than 20 minutes is discarded — a stale crowd signal is
worse than none, because it is confidently wrong.

**Usage tracking** meters reservations, not completions, because a burst of
in-flight requests would otherwise overshoot the ceiling before any returned. A
call that never reached the provider hands its reservation back. At 75% of a
day's budget the tracker warns; at 90% it stands down everything except live
action, holding the last tenth for a surge — a game going to overtime arriving
at an empty bucket is the failure this prevents.

**429s** are retried with exponential backoff and full jitter, capped at four
attempts. Jitter matters more than usual: every vertical shares one key and one
minute window, so without it a dozen throttled workers would wake at the same
instant and re-throttle each other forever. A 429 also halves the bucket
immediately rather than waiting for the next header to suggest it.

## Adaptive polling

Ingestion is **bulk**: one request per sport per cycle, not one per fixture. A
Saturday card of 40 soccer fixtures cost 40 requests per cycle under the old
per-game loop and costs 1 now.

`live=all` is **not universal** — that assumption did not survive contact:

| Verticals | Bulk sweep |
| --- | --- |
| football, american-football, nba | `?live=all` |
| basketball, baseball, hockey, rugby, afl, volleyball, handball, mma | `?date=YYYY-MM-DD` (whole card, filtered locally) |
| formula-1 | `?season=YYYY` |

The rest reject the parameter outright: `{"live": "The Live field do not exist."}`.

State comes free from the sweep — the response says how many fixtures are live,
at a break, upcoming or finished — so knowing what to do next costs no extra
request.

| State | Target cadence |
| --- | --- |
| Live action | 5–10s |
| Half-time / intermission | 2–3 min |
| Pre-game | 10–15 min |
| Final | 10–15 min |

### The target is not always affordable, and the system says so

**On the free tier, 5-second live polling is arithmetically impossible.** 100
requests/day against a three-hour window is one request every ~2 minutes; the
requested cadence is 86–172× the entire daily budget for one sport.

So target and affordable are kept as separate numbers. The scheduler computes
`seconds_left_in_day / requests_left` and polls at whichever is slower, and the
dashboard shows both:

```
football     live     every 12s (want 8s)   budget-limited: 85 requests left for 34m of the day
basketball   live     every 16s (want 8s)   budget-limited: 74 requests left for 34m of the day
afl          idle     every 30m
```

A venue on a custom tier reaches the 8-second target; a free venue does not, and
finds out from the dashboard rather than from a feed that went silent at 19:00.

**Free-plan limits worth knowing** (discovered by running against it, not from
the docs): the date window is **±1 day only**, so historical backfill is
impossible, and Formula 1 is restricted to **seasons 2022–2024**. A vertical the
plan does not cover stands down until the next day rather than retrying on
cadence — that error is permanent, and rediscovering it every cycle spends real
quota to be told the same thing.

## Monitoring: the Dashboard Design System

The operator dashboard is built on **`pkg/dds`**, the Offload Intelligence
Dashboard Design System, shared with LiveMesh, Relic and Atmos. Four
independently-styled dashboards is how a suite stops looking like one product,
so the palette, the twelve-column grid, the card anatomy and the alert
behaviour live in one versioned package. This product supplies only the numbers.

```bash
./bin/loadtest -mode production -dashboard-addr :8090 -metrics-addr :9102
```

| Surface | Purpose |
| --- | --- |
| `:8090/` | the operator page |
| `:8090/api/state` | everything the page shows, as JSON |
| `:8090/healthz` | 503 when the licence is invalid |
| `:9102/metrics` | Prometheus exposition |

**Port 9102, not 9090.** 9090 is Prometheus *server's* own default; an appliance
running Prometheus locally would have the two fight over the bind. 9100 is
node_exporter's, so the exporter range's next free slot is used. Override with
`-metrics-addr`.

### Card anatomy

Every metric card carries the same four things — title, current value, a
one-hour sparkline, and a health lamp — so an operator reads any card in any
product the same way. A card whose threshold is crossed pulses; the pulse is
guarded by `prefers-reduced-motion`, and the border colour carries the same
information for a viewer who has asked for less movement. Motion draws the eye,
it is never the only signal.

Thresholds are DDS-wide, not per product: **latency > 2s** and
**error rate > 5%**. An operator learns that a pulsing card means the same thing
in Ingest as it does in Atmos, and that only holds if the rule is defined once.

### The Golden Signals

| Signal | What it is |
| --- | --- |
| Throughput | messages/minute, per sport |
| Latency | poll-to-Kafka delta, p95 |
| Real-time fidelity | three components — see below |
| Error rate | by provider and by 4xx / 5xx / transport class |
| Provider health | all 14 sports in the sidebar |
| Kafka partition balance | writes per partition, hot-partition detection |
| Edge resources | Minisforum CPU, memory, load, plus process RSS |
| Flink state buffer | optional; see below |

Errors are split by class because 4xx and 5xx mean opposite things: a 4xx is our
request being wrong, a 5xx is the provider failing, and a transport fault never
reached them at all. Folding them together makes an outage and a bad parameter
look identical. A 429 is not an error — being throttled is the limiter working.

### Real-time fidelity is three numbers, not one

The obvious definition — *current system time minus API payload timestamp* —
does not measure freshness with this provider, and shipping it would have put an
alarming number on a healthy dashboard.

API-Sports sends `timestamp` as the fixture's **scheduled kickoff**, not an
update time. A live match with `timestamp = 22:30`, `status = 1H`,
`elapsed = 44` yields `now − timestamp = 44 minutes`. That is match elapsed
time. For a finished fixture it is hours; for tomorrow's card it is negative.
The provider sends no per-record update stamp, so the literal metric is not
implementable as intended.

What is measured instead, each with one clear meaning:

| Component | Definition |
| --- | --- |
| **Ingest age** | `now − fetch time`. Always valid; the true staleness signal. |
| **Provider clock skew** | provider `Date` header − local clock. Catches a drifting appliance clock, which silently corrupts every other time-based metric and every Flink event-time window. |
| **Live-match lag** | `(wall clock − kickoff) − reported elapsed`. How far behind live play the provider's data is. |

Live-match lag counts **first-half fixtures only**. `elapsed` does not include
the half-time interval, so a second-half fixture would read roughly fifteen
minutes of false lag. Rather than apply a fudge factor for a break whose length
varies, the smaller honest sample is used, reported as a median so one stale
fixture cannot drag a card of forty healthy ones.

### The Flink state gauge is optional, and off by default

offload-ingest is the **producer**. Flink is a separate process — a different
job, usually a different host, and in this suite a different product. One
process cannot measure another's state buffer, so this is not a native metric
here.

By default the card says so and names where the metric belongs, rather than
showing a confident zero for something it cannot see. `-flink-addr` enables a
scraper against Flink's own REST API for venues that want the number surfaced on
the ingest box anyway; every figure is then Flink's, not ours.

### Edge resources

Read from `/proc/stat` and `/proc/meminfo` on the Linux deployment target and
via `sysctl`/`vm_stat` on macOS for development — behind an interface, with no
new dependency. `MemAvailable` is used rather than `MemFree`, because page cache
is reclaimable and counting it as used makes every healthy Linux box look full.

Process RSS and goroutine count are recorded even when the host sampler cannot
read the platform: "our process is leaking" is a distinct and more common
failure than "the box is out of memory".

### Partition balance

The balancer hashes the fixture id, so writes-per-partition is measurable
client-side and detects hot partitions caused by our keying. Broker-side
consumer *lag* needs the Admin API and is out of scope.

In a dry run there is no broker, but the question is still decidable offline
because the balancer is a pure function of the key — the skew is projected
against an assumed topic width and labelled as projected, so a venue can find a
hot-partition problem before deploying. Skew is suppressed until at least ten
writes per partition; sixteen messages across five partitions is uneven by
arithmetic, not by keying.

## Schema provenance

Not every feed is equally evidenced, and the catalog says so rather than
flattening the difference into a single "verified" bit. `loadtest -endpoints`
prints the tier per endpoint, and a test fails if a claim outruns the evidence
on disk.

| Tier | Meaning | Count |
| --- | --- | --- |
| `captured` | Diffed against a real provider response. **The only tier that is proof.** | 20 |
| `modeled` | Shape follows the family, but no capture backs it. | 14 |

The distinction is not academic. Earlier in this project a flag treated
"documented" as "verified", and captures showed that was wrong: NCAA
data-dictionary pages described **37–49%** of what the API really returned, and
an OpenAPI spec declared **14 columns the live endpoint never sends**.

**`modeled` (14)** — the API-Sports verticals whose capture came back empty
because the sport was out of season on the capture date, or the free plan does
not cover the window: NFL, NCAAF, NBA, AFL, rugby, UFC and MMA. The route is
verified callable; the *document shape* is not yet evidenced. An empty card
proves the endpoint works and nothing more, and the provenance test enforces
exactly that distinction — a capture with `results: 0` cannot promote a feed to
`captured`. Re-run `make capture` in season and they promote themselves.

### Simulation carries production's shape, by construction

This is the reason the consolidation touched the generators at all. When
production moved to API-Sports, a simulation still emitting SportsDataIO-shaped
documents would have been load-testing a pipeline nobody was going to run.

So the API-Sports generators do not invent a shape: they load a **real captured
response** and evolve it — the clock advances, scores move, statuses walk from
`NS` through `1H`, `HT`, `2H` to `FT`, and the fixture rolls over. The document
shape is authentic because it came off the wire.

Two families, reproduced rather than normalised away:

| Family | Sports | Shape |
| --- | --- | --- |
| fixture | soccer | nested `fixture{}`, `league{}`, `teams{}`, `goals{}`, `score{}` |
| games | everything else | flat `id`/`date`/`status`/`league`/`teams`/`scores` |

Details that survive because they are copied rather than cleaned up: `elapsed`
is `null` before kick-off and after the whistle, not `0` — a consumer averaging
it would silently fold those zeroes in. Score sub-shapes differ per vertical
(basketball reports per-quarter columns, baseball a per-inning map, hockey a
plain integer), so totals are written into whichever structure is already there.

## Provider quirks reproduced on purpose

These look like bugs and are not. Each is pinned by a test, because a consumer
that assumes one shape fits all will break on the others.

**API-Sports answers a rejected request with HTTP 200** and the reason inside
`errors`. A client that trusts the status code reports success and an empty
result forever. Worse, the field is not a stable type: success sends
`"errors": []` and failure `"errors": {"live": "..."}`. Both observed live,
which is why it is decoded by hand. The capturer had this bug briefly and wrote
250-byte error documents to disk labelled "ok" — now it checks the envelope and
refuses to save one.

**Quota is metered per host, not per account.** Six sports on the free plan is
600 requests/day, not 100.

**`live=all` exists on three verticals out of twelve.** See
[Adaptive polling](#adaptive-polling).

**Free-plan windows are ±1 day**, and Formula 1 only serves seasons 2022–2024.

Away from API-Sports the conventions differ again, and those differences are
reproduced too. Several providers use zone-less US Eastern timestamps
(`2026-08-30T13:05:00`) with `Utc`-suffixed variants carrying the `Z`, and
nullable scalars are pointers so a real JSON `null` round-trips and "no score
yet" stays distinguishable from "a score of zero". AllScores omits optional
fields entirely rather than sending them null, so one struct covers a tennis tie
and a league fixture by way of `omitempty`, and it sends every per-player
statistic as a string — `"0.01"` for expected assists included. Cricbuzz returns
overs, economy and strike rate as display strings.

**Betting content is excluded on purpose.** AllScores hangs bookmaker material
off `promotedPredictions` and `relatedLines`. None of it is modelled, and all of
it is stripped from the captured side of the schema comparison too, so the
exclusion reads as a decision rather than a permanent gap. The provider does
carry prediction data if it is ever wanted.

## Configuration

Credentials come from the environment. A local `.env` is loaded at startup via
`godotenv` (`config`), searching the working directory and its parents
so the binary behaves the same from the module root or from `cmd/loadtest`.

```
APISPORTS_KEY=...                # the primary provider
OFFLOAD_MODE=simulation          # or production
OFFLOAD_LICENSE_PATH=license.key
OFFLOAD_LICENSE_PUBKEY=...       # development; a release build embeds it

RAPIDAPI_KEY=...                 # cricket and tennis
GOLF_API_KEY=...                 # golf; falls back to RAPIDAPI_KEY
```

`OFFLOAD_MODE` defaults to **simulation**: an operator who mis-types it gets a
load test, not an unplanned run against a live metered API.

A release build embeds the licence public key, so a tampered binary is the only
way around it:

```bash
go build -ldflags "-X github.com/offloadintelligence/offload-ingest/pkg/licensing.publicKeyB64=$(cat keys/license.pub)" ./cmd/loadtest
```

A build with no key verifies **nothing** and says so, rather than accepting
every licence — that is the direction this has to fail in.

Real environment variables always win over the file, so a container's injected
credentials take precedence over a stray `.env` in an image. A missing file is
not an error. `.env` is gitignored; commit `.env.example` instead. The key is
never logged — only a redacted fingerprint at debug level.

## Schema verification against the live API

`fixtures/<provider>/` holds real responses captured from each upstream, and
`schematool schemas` diffs every generated payload against them. The current
result is in `docs/schema-comparison.txt`: **all 20 bound feeds cover 100% of
the real response's JSON paths.** The other 14 have no capture bound, and the
comparison says so rather than scoring them.

Coverage is measured as paths missing from ours, not as an exact set match. Our
side is the union over 1,500 simulated ticks, so it legitimately carries
conditional fields one real match happens not to have — a tiebreak column, a
suspended player, a televised fixture. A path we *lack* is a modelling gap; a
path we have and the capture does not is usually a condition that capture did
not meet. Both are printed, and `-paths` prints them in full rather than by
leaf name when the short form is ambiguous.

Running these against a live key has repeatedly corrected things the published
documentation got wrong. A sample from this project:

| Finding | Detail |
| --- | --- |
| API-Sports returns 200 on rejected requests | The reason is inside `errors`; the status code says nothing. |
| `live=all` works on 3 verticals of 12 | The rest answer `The Live field do not exist.` |
| Rate-limit header names are inverted from the obvious reading | `x-ratelimit-remaining` is the minute window; the day is `x-ratelimit-requests-remaining`. |
| Free plans serve a ±1 day window | Historical backfill is impossible; F1 is capped at 2022–2024. |
| NCAA models were badly incomplete | Data-dictionary pages described 37–49% of what the API returned. |
| An OpenAPI spec over-declared by 14 columns | The live endpoint never sent them. |

Three commands, all in Go (`cmd/schematool`), all provider-aware:

```bash
make capture            # refresh the captures from every provider
make verify-feeds       # both checks below
make compare-schemas    # payload SHAPE vs the captured responses
make validate-routes    # every route template called against the live API
```

`make capture` is discovery-driven: it fetches a schedule, picks a completed
fixture out of it and follows that id into the box score, so refreshing does not
depend on hand-maintained game ids going stale. That step is worth having
end-to-end rather than trusting: re-running it after the soccer switch found the
AllScores capture had never once followed a fixture into its match document.
Two reasons, both silent — the window ended today, so the card came back
unplayed, and Go's reference layout treats the `2` in a literal `"%2F"` as the
day of the month, so `"02%2F01%2F2006"` rendered as `"29%29F08%29F2026"` and the
API ignored the range. It also reports finished matches as `Ended`, not
`Final`.

Onboarding a provider needs no throwaway scripts either — `schematool infer`
generates Go structs from a capture, which is how the Cricbuzz models were
produced:

```bash
go run ./cmd/schematool infer -file cricbuzz/scorecard.json \
    -path scorecard.batsman -type Batsman
```

Both are implemented in `cmd/schematool`, in Go — not scripts. The schema check
calls `generators.Endpoints()` and runs each feed in-process rather than
shelling out to the CLI and parsing its columns, which is how an earlier version
worked and was quietly fragile.

**Both are needed.** The schema comparison diffs payloads against captures
fetched with correct URLs, so it cannot see a wrong route. Route validation
found 8 broken endpoints on its first run that the shape comparison had passed
at 100% — wrong path prefixes, and two routes that did not exist at all and were
really projections of another document.

On API-Sports it also has to look *inside* the body, because a rejected request
still returns 200. A route that answers with a `plan` or parameter error is
reported as such rather than counted as healthy.

Current state: **17 of 17 routes return 200.** Formula 1 is flagged separately —
it is callable, but the free plan does not cover the current season, which is a
licensing boundary rather than a broken route.

## Quick start

```bash
make build
make keygen                                  # once: the Ed25519 signing pair
make license TIER=free                       # issue, pinned to this machine
make verify-license

./bin/loadtest -endpoints                    # the catalog, with provenance
```

**Simulation** — generated payloads, no upstream call, no quota spent:

```bash
make simulation                              # dashboard on :8090
```

**Production** — live API-Sports ingest, paced by the licence tier:

```bash
make production                              # dashboard :8090, metrics :9102
```

Against a real broker, split by sport and feed kind so a large bulk sweep and a
terse status row do not share a topic:

```bash
./bin/loadtest -mode production \
  -brokers kafka-1:9092,kafka-2:9092 \
  -topic ingest -topic-per-sport -topic-per-feed \
  -dashboard-addr :8090 -duration 10m
```

`-no-license` runs the generators without a signed key for development. It
refuses production: the licence is what carries the tier the limiter is sized
from, and there is no sensible default for "how hard may this venue hit a paid
API".

`loadtest -h` lists every flag.

## Deploying with Docker Compose

The appliance ships as one image. `deployments/` carries the Dockerfile, the
Compose stack and the Prometheus configuration.

### Quick start

**Simulation** — the default. Runs on a fresh clone with an empty environment:
no API key, no licence, no `.env`.

```bash
cd deployments
docker compose up -d                    # kafka + kafka-ui + loadtest generator
```

**Production** — the licensed pipeline against live providers. It is a separate
overlay file, applied on top:

```bash
cp ../.env.example .env                 # then fill in the keys below
docker compose -f docker-compose.yml -f docker-compose.production.yml up -d
```

That adds two containers to the three above: `offload-ingest` itself, and a
local Prometheus on `:9090` scraping the appliance on `:9102`.

### Why two files rather than one file with profiles

Starting live ingest spends metered quota against a real API key, so it must be
impossible to start by accident — and simulation must need nothing, or the
"safe default" is only a claim in a README.

A `production` profile inside one file cannot deliver both. **Compose
interpolates every variable in a file before it applies profiles**, so the
`${APISPORTS_KEY:?...}` guard on the live service fires on a plain
`docker compose up -d` too, even though that command was never going to start
the guarded service. The simulation stack then could not launch on a clean
clone, which is the opposite of what was intended. Splitting the files is what
makes both properties hold at once, and CI asserts each of them.

Stop everything, keeping the Kafka and Prometheus volumes:

```bash
docker compose -f docker-compose.yml -f docker-compose.production.yml down
```

### Required environment variables

Read from the host environment or a `.env` beside the Compose file. Nothing is
baked into the image — the image is identical on every appliance, and the
licence is what differs.

| Variable | Required | Purpose |
| --- | --- | --- |
| `APISPORTS_KEY` | **yes** | The primary provider. Compose refuses to start without it. |
| `OFFLOAD_LICENSE_FILE` | **yes** | Host path to the signed licence, mounted read-only at `/etc/offload/license.key`. Defaults to `./license.key`. |
| `OFFLOAD_LICENSE_PUBKEY` | dev only | Verification key. A release build embeds it; set this only for development builds. |
| `RAPIDAPI_KEY` | for cricket/tennis | One key covers Cricbuzz and AllScores. |
| `GOLF_API_KEY` | for golf | live-golf-data is RapidAPI-hosted; falls back to `RAPIDAPI_KEY` when unset. |
| `VENUE_ID` | recommended | Stamped onto every Prometheus series as the `venue` label. Unset means an unlabelled venue that a fleet-wide Grafana cannot separate from any other. |
| `OFFLOAD_MODE` | no | `production` is set by the Compose service. Defaults to `simulation` everywhere else, so a mistyped value gets a load test rather than an unplanned run against a metered API. |
| `VERSION`, `COMMIT`, `BUILD_DATE` | no | Stamped into the binary at build time. |

`.env` is gitignored and `.env.example` documents every variable including the
optional ones.

### How to verify

**1. Is the appliance producing data?**

```bash
curl -sS -o /dev/null -w '%{http_code}\n' http://localhost:9102/health
```

`200` means at least one sport has published inside the fifteen-minute window
*and* no provider is serving a rate-limit hard floor. `503` means one of those
is false, and the body says which:

```bash
curl -sS http://localhost:9102/health | jq
```

```json
{
  "ok": false,
  "status": "data_starved",
  "detail": "no data for 1820s, window is 900s (last: soccer)",
  "last_poll": "2026-09-02T21:38:11Z",
  "poll_age_seconds": 44.2,
  "rate_limited": false
}
```

`status` is a closed set — `ok`, `starting`, `data_starved`, `rate_limited` —
so an alerting rule can match on it without breaking when the prose improves.

Read `last_poll` alongside `last_data`. A box whose polls are current but whose
data is stale is **quiet** — nothing licensed is playing, which at 04:00 is the
overwhelmingly likely case. A box whose polls are *also* stale is **broken**.
Suppress on the first, page on the second; the appliance cannot tell them apart
on its own, because the fixture calendar that would settle it is upstream data
it does not hold.

The same probe is served on the dashboard at `http://localhost:8090/health`.
`/healthz` is the separate *liveness* check — it answers only "is the process up
and licensed", which is what an orchestrator should restart on. Restarting a
data-starved appliance does not make a provider start answering.

**2. Are the metrics being scraped?**

```bash
curl -sS http://localhost:9102/metrics | grep '^offload_ingest_messages_total'
```

Then confirm Prometheus agrees, rather than trusting that a config file means a
working scrape:

```bash
curl -sS 'http://localhost:9090/api/v1/query?query=up{job="offload-ingest"}' | jq '.data.result'
```

`value` of `1` is a healthy scrape. `0` means Prometheus can reach the port but
not parse it; no result at all means the target was never configured.

Useful series once data is flowing:

| Series | Reads |
| --- | --- |
| `offload_ingest_provider_mode{provider=}` | `1` live against the vendor, `0` simulated. `sum()` is how many feeds are spending quota. |
| `offload_ingest_messages_total` | Published downstream. Flat is the starvation signal. |
| `offload_ingest_sport_messages_total{sport=}` | Which feed went quiet. |
| `offload_ingest_dropped_records_total{sport,reason}` | Scope enforcement. A rising `out_of_scope` means a licence mismatch. |
| `offload_ingest_golf_throttled` | `1` while the 429 hard floor holds. |
| `offload_ingest_golf_cadence_minutes` | Golf's current polling interval. |

**3. Which providers are actually live?**

```bash
curl -sS http://localhost:9102/metrics | grep provider_mode
```

```
offload_ingest_provider_mode{provider="allscores"} 0
offload_ingest_provider_mode{provider="apisports"} 1
offload_ingest_provider_mode{provider="cricbuzz"} 0
offload_ingest_provider_mode{provider="livegolf"} 1
```

`1` means that provider is contacting its vendor and spending real quota. `0`
means it is not — but check *which kind* of zero: in simulation mode a `0`
provider still generates data locally, whereas `cricbuzz` and `allscores` have
no live client at all and therefore emit **nothing** in production mode.

The gauge reports **what the runtime assembled, not what the environment was
configured with**, because those come apart routinely:

- In simulation mode every provider is `0` even with every key present. Nothing
  is being spent, and reporting otherwise would say quota is going out when it
  is not.
- `cricbuzz` and `allscores` are `0` in *every* mode, key or no key. Neither
  package contains an HTTP client at all — only wire models — so there is
  nothing to go live with. `RAPIDAPI_KEY` is set because the schema tooling
  uses it, so a configuration-derived check would wrongly call them live. In
  production these two sports emit no records whatsoever.
- `livegolf` is `0` when the licence does not entitle golf, which is a
  commercial fact rather than a deployment mistake.

A missing `APISPORTS_KEY` does **not** silently degrade to `0`. Production mode
refuses to start at all without it (`production mode needs an API-Sports key`),
so a venue cannot quietly end up serving simulated data believing it is live.

**3. Is the container healthcheck wired?**

```bash
docker inspect --format '{{.State.Health.Status}}' offload-ingest
```

The runtime image is distroless — no shell, no curl — so the healthcheck runs
the shipped binary's own probe (`loadtest -health-check=<url>`) rather than
fattening the image or dropping the check. `docker inspect` also keeps the
failure log, which is often the only forensic record of an appliance that spent
the night restarting.

Note the container is unhealthy while data-starved, by design. If you would
rather a starved appliance not be restarted by an orchestrator, keep the
readiness probe on `/health` and point the *liveness* probe at `/healthz`.

## Polling a raw endpoint

`-mode production` is the supported path to live data. For one-off inspection of
an arbitrary URL, the old worker pool still accepts fully-formed endpoints and
forwards the response opaquely:

```bash
APISPORTS_KEY=... ./bin/loadtest \
  -poll-endpoints 'https://v3.football.api-sports.io/fixtures?live=all' \
  -poll-endpoint-sport soccer -poll-endpoint-kind boxscore
```

Note this bypasses the licence-driven limiter, so it spends quota unmetered. Use
it to look at a payload, not to run a venue.

## Tuning the load

| Knob | Effect |
| --- | --- |
| `-poll-workers` | Concurrent pollers, and the partition-key cardinality |
| `-poll-kinds` / `-burst-kinds` | Which feed kinds each emitter carries |
| `-poll-interval` / `-poll-jitter` | Steady-state rate; jitter prevents a thundering herd |
| `-burst-emitters` / `-burst-size` | Burst concurrency and depth |
| `-in-burst-delay` | `0` sends a burst as one batch; non-zero drips it |
| `-async` | Fire-and-forget writes: much faster, errors reported asynchronously |
| `-acks` | `0` none, `1` leader, `-1` all — the main durability/throughput lever |
| `-seed` | Reuse a seed to replay a run byte for byte |
| `-captured-dir` | Replay saved provider responses instead of simulating |

Reproducibility is enforced by a test, with one honest caveat: **the same seed
replays the same simulation byte for byte, but timestamps come from the wall
clock.** Two runs of the same seed differ only in their `Updated` / `DateTime`
fields. That is deliberate — downstream event-time windowing needs real
timestamps — but it means a byte-for-byte diff of two runs must exclude them.
The test pins the clock to make the guarantee testable. Run the inbound direction with `-webhook-addr :8088`, which
also serves `/healthz` and `/stats`.

## Build and test

```bash
make test        # full suite
make test-race   # race detector — the concurrency here is the point
make cover       # coverage.html
make check       # fmt, vet, lint, race suite; run before pushing
```

The suite covers the things that are easy to break silently: every endpoint
renders and marshals; per-fixture sequence numbers never skip; a finished
fixture rolls over instead of going quiet; the same seed replays byte for byte;
routing metadata never leaks into the payload; and each provider quirk in the
table above stays as it is.

## Cross-compiling

`linux/amd64` is the deployment target; the rest of the matrix is for running
the generator from a workstation.

```bash
make build-linux-amd64   # dist/loadtest-linux-amd64
make cross               # linux and darwin amd64/arm64, windows/amd64
make release             # cross + tarballs + SHA256SUMS
make docker-build        # linux/amd64 image via buildx
make docker-push REGISTRY=ghcr.io/offloadintelligence
```

The container build cross-compiles from the builder's native architecture
rather than emulating amd64 under QEMU, so an arm64 laptop builds the amd64
image at full speed.
