# offload-ingest — Roadmap and Technical Debt

## Future Extraction: Shared Library

The `pkg/dds/` package is currently co-located for rapid development. Upon the
start of the second Offload Intelligence product repository, this package must be
refactored into a standalone repository (`randymassafra/offload-dds`) and imported
via Go Modules. Ensure no cross-package dependencies exist that would prevent this
migration.

**Status: migration-ready.** Verified at initialization — `pkg/dds` imports only
the standard library (`embed`, `html/template`, `strings`). It has zero imports
from `internal/`, `pkg/ingest`, `pkg/licensing` or `pkg/metrics`. The dependency
runs one way only: the dashboard imports the design system, never the reverse.

To keep it that way:

- `pkg/dds` renders; it must never model. It takes a `Product`, a sidebar, a body
  and a render function — it knows nothing about sports, licences or quotas.
- Product-specific styling belongs in the product. A rule only one dashboard needs
  is a sign the shared component is wrong, not that the system needs an exception.
- The extraction itself is then: move the directory, `go mod init`, tag, and
  replace the import path. No code changes.

A guard is worth adding before the second product starts — this must print
nothing, and does today:

```bash
go list -deps ./pkg/dds | grep offloadintelligence | grep -v '/pkg/dds$'
```

## Central Observability Strategy

Local dashboards (per-product) serve as edge-flight instruments. A centralized
Grafana instance will be deployed later to scrape all `/metrics` endpoints via
Prometheus for fleet-wide monitoring. All products must expose a
Prometheus-compatible `/metrics` endpoint on port 9102.

**Status: implemented.** `-metrics-addr :9102` serves the exposition format on a
listener separate from the dashboard, because the two have different audiences and
may be firewalled differently. Every metric is prefixed `offload_ingest_` so four
products' series stay distinguishable in one Prometheus server; each product should
claim its own namespace.

Open items for the fleet rollout:

- **Scrape target discovery.** Venue appliances are behind venue networks. Either
  Prometheus reaches in (VPN, tailnet) or the appliance pushes out
  (remote-write, Pushgateway). This is a network decision, not a code one, and it
  gates the whole strategy.
- **Tenant labelling.** Series are not currently labelled by `tenant_id`, so a
  fleet-wide Grafana cannot separate venues. The licence already carries the
  tenant; add it as a constant label before the first venue is onboarded, because
  relabelling historical series afterwards is painful.
- **Exporter library.** The exposition encoder is hand-written (~200 lines,
  test-verified) rather than pulling in `client_golang` and several megabytes of
  transitive dependencies onto an appliance. If the suite standardises on
  `client_golang`, `pkg/metrics/prometheus.go` is the only file that changes.
- **Port 9102, not 9090.** 9090 is Prometheus *server's* default; an appliance
  running a local Prometheus would have the two contend for the bind. 9100 is
  node_exporter's. Keep 9102 consistent across all four products.

## Provider Coverage

- **7 of 20 API-Sports feeds are `modeled`, not `captured`.** NFL, NCAAF, NBA,
  AFL, rugby, UFC and MMA were out of season on the capture date, so their
  captures returned valid but empty cards. The routes are verified callable; the
  document shapes are not yet evidenced. Re-run `make capture` in season and they
  promote themselves — the provenance test enforces that an empty card cannot
  count as proof.
- **The vendor list is closed at two: API-Sports and RapidAPI.** Every sport is
  now served by API-Sports directly, or by a RapidAPI-hosted feed
  (`livegolf` for golf, `cricbuzz` for cricket, `allscores` for tennis). Those
  three share one authentication scheme and one host header, so onboarding
  another is a host name in `upstream()` rather than a new client.
  SportsDataIO was the third scheme and is gone — `internal/sdio/` is deleted,
  along with its models, fixtures and key.
- **Motorsport is Formula 1, and only Formula 1.** NASCAR was retired rather
  than migrated. API-Sports sells no NASCAR host at any spelling, so keeping it
  meant keeping an entire fourth vendor for one sport. Two RapidAPI candidates
  were evaluated and neither closed the gap: `nascar-motorsport-api` wraps
  ESPN's hidden API and is not subscribed; `motorsportapi` is a SofaScore
  wrapper whose race-result endpoints could not be discovered. If NASCAR comes
  back, it comes back as a RapidAPI provider under `internal/provider/`,
  which is now a one-file addition.
- **Formula 1 needs a paid plan.** The free tier serves seasons 2022–2024 only,
  so F1 stands down until the day rolls over. It will work on any paid tier.
  This now gates the entire motorsport category, where it previously only
  gated half of it.
- **Cricket, tennis and golf are production-wired but not API-Sports.** Golf
  runs through the same `ScopeValidator` and `MultiStreamer` fan-in as every
  other sport; cricket and tennis are simulation-only in production mode until
  their streamers are added.
- **The capture-backed tests do not run on a clean clone.** `/fixtures/` is
  gitignored, so `TestCapturedLeaderboardDecodes` and its sibling skip rather
  than fail when the captures are absent — including in any CI that clones
  fresh. They are a local guard today. Fixing this means either committing a
  redacted capture per provider or running `make capture` as a CI step with a
  live key; the first is cheaper and does not spend quota.
- **Golf's payload is rebuilt, not passed through.** It is the one provider
  whose Kafka value is re-marshalled from typed structs rather than forwarded
  as provider bytes, because live-golf-data serves MongoDB extended JSON
  (`{"$numberInt":"18"}`) that would otherwise reach every downstream consumer.
  The cost is that an unmodelled field is silently dropped rather than carried,
  so `TestCapturedLeaderboardKeepsEveryField` fails the build if the capture
  ever contains a field the model does not. Worth revisiting if a second
  provider ever needs the same treatment.

## Production Readiness

Added in the deployment-hardening pass. What is done, and what is still open.

- **Resilience is tested end to end.** `internal/pipeline` assembles the real
  client, limiter, streamer, validator and publisher against a scripted hostile
  provider (malformed JSON, 500, 429, then recovery) and asserts the pipeline
  survives, logs, and republishes. The harness deliberately mirrors
  `cmd/loadtest/production.go` exactly — an earlier version retried after an
  error from `Next`, which made every test pass even with the streamer's
  recovery deleted. A test double more forgiving than production tests nothing.
- **`/health` is a readiness probe, not a liveness one.** 200 only when a sport
  has produced data inside fifteen minutes AND no provider is under a 429 hard
  floor. Served on both `:9102` and the dashboard. `/healthz` remains the
  liveness check. See `pkg/metrics/health.go`.
- **The overnight problem is reported, not solved.** "No data in fifteen
  minutes" is indistinguishable from "nothing licensed is playing" without a
  fixture calendar, which is upstream data the appliance does not hold. Health
  therefore reports `last_poll` beside `last_data` so a monitoring system can
  separate quiet from broken. **An alerting rule still has to be written that
  suppresses on the first and pages on the second** — until it is, a venue that
  alerts on `/health` alone will page every night.
- **The Dockerfile pinned Go 1.24 against a `go 1.25.0` module.** The image
  could not have built at all. Fixed; worth a CI job that builds the image,
  because nothing in `go test` would ever have caught it.
- **The container healthcheck runs the shipped binary.** `loadtest
  -health-check=<url>`, because the distroless runtime has no shell and no
  curl. The alternative was fattening the image or dropping the check.
- **Compose gates production behind a separate overlay file.** `docker compose
  up -d` starts the simulation stack and requires no credentials at all; the
  live pipeline needs `-f docker-compose.yml -f docker-compose.production.yml`,
  so nobody spends a free tier's daily budget by running the wrong command in
  the wrong directory.

  It was a profile first, and that did not work. Compose interpolates every
  variable in a file before applying profiles, so the `${APISPORTS_KEY:?}`
  guard on the profiled live service also broke the unprofiled default `up` —
  the simulation stack could not start on a clean clone. Two files is the only
  arrangement where both hold. CI asserts each half.
- **The simulation stack needed `-no-license`.** Without it the generator exits
  on "this build has no license public key" while Kafka comes up around it, so
  the stack looks healthy and produces nothing. Found by writing the CI check
  that the default stack actually starts rather than merely parses.

Still open:

- **No CI.** Everything above is verified locally. There is no pipeline running
  `go test ./...`, `go vet`, or a Docker build on push, which is what would have
  caught the Go version mismatch.
- **`production.go` exits on any error from `Next`.** Currently unreachable —
  `ProductionStreamer` swallows sweep failures and `MultiStreamer` isolates a
  dead source — but it means any future error path that reaches the top of the
  loop terminates the service rather than degrading it. Worth converting to a
  bounded consecutive-failure tolerance before a fourth provider lands.
- **Provider mode is exported; overall health is not.**
  `offload_ingest_provider_mode{provider=}` reports 1/0 per provider. The
  equivalent for readiness still does not exist.
- **Health is not exported as a metric.** A rule must scrape `/health` or infer
  starvation from `offload_ingest_messages_total` going flat. A
  `offload_ingest_healthy` gauge would let one Prometheus rule cover both.

## Licensing

- **Only the `free` tier is verified against a live key.** The `pro`, `ultra` and
  `mega` ceilings are transcribed from the pricing page. This is safe because the
  provider's own response headers override the table at runtime, but the rows
  should be confirmed when a paid key exists.
- **No revocation list.** A leaked licence is valid until it expires. If that
  becomes a concern, the licence already carries a `license_id` to check against.

## Observability Gaps

- **Flink state buffer belongs to the Flink product.** `offload-ingest` is the
  producer; Flink is a separate process and cannot be measured from here. The card
  ships as "not configured" and names where the metric belongs. `-flink-addr`
  enables an optional REST scraper for venues that want it surfaced locally.
- **Kafka lag is not measured.** Writes-per-partition is measured client-side and
  detects hot partitions caused by our keying. Broker-side *consumer lag* needs
  the Admin API and is not implemented.
- **Host metrics cover Linux and macOS only.** `/proc` on the deployment target,
  `sysctl`/`vm_stat` for development. Any other platform reports unavailable
  rather than a confident zero.

## Known Constraints

- **The free tier cannot fund the target polling cadence.** 100 requests/day/host
  against a three-hour window is one poll every ~2 minutes; the 5–10s live target
  is 86–172× that budget. The scheduler reports target and affordable separately
  and the dashboard shows both, rather than exhausting the day before kick-off.
  This is arithmetic, not a bug — it resolves with a paid tier.
