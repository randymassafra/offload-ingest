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
- **Production is API-Sports-only.** Cricket, tennis, golf and NASCAR have no
  API-Sports host and are simulation-only in production mode. Adding them means
  extending the production streamer to route per provider.
- **Formula 1 needs a paid plan.** The free tier serves seasons 2022–2024 only,
  so F1 stands down until the day rolls over. It will work on any paid tier.

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
