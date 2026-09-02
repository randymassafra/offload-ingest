package dashboard

import (
	"html/template"

	"github.com/offloadintelligence/offload-ingest/pkg/dds"
)

// renderPage builds the static shell. Every number arrives from /api/state, so
// this is rendered once and reused.
func renderPage(version string) string {
	return dds.Shell(dds.ShellOptions{
		Product:  dds.Product{Name: ProductName, Version: version},
		StateURL: "/api/state",
		Sidebar:  template.HTML(`<div id="sidebar"></div>`),
		Body: template.HTML(`
      <div id="warnings"></div>
      <div class="dds-grid" id="cards"></div>
      <div class="dds-grid" style="margin-top:16px">
        <section class="dds-card dds-col-12">
          <div class="dds-card-head"><h3 class="dds-card-title">API budget and polling cadence</h3></div>
          <table class="dds-table"><thead><tr>
            <th>Vertical</th><th>Daily budget</th><th>Used</th><th>Crowd weight</th>
            <th>Poll every</th><th>State</th><th>Provider headroom</th>
          </tr></thead><tbody id="budgets"></tbody></table>
        </section>
      </div>`),
		Script: template.JS(renderScript),
	})
}

// renderScript is the product's render function. It receives the decoded state
// and drives the shared DDS primitives; it contains no styling of its own.
const renderScript = `
function healthOf(v) { return v || 'unknown'; }

function render(d) {
  DDS.header({
    health: d.health, statusText: d.status, mode: d.mode, updated: d.now,
  });

  /* ---- warnings ---- */
  document.getElementById('warnings').innerHTML =
    (d.warnings || []).map(w =>
      '<div class="dds-banner" data-health="' + DDS.esc(w.health) + '">' +
      DDS.esc(w.text) + '</div>').join('');

  /* ---- sidebar: every sport this build knows about ---- */
  const provs = d.providers || [];
  const live = provs.filter(p => p.live).length;
  document.getElementById('sidebar').innerHTML =
    DDS.sideList('Providers · ' + live + ' live of ' + provs.length,
      provs.map(p => ({
        name: p.sport,
        meta: p.live ? (p.state || '') : 'sim',
        health: healthOf(p.health),
        muted: !p.live,
        note: p.note || (p.provider + ' · ' + (p.state || 'idle')),
      })));

  /* ---- Golden Signal cards ---- */
  const t = d.throughput || {}, l = d.latency || {}, dr = d.drift || {},
        e = d.errors || {}, pt = d.partitions || {}, h = d.host || {},
        f = d.flink || {};

  const cards = [];

  cards.push(DDS.card({
    title: 'Ingestion rate', span: 3,
    value: DDS.num(t.value, 0), unit: t.unit,
    sub: t.sub, health: healthOf(t.health), series: t.series,
  }));

  cards.push(DDS.card({
    title: 'Poll → Kafka latency', span: 3,
    value: DDS.ms(l.value), sub: l.sub,
    health: healthOf(l.health), alert: l.alert, series: l.series,
  }));

  /* Real-time fidelity is three numbers, not one: the headline is pipeline
   * staleness, and provider clock skew and live-match lag sit beneath it. */
  cards.push(DDS.card({
    title: 'Real-time fidelity', span: 3,
    value: DDS.dur(dr.ingest_age_seconds), unit: 'ingest age p95',
    sub: 'clock skew ' + (dr.skew_known ? DDS.dur(Math.abs(dr.provider_skew_seconds)) : '—') +
         ' · match lag ' + (dr.lag_samples > 0 ? DDS.dur(dr.live_match_lag_seconds) : 'n/a'),
    health: healthOf(dr.health), alert: dr.alert, series: dr.series,
  }));

  cards.push(DDS.card({
    title: 'Error rate', span: 3,
    value: DDS.pct(e.rate),
    sub: (e.class_4xx || 0) + ' 4xx · ' + (e.class_5xx || 0) + ' 5xx · ' +
         (e.transport || 0) + ' transport · ' + (e.throttles || 0) + ' throttled',
    health: healthOf(e.health), alert: e.alert, series: e.series,
  }));

  /* Scope enforcement. Present only once something has been dropped — a card
     reading zero on every healthy venue is noise. */
  const drops = d.drops || [];
  if (drops.length) {
    const worst = drops[0];
    cards.push(DDS.card({
      title: 'Scope enforcement', span: 4,
      value: worst.inconclusive ? '—' : DDS.pct(worst.rate),
      unit: worst.inconclusive ? 'sampling' : 'dropped · ' + DDS.esc(worst.sport),
      sub: drops.map(x => DDS.esc(x.sport) + ' ' + x.dropped + '/' +
             (x.dropped + x.published) + (x.inconclusive ? ' (sampling)' : '')).join(' · '),
      health: worst.mismatch ? 'warn' : 'ok',
      alert: worst.mismatch,
    }));
  }

  cards.push(DDS.card({
    title: 'Kafka partition balance', span: 4,
    value: (pt.partition_count && !pt.insufficient) ? DDS.pct(pt.skew) : '—',
    unit: 'skew',
    sub: !pt.partition_count ? 'no partition data yet'
      : pt.insufficient ? 'sampling — too few writes to judge balance yet'
      : (pt.partition_count + ' partitions · hottest is ' + pt.hottest +
         (pt.projected ? ' · projected, no broker attached' : '')),
    health: healthOf(pt.health), alert: pt.alert,
    body: (pt.rows || []).length
      ? '<div class="dds-bar" style="margin-top:6px">' +
        (pt.rows || []).map(r =>
          '<i style="width:' + (r.share * 100).toFixed(1) + '%;display:inline-block" data-health="' +
          (r.share > (1.8 / Math.max(1, pt.partition_count)) ? 'bad' : 'ok') + '"></i>').join('') +
        '</div>' : '',
  }));

  cards.push(DDS.card({
    title: 'Edge host — CPU', span: 2,
    value: h.available ? DDS.num(h.cpu_percent, 0) : '—', unit: '%',
    sub: h.available ? ('load ' + DDS.num(h.load1, 2)) : 'unavailable on this platform',
    health: healthOf(h.cpu_health), alert: h.alert, series: h.cpu_series,
  }));

  cards.push(DDS.card({
    title: 'Edge host — memory', span: 3,
    value: h.available ? DDS.num(h.memory_percent, 0) : '—', unit: '%',
    sub: h.available
      ? (DDS.bytes(h.memory_used_bytes) + ' of ' + DDS.bytes(h.memory_total_bytes) +
         ' · process ' + DDS.bytes(h.process_memory_bytes))
      : ('process ' + DDS.bytes(h.process_memory_bytes) + ' · ' +
         DDS.num(h.goroutines, 0) + ' goroutines'),
    health: healthOf(h.memory_health), alert: h.alert, series: h.memory_series,
  }));

  cards.push(DDS.card({
    title: 'Flink state buffer', span: 3,
    value: f.configured && f.reachable ? DDS.bytes(f.state_bytes) : '—',
    sub: f.configured && f.reachable
      ? ('checkpoint age ' + DDS.dur(f.checkpoint_age_seconds) + ' · ' + DDS.esc(f.note))
      : DDS.esc(f.note || 'not configured'),
    health: healthOf(f.health), alert: f.alert, series: f.series,
  }));

  document.getElementById('cards').innerHTML = cards.join('');

  /* ---- budget table ---- */
  document.getElementById('budgets').innerHTML = (d.budgets || []).map(b => {
    const pct = Math.round((b.pressure || 0) * 100);
    return '<tr>' +
      '<td><b>' + DDS.esc(b.vertical) + '</b></td>' +
      '<td>' + b.budget + ' <span class="dds-muted">req</span></td>' +
      '<td>' + b.used_today + ' <span class="dds-muted">(' + pct + '%)</span>' +
        '<div class="dds-bar" style="margin-top:4px"><i data-health="' +
        DDS.esc(b.health) + '" style="width:' + Math.min(100, pct) + '%"></i></div></td>' +
      '<td>' + ((b.weight || 0) * 100).toFixed(0) + '%</td>' +
      '<td>' + DDS.dur(b.interval_seconds) +
        (b.target_seconds && b.interval_seconds > b.target_seconds + 1
          ? ' <span class="dds-muted">(want ' + DDS.dur(b.target_seconds) + ')</span>' : '') + '</td>' +
      '<td><span class="dds-pill" data-health="' +
        (b.deferring ? 'bad' : b.state === 'live' ? 'ok' : 'unknown') + '">' +
        DDS.esc(b.deferring ? 'deferred' : (b.state || 'idle')) + '</span></td>' +
      '<td class="dds-muted">' + (b.has_api_view
        ? (b.api_day_remaining + '/' + b.api_day_limit + ' today')
        : 'not yet observed') + '</td>' +
    '</tr>';
  }).join('') || '<tr><td colspan="7" class="dds-muted">' +
      'No licensed API-Sports verticals — simulation mode spends no upstream quota.</td></tr>';
}
`
