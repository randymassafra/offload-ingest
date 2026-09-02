package dashboard

// indexHTML is the operator page.
//
// Deliberately one file with no dependencies: a venue appliance may have no
// outbound internet beyond the API host, so a CDN reference would render a
// blank page exactly when someone needs it. It polls /api/state every two
// seconds and re-renders; nothing is templated server-side, so the JSON
// endpoint and the page can never disagree.
const indexHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>offload-ingest</title>
<style>
  :root {
    --bg:#0e1116; --panel:#161b22; --line:#2a323d; --fg:#e6edf3;
    --muted:#8b949e; --ok:#3fb950; --warn:#d29922; --bad:#f85149; --accent:#58a6ff;
  }
  @media (prefers-color-scheme: light) {
    :root { --bg:#f6f8fa; --panel:#fff; --line:#d0d7de; --fg:#1f2328;
            --muted:#636c76; --accent:#0969da; }
  }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--fg);
         font:14px/1.5 ui-sans-serif,-apple-system,Segoe UI,Roboto,sans-serif; }
  header { padding:18px 22px; border-bottom:1px solid var(--line);
           display:flex; align-items:baseline; gap:16px; flex-wrap:wrap; }
  h1 { font-size:17px; margin:0; letter-spacing:.2px; }
  .mode { font:600 11px ui-monospace,monospace; padding:3px 9px; border-radius:99px;
          text-transform:uppercase; letter-spacing:.08em; }
  .mode.production { background:rgba(63,185,80,.15); color:var(--ok);
                     border:1px solid rgba(63,185,80,.4); }
  .mode.simulation { background:rgba(210,153,34,.15); color:var(--warn);
                     border:1px solid rgba(210,153,34,.4); }
  .spacer { flex:1; }
  .muted { color:var(--muted); font-size:12px; }
  main { padding:20px 22px; display:grid; gap:18px;
         grid-template-columns:repeat(auto-fit,minmax(330px,1fr)); }
  section { background:var(--panel); border:1px solid var(--line); border-radius:10px;
            padding:16px 18px; }
  section.wide { grid-column:1/-1; }
  h2 { font-size:11px; text-transform:uppercase; letter-spacing:.09em;
       color:var(--muted); margin:0 0 12px; font-weight:600; }
  .kv { display:grid; grid-template-columns:auto 1fr; gap:6px 14px; }
  .kv dt { color:var(--muted); }
  .kv dd { margin:0; font-variant-numeric:tabular-nums; }
  .stats { display:grid; grid-template-columns:repeat(auto-fit,minmax(96px,1fr)); gap:14px; }
  .stat b { display:block; font-size:21px; font-weight:600;
            font-variant-numeric:tabular-nums; }
  .stat span { font-size:11px; color:var(--muted); text-transform:uppercase;
               letter-spacing:.06em; }
  table { width:100%; border-collapse:collapse; font-variant-numeric:tabular-nums; }
  th { text-align:left; font-size:11px; text-transform:uppercase; color:var(--muted);
       font-weight:600; letter-spacing:.05em; padding:0 8px 7px 0; }
  td { padding:7px 8px 7px 0; border-top:1px solid var(--line); }
  .bar { height:6px; border-radius:3px; background:var(--line); overflow:hidden;
         min-width:70px; }
  .bar i { display:block; height:100%; background:var(--accent); }
  .bar i.warn { background:var(--warn); }
  .bar i.bad { background:var(--bad); }
  .pill { font:600 10px ui-monospace,monospace; padding:2px 7px; border-radius:99px;
          border:1px solid var(--line); color:var(--muted); text-transform:uppercase; }
  .pill.live { color:var(--ok); border-color:rgba(63,185,80,.45); }
  .pill.break { color:var(--warn); border-color:rgba(210,153,34,.45); }
  .pill.deferred { color:var(--bad); border-color:rgba(248,81,73,.45); }
  .warn-box { border-left:3px solid var(--warn); background:rgba(210,153,34,.09);
              padding:9px 13px; border-radius:0 6px 6px 0; margin-bottom:8px; }
  .warn-box.bad { border-left-color:var(--bad); background:rgba(248,81,73,.09); }
  .ok { color:var(--ok); } .bad { color:var(--bad); } .warnc { color:var(--warn); }
  code { font:12px ui-monospace,monospace; color:var(--muted); }
</style>
</head>
<body>
<header>
  <h1>offload-ingest</h1>
  <span id="mode" class="mode">—</span>
  <span class="spacer"></span>
  <span class="muted" id="foot">connecting…</span>
</header>
<main>
  <section id="warnings" class="wide" hidden></section>

  <section>
    <h2>Licence</h2>
    <dl class="kv" id="lic"></dl>
  </section>

  <section>
    <h2>Throughput</h2>
    <div class="stats" id="stats"></div>
  </section>

  <section class="wide">
    <h2>API headroom by sport</h2>
    <table><thead><tr>
      <th>Sport</th><th>Daily budget</th><th>Used</th><th>Crowd weight</th>
      <th>Poll every</th><th>State</th><th>API says</th>
    </tr></thead><tbody id="budgets"></tbody></table>
  </section>
</main>
<script>
const fmtDur = s => {
  if (!s || s <= 0) return '—';
  if (s < 60) return s.toFixed(0) + 's';
  if (s < 3600) return (s/60).toFixed(s < 600 ? 1 : 0) + 'm';
  return (s/3600).toFixed(1) + 'h';
};
const esc = s => String(s ?? '').replace(/[&<>"]/g, c =>
  ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

function render(d) {
  const mode = d.mode || 'simulation';
  const m = document.getElementById('mode');
  m.textContent = mode; m.className = 'mode ' + mode;

  const w = document.getElementById('warnings');
  if (d.warnings && d.warnings.length) {
    w.hidden = false;
    w.innerHTML = d.warnings.map(x =>
      '<div class="warn-box' + (/INVALID|EXPIRED/.test(x) ? ' bad' : '') + '">' +
      esc(x) + '</div>').join('');
  } else { w.hidden = true; }

  const L = d.license || {};
  const tier = L.tier || {};
  const status = !L.valid ? '<span class="bad">INVALID</span>'
    : L.in_grace ? '<span class="warnc">IN GRACE</span>'
    : '<span class="ok">VALID</span>';
  document.getElementById('lic').innerHTML = [
    ['Status', status],
    ['Venue', esc(L.venue_name || L.tenant_id || '—')],
    ['Tenant', '<code>' + esc(L.tenant_id || '—') + '</code>'],
    ['Tier', esc(tier.name || '—') + ' · ' + (tier.requests_per_minute||0) +
             '/min · ' + (tier.requests_per_day||0) + '/day/host'],
    ['Sports', esc((L.sports||[]).join(', ') || '—')],
    ['Expires', L.expires_at ? new Date(L.expires_at).toLocaleString() : '—'],
    ['Machine', '<code>' + esc((L.fingerprint||'—').slice(0,16)) + '…</code>'],
  ].map(([k,v]) => '<dt>'+k+'</dt><dd>'+v+'</dd>').join('');

  const M = d.metrics || {};
  document.getElementById('stats').innerHTML = [
    ['Requests', M.requests||0], ['Messages', M.messages||0],
    ['Req/min', M.requests_per_minute||0],
    ['Req/sec', (M.requests_per_second||0).toFixed(2)],
    ['429s', M.throttles_429||0], ['Errors', M.errors||0],
    ['Sweeps', M.sweeps||0],
    ['Uptime', fmtDur(M.uptime_seconds)],
  ].map(([k,v]) => '<div class="stat"><b>'+v+'</b><span>'+k+'</span></div>').join('');

  const plans = {};
  (d.plans||[]).forEach(p => plans[p.vertical] = p);
  document.getElementById('budgets').innerHTML = (d.budgets||[]).map(b => {
    const p = plans[b.vertical] || {};
    const pct = Math.round((b.pressure||0)*100);
    const cls = pct >= 90 ? 'bad' : pct >= 75 ? 'warn' : '';
    const state = p.state || 'idle';
    const stateCls = b.deferring ? 'deferred'
      : state === 'live' ? 'live' : state === 'break' ? 'break' : '';
    const api = b.has_api_view
      ? b.api_day_remaining + '/' + b.api_day_limit + ' today, ' +
        b.api_minute_remaining + ' this min'
      : '<span class="muted">not yet observed</span>';
    return '<tr>' +
      '<td><b>' + esc(b.vertical) + '</b></td>' +
      '<td>' + b.budget + '<span class="muted"> req</span></td>' +
      '<td>' + b.used_today + ' <span class="muted">(' + pct + '%)</span>' +
        '<div class="bar"><i class="' + cls + '" style="width:' +
        Math.min(100,pct) + '%"></i></div></td>' +
      '<td>' + ((b.weight||0)*100).toFixed(0) + '%</td>' +
      '<td>' + fmtDur(p.interval_seconds) +
        (p.target_seconds && p.interval_seconds > p.target_seconds + 1
          ? ' <span class="muted">(want ' + fmtDur(p.target_seconds) + ')</span>' : '') +
        '</td>' +
      '<td><span class="pill ' + stateCls + '">' +
        esc(b.deferring ? 'deferred' : state) + '</span></td>' +
      '<td class="muted">' + api + '</td>' +
    '</tr>';
  }).join('') || '<tr><td colspan="7" class="muted">No licensed API-Sports verticals — ' +
                 'simulation mode spends no upstream quota.</td></tr>';

  document.getElementById('foot').textContent =
    'updated ' + new Date(d.now).toLocaleTimeString();
}

async function tick() {
  try {
    const r = await fetch('/api/state', {cache:'no-store'});
    render(await r.json());
  } catch (e) {
    document.getElementById('foot').textContent = 'disconnected — retrying';
  }
}
tick(); setInterval(tick, 2000);
</script>
</body>
</html>
`
