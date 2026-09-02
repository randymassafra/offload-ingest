/* Offload Intelligence — Dashboard Design System 1.0.0
 *
 * Rendering primitives shared by every product dashboard. This file knows how a
 * card looks; it knows nothing about what any product measures. Each dashboard
 * fetches its own state and hands plain values to these functions.
 *
 * No framework and no build step, deliberately. The whole file is smaller than
 * the runtime of any library that would replace it, and an appliance that
 * cannot reach a CDN still renders. */

const DDS = (() => {
  'use strict';

  const esc = s => String(s ?? '').replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

  /* ---- formatting ------------------------------------------------------ */

  const num = (v, dp = 0) =>
    (v === null || v === undefined || Number.isNaN(v)) ? '—'
      : Number(v).toLocaleString(undefined, {
          minimumFractionDigits: dp, maximumFractionDigits: dp });

  /* Durations are rendered at the precision an operator can act on. Sub-second
   * latency in milliseconds, minutes for polling cadence, hours for uptime —
   * a single unit would make one of those unreadable. */
  const dur = s => {
    if (s === null || s === undefined || !isFinite(s) || s <= 0) return '—';
    if (s < 1) return (s * 1000).toFixed(0) + 'ms';
    if (s < 60) return s.toFixed(s < 10 ? 1 : 0) + 's';
    if (s < 3600) return (s / 60).toFixed(s < 600 ? 1 : 0) + 'm';
    if (s < 86400) return (s / 3600).toFixed(1) + 'h';
    return (s / 86400).toFixed(1) + 'd';
  };

  const ms = v => {
    if (v === null || v === undefined || !isFinite(v)) return '—';
    if (v < 1000) return v.toFixed(v < 10 ? 1 : 0) + 'ms';
    return (v / 1000).toFixed(2) + 's';
  };

  const bytes = v => {
    if (!isFinite(v) || v <= 0) return '—';
    const u = ['B', 'KB', 'MB', 'GB', 'TB'];
    let i = 0;
    while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
    return v.toFixed(i === 0 ? 0 : 1) + ' ' + u[i];
  };

  const pct = v => (v === null || v === undefined || !isFinite(v))
    ? '—' : (v * 100).toFixed(v < 0.1 ? 1 : 0) + '%';

  /* ---- sparkline ------------------------------------------------------- */

  /* One hour of history as an inline SVG polyline.
   *
   * The y-axis is scaled to the series' own range rather than to zero. A feed
   * polling steadily at 11 requests/minute would otherwise draw a flat line at
   * the top of the box and hide exactly the dip an operator is looking for.
   * The floor is pinned to zero only when the series itself reaches zero. */
  function sparkline(series, opts = {}) {
    const w = opts.width || 240, h = opts.height || 34, pad = 2;
    const pts = (series || []).filter(v => v !== null && v !== undefined && isFinite(v));
    if (pts.length < 2) {
      return `<svg class="dds-spark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
        <text class="dds-spark-empty" x="2" y="${h / 2 + 3}">collecting…</text></svg>`;
    }
    let lo = Math.min(...pts), hi = Math.max(...pts);
    if (lo > 0 && hi / lo < 4) lo = lo * 0.92;   // keep a near-flat line off the edge
    if (hi === lo) { hi = lo + 1; }
    const span = hi - lo;
    const dx = (w - pad * 2) / (pts.length - 1);
    const y = v => pad + (h - pad * 2) * (1 - (v - lo) / span);

    const line = pts.map((v, i) => `${(pad + i * dx).toFixed(1)},${y(v).toFixed(1)}`).join(' ');
    const area = `${pad},${h - pad} ${line} ${(pad + (pts.length - 1) * dx).toFixed(1)},${h - pad}`;
    return `<svg class="dds-spark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none"
              role="img" aria-label="${esc(opts.label || 'one hour trend')}">
      <polygon class="dds-spark-area" points="${area}"/>
      <polyline class="dds-spark-line" points="${line}"/>
    </svg>`;
  }

  /* ---- card ------------------------------------------------------------ */

  /* The mandated anatomy: title, current value, one-hour sparkline, health lamp.
   *
   * `alert` drives the pulse and is separate from `health` on purpose. A card
   * can be amber without demanding attention — the pulse is reserved for a
   * threshold an operator agreed is worth interrupting them for. */
  function card(o) {
    const span = o.span || 3;
    const health = o.health || 'unknown';
    const alert = o.alert ? (o.alert === true ? health : o.alert) : '';
    const alertAttr = (alert === 'bad' || alert === 'warn') ? ` data-alert="${alert}"` : '';
    return `<article class="dds-card dds-col-${span}"${alertAttr}>
      <div class="dds-card-head">
        <h3 class="dds-card-title">${esc(o.title)}</h3>
        <span class="dds-lamp" data-health="${esc(health)}"
              title="${esc(o.healthNote || health)}"></span>
      </div>
      <div class="dds-card-value">${o.value}${
        o.unit ? `<span class="dds-unit">${esc(o.unit)}</span>` : ''}</div>
      ${o.sub ? `<div class="dds-card-sub">${o.sub}</div>` : ''}
      ${o.body || ''}
      ${o.series ? sparkline(o.series, { label: o.title + ' over the last hour' }) : ''}
    </article>`;
  }

  /* ---- sidebar --------------------------------------------------------- */

  function sideItem(o) {
    return `<li class="dds-side-item" data-muted="${o.muted ? 'true' : 'false'}">
      <span class="dds-lamp" data-health="${esc(o.health || 'unknown')}"
            title="${esc(o.note || '')}"></span>
      <span class="dds-side-name">${esc(o.name)}</span>
      <span class="dds-side-meta">${esc(o.meta ?? '')}</span>
    </li>`;
  }

  function sideList(title, items) {
    return `<h2 class="dds-side-title">${esc(title)}</h2>
      <ul class="dds-side-list">${items.map(sideItem).join('')}</ul>`;
  }

  /* ---- header ---------------------------------------------------------- */

  function header(o) {
    const el = document.getElementById('dds-header');
    if (!el) return;
    el.querySelector('[data-dds="status-lamp"]').dataset.health = o.health || 'unknown';
    el.querySelector('[data-dds="status-text"]').textContent = o.statusText || '—';
    const mode = el.querySelector('[data-dds="mode"]');
    if (mode) { mode.dataset.mode = o.mode || 'simulation'; mode.textContent = o.mode || '—'; }
    const updated = el.querySelector('[data-dds="updated"]');
    if (updated) {
      updated.textContent = o.updated
        ? new Date(o.updated).toLocaleTimeString(undefined, { hour12: false })
        : '—';
    }
  }

  /* ---- polling --------------------------------------------------------- */

  /* Poll the product's own state endpoint. A failed fetch degrades the header
   * lamp rather than blanking the page: the last good numbers plus a visible
   * "disconnected" is more use than an empty screen. */
  function poll(url, render, intervalMs = 2000) {
    let failures = 0;
    const tick = async () => {
      try {
        const r = await fetch(url, { cache: 'no-store' });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        render(await r.json());
        failures = 0;
      } catch (e) {
        if (++failures === 1) {
          const lamp = document.querySelector('[data-dds="status-lamp"]');
          const text = document.querySelector('[data-dds="status-text"]');
          if (lamp) lamp.dataset.health = 'bad';
          if (text) text.textContent = 'disconnected';
        }
      }
    };
    tick();
    return setInterval(tick, intervalMs);
  }

  return { esc, num, dur, ms, bytes, pct, sparkline, card, sideItem, sideList, header, poll };
})();
