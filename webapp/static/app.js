/* JARVIS intraday viewer: plain JS + Plotly (served locally from /vendor/plotly.min.js).
   Structure: three sections (Equities, Market, Derivatives), each with its own controls, key figures and sub-tabs.
   Every chart is its own Plotly figure with its own HTML legend; time charts in one section zoom together. */
const $ = (id) => document.getElementById(id);
const css = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
const fmt = (x, d = 2) => (x == null || Number.isNaN(x) ? '–' : Number(x).toLocaleString('en-IN', { minimumFractionDigits: d, maximumFractionDigits: d }));
const fmtInt = (x) => (x == null || Number.isNaN(x) ? '–' : Math.round(x).toLocaleString('en-IN'));
const fmtBig = (x) => {                           // Indian lakh / crore, as traders read them
  if (x == null) return '–';
  const a = Math.abs(x);
  if (a >= 1e7) return (x / 1e7).toLocaleString('en-IN', { maximumFractionDigits: 2 }) + ' Cr';
  if (a >= 1e5) return (x / 1e5).toLocaleString('en-IN', { maximumFractionDigits: 2 }) + ' L';
  return fmtInt(x);
};
const signed = (x, f = fmtInt) => (x == null ? '–' : `<span class="${x >= 0 ? 'pos' : 'neg'}">${x > 0 ? '+' : ''}${f(x)}</span>`);
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const niceDate = (d) => (d ? new Date(d + 'T00:00:00').toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' }) : '');
const WIDTH_LABEL = { 1: '1 second', 5: '5 seconds', 15: '15 seconds', 30: '30 seconds', 60: '1 minute', 300: '5 minutes' };
const VOLWIN_LABEL = { 60: '1 minute', 300: '5 minutes', 900: '15 minutes', 1800: '30 minutes' };
const SUBS = { instrument: ['price', 'book', 'vol'], overview: ['heat', 'liq', 'table'], fo: ['summary', 'chain', 'oi', 'contract', 'greeks'] };

const state = {
  meta: null, day: null, tab: 'instrument', sub: { instrument: 'price', overview: 'heat', fo: 'summary' },
  symbol: null, width: 1, volwin: 300, data: null,
  overview: [], sort: ['volume', -1], heatN: 20, heatMetric: 'realised_vol_pct',
  underlying: null, expiry: null, foData: null, foContract: null, foContractData: null, greeksData: null, range: 10, focus: 'active',
};
let reqId = 0, foReq = 0, fcReq = 0;
const phone = () => window.innerWidth < 640;

/* ---------------------------------------------------------------- plumbing */
async function api(path, params) {
  const r = await fetch(path + '?' + new URLSearchParams(params));
  if (!r.ok) {
    let msg = r.statusText;
    try { msg = (await r.json()).detail || msg; } catch (e) { /* not json */ }
    throw new Error(msg);
  }
  return r.json();
}
function setStatus(t, kind = 'ok') {
  $('status').textContent = t;
  $('context').className = 'context' + (kind === 'err' ? ' err' : kind === 'busy' ? ' busy' : '');
}
const fail = (e) => setStatus(e.message || String(e), 'err');
const fillSelect = (el, items, sel) => {
  el.innerHTML = items.map((it) => it.group
    ? `<optgroup label="${esc(it.group)}">${it.items.map(([v, l]) => `<option value="${esc(v)}">${esc(l)}</option>`).join('')}</optgroup>`
    : `<option value="${esc(it[0])}">${esc(it[1])}</option>`).join('');
  if (sel != null) el.value = sel;
};

function readHash() {
  const p = new URLSearchParams(location.hash.slice(1));
  return Object.fromEntries(p.entries());
}
function writeHash() {
  const p = { tab: state.tab, date: state.day || '' };
  p.sub = state.sub[state.tab];
  if (state.tab === 'instrument') Object.assign(p, { symbol: state.symbol || '', width: state.width, volwin: state.volwin });
  if (state.tab === 'fo') Object.assign(p, { underlying: state.underlying || '', expiry: state.expiry || '', contract: state.foContract || '',
                                              width: state.width, volwin: state.volwin, range: state.range, focus: state.focus });
  history.replaceState(null, '', '#' + new URLSearchParams(p).toString());
}

/* ---------------------------------------------------------------- charts */
function theme() {
  return { fg: css('--fg'), fg2: css('--fg2'), muted: css('--muted'), grid: css('--grid'), line: css('--line'), up: css('--up'), down: css('--down'),
           acc: css('--accent'), orange: css('--orange'), purple: css('--purple'), teal: css('--teal'), yellow: css('--yellow') };
}
function baseLayout(extra = {}) {
  const t = theme(), p = phone();
  return Object.assign({
    margin: { l: p ? 46 : 64, r: p ? 10 : 20, t: 8, b: 30 }, paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
    font: { color: t.fg2, family: css('--font') || 'sans-serif', size: 11 }, showlegend: false, dragmode: p ? false : 'zoom',
    hoverlabel: { bgcolor: '#1c1c1f', bordercolor: 'rgba(255,255,255,.18)', font: { color: t.fg, size: 12 } },
  }, extra);
}
const timeAxis = (extra = {}) => Object.assign({ type: 'date', gridcolor: theme().grid, linecolor: theme().line, zeroline: false, tickformat: '%H:%M',
                                                 hoverformat: '%H:%M:%S', showspikes: true, spikemode: 'across', spikethickness: 1, spikecolor: theme().muted, spikedash: 'dot' }, extra);
const valAxis = (extra = {}) => Object.assign({ gridcolor: theme().grid, linecolor: theme().line, zerolinecolor: theme().line, tickfont: { size: 10 }, automargin: true }, extra);
const config = () => ({ responsive: true, displaylogo: false, displayModeBar: !phone() && 'hover', scrollZoom: false,
                        modeBarButtonsToRemove: ['select2d', 'lasso2d', 'autoScale2d', 'toImage'] });

/* one HTML legend per chart: [{name, color, kind: line|bar|dash, traces: [i, ...], off: hidden by default}].
   plot(id, traces, layout, items) draws the chart with the series the viewer last chose to show or hide. */
const legendOff = {};
const isOff = (chartId, it) => legendOff[chartId + '|' + it.key] ?? !!it.off;
function plot(chartId, traces, layout, items, cfg) {
  (items || []).forEach((it) => { it.key = it.key || it.name; if (isOff(chartId, it)) it.traces.forEach((i) => { traces[i].visible = false; }); });
  Plotly.react(chartId, traces, layout, cfg || config());
  if (items) legend(chartId, items);
}
function legend(chartId, items) {
  const el = document.querySelector(`.legend[data-for="${chartId}"]`);
  if (!el) return;
  el.innerHTML = items.map((it, k) => `<button type="button" data-k="${k}" class="${isOff(chartId, it) ? 'off' : ''}" aria-pressed="${!isOff(chartId, it)}" title="Show or hide">` +
    `<i class="${it.kind || 'line'}" style="color:${it.color}"></i>${esc(it.name)}</button>`).join('');
  el.querySelectorAll('button').forEach((b) => b.onclick = () => {
    const it = items[+b.dataset.k];
    const off = b.classList.toggle('off');
    b.setAttribute('aria-pressed', !off);
    legendOff[chartId + '|' + it.key] = off;
    Plotly.restyle(chartId, { visible: off ? false : true }, it.traces);
  });
}

/* time charts in one group share their zoom */
const groups = {};
function syncGroup(name, ids) {
  groups[name] = ids;
  let busy = false;
  ids.forEach((id) => {
    const div = $(id);
    if (!div || !div.on) return;
    div.removeAllListeners && div.removeAllListeners('plotly_relayout');
    div.on('plotly_relayout', (ev) => {
      if (busy) return;
      let upd = null;
      if (ev['xaxis.range[0]'] !== undefined) upd = { 'xaxis.range': [ev['xaxis.range[0]'], ev['xaxis.range[1]']] };
      else if (ev['xaxis.range']) upd = { 'xaxis.range': ev['xaxis.range'] };
      else if (ev['xaxis.autorange']) upd = { 'xaxis.autorange': true };
      if (!upd) return;
      busy = true;
      Promise.all(ids.filter((o) => o !== id && $(o) && $(o).data).map((o) => Plotly.relayout(o, upd))).finally(() => { busy = false; });
    });
  });
}
function resizeVisible() {
  document.querySelectorAll('.view:not([hidden]) .subview:not([hidden]) .plot').forEach((p) => { if (p.data) Plotly.Plots.resize(p); });
}

/* ---------------------------------------------------------------- sections and sub-tabs */
function setTab(tab, load = true) {
  state.tab = tab;
  for (const t of Object.keys(SUBS)) {
    $('tab-' + t).setAttribute('aria-selected', t === tab);
    $('view-' + t).hidden = t !== tab;
  }
  showSub(tab, state.sub[tab], false);
  if (!load) return;
  if (tab === 'fo') loadFoUnderlyings().catch(fail);
  else if (tab === 'overview') loadOverview().catch(fail);
  else loadSymbols().catch(fail);
}
function showSub(tab, sub, rerender = true) {
  if (!SUBS[tab].includes(sub)) sub = SUBS[tab][0];
  state.sub[tab] = sub;
  const view = $('view-' + tab);
  view.querySelectorAll(`.subtabs[data-tab="${tab}"] button`).forEach((b) => b.setAttribute('aria-selected', b.dataset.sub === sub));
  view.querySelectorAll('.subview').forEach((v) => { v.hidden = v.dataset.sub !== sub; });
  writeHash();
  if (rerender) requestAnimationFrame(() => { resizeVisible(); if (tab === 'fo' && sub === 'chain') centreChain(); });
}
document.querySelectorAll('.subtabs').forEach((nav) => nav.querySelectorAll('button').forEach((b) => b.onclick = () => {
  showSub(nav.dataset.tab, b.dataset.sub);
  if (nav.dataset.tab === 'fo' && b.dataset.sub === 'contract' && !state.foContractData && state.foContract) loadFoContract(state.foContract);
  if (nav.dataset.tab === 'fo' && b.dataset.sub === 'greeks') loadGreeks();
}));

/* ---------------------------------------------------------------- start-up */
async function init() {
  const h = readHash();
  try { state.meta = await api('/api/meta', {}); } catch (e) { fail(new Error('Cannot reach the API: ' + e.message)); return; }
  $('demo-banner').hidden = !state.meta.demo;
  const widths = state.meta.widths.map((w) => [w, WIDTH_LABEL[w] || w + ' s']);
  const wins = state.meta.vol_windows.map((w) => [w, VOLWIN_LABEL[w] || w + ' s']);
  state.width = state.meta.widths.includes(+h.width) ? +h.width : 1;
  state.volwin = state.meta.vol_windows.includes(+h.volwin) ? +h.volwin : 300;
  for (const id of ['width', 'fo-width', 'fo-greeks-width']) fillSelect($(id), widths, state.width);
  for (const id of ['volwin', 'fo-volwin']) fillSelect($(id), wins, state.volwin);
  if (!state.meta.days.length) {
    $('empty').hidden = false; document.querySelectorAll('.view').forEach((v) => { v.hidden = true; }); setStatus('No data yet', 'err'); return;
  }
  fillSelect($('date'), state.meta.days.map((d) => [d, niceDate(d)]), (state.day = state.meta.days.includes(h.date) ? h.date : state.meta.days[0]));
  state.symbol = h.symbol || null; state.underlying = h.underlying || null; state.expiry = h.expiry || null; state.foContract = h.contract || null;
  if (['5', '10', '20', '0'].includes(h.range)) state.range = +h.range;
  if (['active', 'atm', 'all'].includes(h.focus)) state.focus = h.focus;
  $('chain-range').value = String(state.range); $('oi-focus').value = state.focus;
  const tab = SUBS[h.tab] ? h.tab : 'instrument';
  if (h.sub) state.sub[tab] = h.sub;
  setTab(tab);
}

/* ================================================================ Equities */
async function loadSymbols() {
  setStatus('Loading instruments…', 'busy');
  const syms = await api('/api/symbols', { date: state.day });
  fillSelect($('symbol'), syms.map((s) => [s.symbol, s.symbol.replace(/\.NS$/, '')]));
  if (!syms.find((s) => s.symbol === state.symbol)) state.symbol = syms[0].symbol;
  $('symbol').value = state.symbol;
  await refreshEquity();
}

async function refreshEquity() {
  const id = ++reqId;
  writeHash();
  setStatus('Loading ' + state.symbol + '…', 'busy');
  try {
    const d = await api('/api/day', { date: state.day, symbol: state.symbol, width: state.width, vol_window: state.volwin });
    if (id !== reqId) return;
    state.data = d;
    $('eq-title').textContent = state.symbol.replace(/\.NS$/, '');
    $('eq-sub').textContent = `NSE · ${niceDate(state.day)} · ${WIDTH_LABEL[d.width]} bars`;
    renderKpis('cards', equityKpis(d.summary));
    renderSeries(d, EQ);
    setStatus(`${state.symbol} · ${state.day} · ${fmtInt(d.series.ts.length)} bars of ${WIDTH_LABEL[d.width]} · times in IST`);
  } catch (e) { if (id === reqId) fail(e); }
}

function renderKpis(target, items) {
  $(target).style.setProperty('--n', items.length);                 // one row on desktop
  $(target).style.setProperty('--nt', items.length <= 4 ? items.length : Math.ceil(items.length / 2));   // two even rows on tablets
  $(target).innerHTML = items.map(([k, v, sub]) => `<div class="kpi"><b>${k}</b><span>${v}</span>${sub ? `<small>${sub}</small>` : ''}</div>`).join('');
}
function equityKpis(s) {
  return [
    ['Last', fmt(s.close), `${signed(s.return_pct, (x) => fmt(x) + '%')} on the open`],
    ['Day high', fmt(s.high), `low ${fmt(s.low)} · open ${fmt(s.open)}`],
    ['Volume', fmtBig(s.volume), `VWAP ${fmt(s.vwap)}`],
    ['Spread', fmt(s.median_spread_bps, 2) + ' bps', `mean ${fmt(s.avg_spread_bps, 2)} bps`],
    ['Realised vol', fmt(s.realised_vol_pct, 1) + '%', 'annualised'],
    ['Bars', fmtInt(s.bars), `book on ${fmt(s.book_coverage_pct, 0)}%`],
  ];
}

/* chart ids per group: Equities (EQ) and the derivatives Contract sub-tab (FC) use the same renderer */
const EQ = { key: 'eq', group: 'eq', price: 'eq-price', volume: 'eq-volume', depth: 'eq-depth', imb: 'eq-imb', spread: 'eq-spread', vol: 'eq-vol', book: 'book-body', note: 'note' };
const FC = { key: 'fc', group: 'fc', price: 'fc-price', volume: 'fc-volume', depth: 'fc-depth', imb: 'fc-imb', spread: 'fc-spread', book: 'fo-contract-book-body', note: 'fo-contract-note' };

function renderSeries(d, ids) {
  const s = d.series, S = d.summary, t = s.ts, T = theme(), step = d.width <= 5 ? 'hv' : 'linear', win = VOLWIN_LABEL[d.vol_window];
  const hasBook = d.has_book, hasOi = d.has_oi && s.oi;
  const p99 = (arr) => { const v = arr.filter((x) => x != null).sort((a, b) => a - b); return v.length ? v[Math.floor(v.length * 0.99)] : 1; };
  const line = (y, name, color, w = 1.4, more = {}) => Object.assign({ x: t, y, name, type: 'scattergl', mode: 'lines', line: { color, width: w, shape: step } }, more);
  // thousands of SVG bars lock up the browser at 1-second resolution: draw dense series as filled GPU steps instead
  const dense = t.length > 1500;
  const bars = (y, name, color, opacity, more = {}) => dense
    ? Object.assign({ x: t, y, name, type: 'scattergl', mode: 'lines', line: { color, width: 0.6, shape: 'hv' }, fill: 'tozeroy', fillcolor: color, opacity }, more)
    : Object.assign({ x: t, y, name, type: 'bar', marker: { color, opacity } }, more);
  const charts = [];

  // price (+ open interest for derivatives)
  {
    const tr = [line(s.close, 'Last', T.acc, 1.6)], lg = [{ name: 'Last', color: T.acc, traces: [0] }];
    if (hasBook) {
      tr.push(line(s.bid, 'Best bid', T.up, 0.7, { opacity: 0.8 }), line(s.ask, 'Best ask', T.down, 0.7, { opacity: 0.8 }));
      lg.push({ name: 'Bid / ask', color: T.fg2, traces: [1, 2], off: true });
    }
    tr.push({ x: [t[0], t[t.length - 1]], y: [S.vwap, S.vwap], name: 'VWAP', type: 'scatter', mode: 'lines', line: { color: T.muted, width: 1, dash: 'dash' }, hoverinfo: 'skip' });
    lg.push({ name: 'VWAP', color: T.muted, kind: 'dash', traces: [tr.length - 1] });
    const layout = baseLayout({ hovermode: 'x unified', xaxis: timeAxis(), yaxis: valAxis({ tickformat: ',.2f', title: phone() ? '' : { text: '₹', font: { size: 11 } } }) });
    if (hasOi) {
      tr.push(Object.assign(line(s.oi, 'Open interest', T.yellow, 1.3), { yaxis: 'y2' }));
      lg.push({ name: 'Open interest (right)', key: 'oi', color: T.yellow, traces: [tr.length - 1] });
      layout.yaxis2 = valAxis({ overlaying: 'y', side: 'right', showgrid: false, tickformat: '.3s' });
      layout.margin = Object.assign({}, layout.margin, { r: phone() ? 40 : 56 });
    }
    plot(ids.price, tr, layout, lg); charts.push(ids.price);
  }
  // volume
  {
    const tr = [bars(s.volume, 'Volume per bar', T.acc, 0.7),
                Object.assign(line(s.cum_volume, 'Cumulative', T.fg2, 1.2), { yaxis: 'y2' })];
    plot(ids.volume, tr, baseLayout({ hovermode: 'x unified', bargap: 0, xaxis: timeAxis(), yaxis: valAxis({ tickformat: '.2s' }),
      yaxis2: valAxis({ overlaying: 'y', side: 'right', showgrid: false, tickformat: '.2s', rangemode: 'tozero' }),
      margin: { l: phone() ? 46 : 64, r: phone() ? 40 : 56, t: 8, b: 30 } }),
      [{ name: 'Per bar', color: T.acc, kind: 'bar', traces: [0] }, { name: 'Cumulative (right)', key: 'cum', color: T.fg2, traces: [1] }]);
    charts.push(ids.volume);
  }
  // size at best: bid up, ask down (raw faint + smoothed)
  if (ids.depth) {
    const neg = (arr) => arr.map((v) => (v == null ? null : -v));
    const qmax = Math.max(p99(s.bid_qty), p99(s.ask_qty), 1) * 1.15;
    const tr = [bars(s.bid_qty, 'Bid size', T.up, 0.25, { hoverinfo: 'skip' }), bars(neg(s.ask_qty), 'Ask size', T.down, 0.25, { hoverinfo: 'skip' }),
                line(s.bid_qty_avg, `Bid (${win} avg)`, T.up, 1.6), line(neg(s.ask_qty_avg), `Ask (${win} avg)`, T.down, 1.6)];
    plot(ids.depth, tr, baseLayout({ hovermode: 'x unified', barmode: 'relative', bargap: 0, xaxis: timeAxis(), yaxis: valAxis({ range: [-qmax, qmax], tickformat: '.2s' }) }),
      [{ name: `Bid (${win} avg)`, key: 'bid', color: T.up, traces: [2] }, { name: `Ask (${win} avg)`, key: 'ask', color: T.down, traces: [3] },
       { name: 'Every second', key: 'raw', color: T.muted, kind: 'bar', traces: [0, 1], off: true }]);
    charts.push(ids.depth);
  }
  if (ids.imb) {
    const tr = [line(s.imbalance_avg, `Imbalance (${win} avg)`, T.yellow, 1.5)];
    plot(ids.imb, tr, baseLayout({ hovermode: 'x unified', xaxis: timeAxis(), yaxis: valAxis({ range: [-1, 1], tickvals: [-1, -0.5, 0, 0.5, 1] }) }),
      [{ name: `${win} average · +1 all bid, −1 all ask`, key: 'imb', color: T.yellow, traces: [0] }]);
    charts.push(ids.imb);
  }
  // spread
  {
    const smax = Math.max(p99(s.spread_bps), 0.1) * 1.2;
    const tr = [line(s.spread_bps, 'Spread (raw)', T.orange, 0.6, { opacity: 0.25, hoverinfo: 'skip' }), line(s.spread_bps_avg, `Spread (${win} avg)`, T.orange, 1.8)];
    plot(ids.spread, tr, baseLayout({ hovermode: 'x unified', xaxis: timeAxis(), yaxis: valAxis({ range: [0, smax], ticksuffix: ' bps' }) }),
      [{ name: `${win} average`, key: 'avg', color: T.orange, traces: [1] }, { name: 'Every second', key: 'raw', color: T.orange, kind: 'bar', traces: [0], off: true }]);
    charts.push(ids.spread);
  }
  if (ids.vol) {
    plot(ids.vol, [line(s.vol_ann, 'Realised vol', T.purple, 1.6)],
      baseLayout({ hovermode: 'x unified', xaxis: timeAxis(), yaxis: valAxis({ rangemode: 'tozero', ticksuffix: '%' }) }),
      [{ name: `${win} window`, key: 'vol', color: T.purple, traces: [0] }]);
    charts.push(ids.vol);
  }
  syncGroup(ids.group, charts);
  const at = new Map(t.map((x, i) => [x, i]));
  charts.forEach((c) => {
    const el = $(c);
    el.removeAllListeners && el.removeAllListeners('plotly_hover');
    el.on('plotly_hover', (ev) => {
      const p = ev.points.find((q) => q.data.x && q.data.x.length === t.length) || ev.points[0];
      const i = p && p.data.x.length === t.length ? p.pointIndex : at.get(String(p && p.x).replace('T', ' ').slice(0, 19));
      if (i != null) bookAt(d, i, ids);
    });
  });
  $(ids.note).textContent = hasBook ? '' : 'This day has no book data (bars from the REST candle API carry no bid or ask), so the book and spread charts are empty.';
  bookAt(d, t.length - 1, ids);
  requestAnimationFrame(resizeVisible);
}

function bookAt(d, i, ids) {
  const s = d.series;
  if (bookMode[ids.key] === 'live') return;            // the live ladder owns the card while it is switched on
  if (i == null || s.ts[i] == null) return;
  const row = (cls, label, px, q) => `<tr class="${cls}"><td>${label}</td><td>${px == null ? '–' : fmt(px)}</td><td>${q == null ? '–' : fmtInt(q)}</td></tr>`;
  const kv = [['Spread', s.spread[i] == null ? '–' : `${fmt(s.spread[i])} · ${fmt(s.spread_bps[i], 2)} bps`], ['Mid', fmt(s.mid[i])],
              ['Imbalance', s.imbalance[i] == null ? '–' : fmt(s.imbalance[i], 2)], ['Last', fmt(s.close[i])], ['Volume', fmtInt(s.volume[i])]];
  if (s.oi && s.oi[i] != null) kv.push(['Open interest', fmtInt(s.oi[i])]);
  kv.push([`Vol (${VOLWIN_LABEL[d.vol_window]})`, s.vol_ann[i] == null ? '–' : fmt(s.vol_ann[i], 1) + '%']);
  $(ids.book).innerHTML = `<div class="ts">${s.ts[i].slice(11)} IST</div>
    <table class="ladder"><tr><td></td><td>Price</td><td>Size</td></tr>${row('ask', 'Ask', s.ask[i], s.ask_qty[i])}${row('bid', 'Bid', s.bid[i], s.bid_qty[i])}</table>
    <dl class="kv">${kv.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join('')}</dl>`;
}

/* ================================================================ Market */
const COLS = [
  ['symbol', 'Instrument', (v) => v.replace(/\.NS$/, '')], ['close', 'Last', fmt], ['return_pct', 'Return', (v) => signed(v, (x) => fmt(x) + '%')],
  ['volume', 'Volume', fmtBig], ['turnover', 'Traded value', (v) => '₹' + fmtBig(v)], ['vwap', 'VWAP', fmt],
  ['realised_vol_pct', 'Realised vol', (v) => fmt(v, 1) + '%'], ['median_spread_bps', 'Spread (bps)', (v) => fmt(v, 2)],
  ['avg_bid_qty', 'Avg bid size', fmtInt], ['avg_ask_qty', 'Avg ask size', fmtInt], ['bars', 'Bars', fmtInt],
];

async function loadOverview() {
  const id = ++reqId;
  writeHash();
  setStatus('Loading market…', 'busy');
  const rows = await api('/api/overview', { date: state.day });
  if (id !== reqId) return;
  rows.forEach((r) => { r.turnover = r.vwap != null && r.volume != null ? r.vwap * r.volume : (r.close || 0) * (r.volume || 0); });
  state.overview = rows;
  renderOverview();
  setStatus(`${rows.length} instruments · ${state.day} · times in IST`);
}

function renderOverview() {
  const rows = state.overview, T = theme();
  $('mk-sub').textContent = `NSE · ${niceDate(state.day)} · ${rows.length} instruments`;
  const tot = rows.reduce((a, r) => a + (r.turnover || 0), 0);
  const ups = rows.filter((r) => r.return_pct > 0).length;
  const vols = rows.map((r) => r.realised_vol_pct).filter((v) => v != null).sort((a, b) => a - b);
  const med = vols.length ? vols[Math.floor(vols.length / 2)] : null;
  const top = [...rows].sort((a, b) => (b.realised_vol_pct || 0) - (a.realised_vol_pct || 0))[0];
  renderKpis('mk-cards', [
    ['Traded value', '₹' + fmtBig(tot), `${rows.length} instruments`],
    ['Advancers', `${ups} / ${rows.length}`, `${rows.length - ups} declined or flat`],
    ['Median vol', fmt(med, 1) + '%', 'annualised'],
    ['Most volatile', top ? top.symbol.replace(/\.NS$/, '') : '–', top ? fmt(top.realised_vol_pct, 1) + '%' : ''],
  ]);
  renderHeat(); renderLeaders(); renderScatter(); renderTable();
}

function renderHeat() {
  const T = theme(), metric = state.heatMetric;
  const ranked = [...state.overview].filter((r) => r.realised_vol_pct != null).sort((a, b) => b.realised_vol_pct - a.realised_vol_pct);
  const rows = state.heatN ? ranked.slice(0, state.heatN) : ranked;
  const label = { realised_vol_pct: 'volatility', return_pct: 'return', median_spread_bps: 'spread' }[metric];
  $('heat-title').textContent = state.heatN && ranked.length > state.heatN ? `Top ${state.heatN} most volatile` : 'All instruments by volatility';
  $('heat-legend').textContent = `Tile size = traded value · colour = ${label}`;
  const vals = rows.map((r) => r[metric]);
  const diverging = metric === 'return_pct';
  const amax = Math.max(...vals.map((v) => Math.abs(v || 0)), 0.01);
  const trace = {
    type: 'treemap', labels: rows.map((r) => r.symbol.replace(/\.NS$/, '')), parents: rows.map(() => ''), ids: rows.map((r) => r.symbol),
    values: rows.map((r) => Math.max(r.turnover || 0, 1)), branchvalues: 'total',
    customdata: rows.map((r) => [r.realised_vol_pct, r.return_pct, r.median_spread_bps, r.turnover]),
    texttemplate: '<b>%{label}</b><br>%{customdata[0]:.1f}% vol<br>%{customdata[1]:+.2f}%',
    hovertemplate: '<b>%{label}</b><br>Realised vol %{customdata[0]:.1f}%<br>Return %{customdata[1]:+.2f}%<br>Spread %{customdata[2]:.2f} bps<br>Traded ₹%{customdata[3]:,.0f}<extra></extra>',
    textfont: { size: phone() ? 11 : 13, color: '#fff' }, textposition: 'middle center', pathbar: { visible: false }, tiling: { pad: 0 }, root: { color: 'rgba(0,0,0,0)' },
    marker: { colors: vals, line: { width: 2, color: '#000' }, pad: { t: 0, l: 0, r: 0, b: 0 }, showscale: !phone(),
      colorscale: diverging ? [[0, '#d70015'], [0.5, '#2c2c2e'], [1, '#248a3d']] : [[0, '#1f2a37'], [0.4, '#7c3a10'], [0.75, '#e8590c'], [1, '#e5243b']],
      cmin: diverging ? -amax : Math.min(...vals), cmax: diverging ? amax : Math.max(...vals),
      colorbar: { thickness: 10, len: 0.8, outlinewidth: 0, tickfont: { size: 10, color: T.fg2 }, ticksuffix: metric === 'median_spread_bps' ? ' bps' : '%' } },
  };
  Plotly.react('chart-heat', [trace], baseLayout({ margin: { l: 0, r: 0, t: 0, b: 0 } }), Object.assign(config(), { displayModeBar: false }));
  const el = $('chart-heat');
  el.removeAllListeners && el.removeAllListeners('plotly_treemapclick');
  el.on('plotly_treemapclick', (ev) => { openEquity(ev.points[0].id); return false; });
}

function renderLeaders() {
  const rows = state.overview;
  const list = (target, sorted, val, max, cls) => {
    $(target).innerHTML = sorted.slice(0, 5).map((r, i) => `<li data-s="${esc(r.symbol)}"><span class="i">${i + 1}</span><span class="sym">${esc(r.symbol.replace(/\.NS$/, ''))}</span><span class="${cls ? cls(r) : ''}">${val(r)}</span>
      <span class="bar"><i style="width:${Math.max(4, Math.abs(max(r)) * 100).toFixed(1)}%"></i></span></li>`).join('');
    $(target).querySelectorAll('li').forEach((li) => li.onclick = () => openEquity(li.dataset.s));
  };
  const byVol = [...rows].sort((a, b) => (b.realised_vol_pct || 0) - (a.realised_vol_pct || 0));
  const vmax = byVol.length ? byVol[0].realised_vol_pct || 1 : 1;
  list('lead-vol', byVol, (r) => fmt(r.realised_vol_pct, 1) + '%', (r) => (r.realised_vol_pct || 0) / vmax);
  const byMove = [...rows].sort((a, b) => Math.abs(b.return_pct || 0) - Math.abs(a.return_pct || 0));
  const mmax = byMove.length ? Math.abs(byMove[0].return_pct || 1) || 1 : 1;
  list('lead-move', byMove, (r) => (r.return_pct > 0 ? '+' : '') + fmt(r.return_pct) + '%', (r) => (r.return_pct || 0) / mmax, (r) => (r.return_pct >= 0 ? 'pos' : 'neg'));
}

function renderScatter() {
  const rows = state.overview, T = theme();
  const mx = Math.max(...rows.map((r) => r.turnover || 0), 1);
  Plotly.react('chart-scatter', [{
    type: 'scatter', mode: 'markers+text', x: rows.map((r) => r.realised_vol_pct), y: rows.map((r) => r.median_spread_bps),
    text: rows.map((r) => r.symbol.replace(/\.NS$/, '')), textposition: 'top center', textfont: { size: 10, color: T.fg2 },
    customdata: rows.map((r) => r.symbol),
    marker: { size: rows.map((r) => 8 + 34 * Math.sqrt((r.turnover || 0) / mx)), color: rows.map((r) => r.return_pct),
              colorscale: [[0, '#ff453a'], [0.5, '#8e8e93'], [1, '#30d158']], cmid: 0, opacity: 0.75, line: { width: 0 } },
    hovertemplate: '<b>%{text}</b><br>vol %{x:.1f}%<br>spread %{y:.2f} bps<extra></extra>',
  }], baseLayout({ hovermode: 'closest', dragmode: false,
    xaxis: valAxis({ title: { text: 'Realised volatility (%)', font: { size: 11 } }, ticksuffix: '%' }),
    yaxis: valAxis({ title: { text: 'Median spread (bps)', font: { size: 11 } }, rangemode: 'tozero' }),
    margin: { l: phone() ? 50 : 70, r: 20, t: 16, b: 50 } }), Object.assign(config(), { displayModeBar: false }));
  const el = $('chart-scatter');
  el.removeAllListeners && el.removeAllListeners('plotly_click');
  el.on('plotly_click', (ev) => openEquity(ev.points[0].customdata));
}

function renderTable() {
  const rows = [...state.overview];
  const [key, dir] = state.sort;
  rows.sort((a, b) => (a[key] > b[key] ? 1 : a[key] < b[key] ? -1 : 0) * dir * (key === 'symbol' ? -1 : 1));
  $('table').innerHTML = '<thead><tr>' + COLS.map(([k, l]) => `<th data-k="${k}">${l}${state.sort[0] === k ? (state.sort[1] < 0 ? ' ▾' : ' ▴') : ''}</th>`).join('') + '</tr></thead><tbody>' +
    rows.map((r) => `<tr data-s="${esc(r.symbol)}">` + COLS.map(([k, , f]) => `<td>${r[k] == null ? '–' : f(r[k])}</td>`).join('') + '</tr>').join('') + '</tbody>';
  $('table').querySelectorAll('th').forEach((th) => th.onclick = () => { const k = th.dataset.k; state.sort = [k, state.sort[0] === k ? -state.sort[1] : -1]; renderTable(); });
  $('table').querySelectorAll('tbody tr').forEach((tr) => tr.onclick = () => openEquity(tr.dataset.s));
}

function openEquity(sym) { state.symbol = sym; state.sub.instrument = 'price'; setTab('instrument'); }

/* ================================================================ Derivatives */
async function loadFoUnderlyings() {
  setStatus('Loading underlyings…', 'busy');
  const meta = await api('/api/fo/meta', { date: state.day });
  if (!meta.underlyings.length) {
    fillSelect($('underlying'), []); fillSelect($('expiry'), []);
    state.foData = null; $('fo-empty').hidden = false; $('fo-body').hidden = true;
    $('fo-title').textContent = 'Derivatives'; $('fo-sub').textContent = niceDate(state.day);
    setStatus('No futures or options collected on ' + state.day); return;
  }
  fillSelect($('underlying'), meta.underlyings.map((u) => [u, u]),
    (state.underlying = meta.underlyings.includes(state.underlying) ? state.underlying : meta.underlyings[0]));
  await refreshFo();
}

async function refreshFo() {
  const id = ++foReq;
  writeHash();
  setStatus('Loading ' + state.underlying + '…', 'busy');
  try {
    const params = { date: state.day, underlying: state.underlying };
    if (state.expiry) params.expiry = state.expiry;
    const d = await api('/api/fo/chain', params);
    if (id !== foReq) return;
    state.foData = d; state.expiry = d.expiry;
    fillSelect($('expiry'), d.expiries.length ? d.expiries.map((e) => [e, niceDate(e)]) : [['', 'none collected']], d.expiry || '');
    renderFo(d);
    writeHash();
    setStatus(`${d.underlying} · ${state.day} · ${d.futures.length} futures · ${d.chain.length} strikes${d.expiry ? ' for ' + d.expiry : ''}`);
  } catch (e) { if (id === foReq) fail(e); }
}

function maxPain(chain) {
  let best = null;
  for (const k of chain) {
    let pay = 0;
    for (const s of chain) pay += (s.ce_oi || 0) * Math.max(0, k.strike - s.strike) + (s.pe_oi || 0) * Math.max(0, s.strike - k.strike);
    if (best == null || pay < best[1]) best = [k.strike, pay];
  }
  return best ? best[0] : null;
}

function renderFo(d) {
  const has = d && (d.futures.length || d.chain.length);
  $('fo-empty').hidden = !!has; $('fo-body').hidden = !has;
  if (!has) return;
  $('fo-title').textContent = d.underlying;
  $('fo-sub').textContent = `NSE derivatives · ${niceDate(state.day)}${d.lot_size ? ' · lot ' + fmtInt(d.lot_size) : ''}`;
  const fut = d.futures[0];
  const mp = d.chain.length ? maxPain(d.chain) : null;
  renderKpis('fo-cards', [
    ['Spot (proxy)', fmt(d.spot), fut ? `near future, ${niceDate(fut.expiry)}` : ''],
    ['ATM strike', fmtInt(d.atm_strike), d.expiry ? `expiry ${niceDate(d.expiry)}` : 'no options'],
    ['Put / call OI', d.pcr == null ? '–' : d.pcr.toFixed(2), d.pcr == null ? '' : d.pcr > 1 ? 'more put interest' : 'more call interest'],
    ['Max pain', fmtInt(mp), mp && d.spot ? `${signed(mp - d.spot, fmtInt)} from spot` : ''],
    ['Call OI', fmtBig(d.total_call_oi), 'this expiry'],
    ['Put OI', fmtBig(d.total_put_oi), 'this expiry'],
  ]);
  renderFoFutures(d); renderKeyStrikes(d); renderChain(d); renderOi(d); fillContracts(d);
}

function renderFoFutures(d) {
  $('fo-futures').innerHTML = '<thead><tr><th>Expiry</th><th>Last</th><th>Basis</th><th>OI</th><th>OI change</th><th>Volume</th></tr></thead><tbody>' +
    d.futures.map((r) => `<tr data-s="${esc(r.symbol)}"><td>${niceDate(r.expiry)}</td><td>${fmt(r.close)}</td>
      <td>${r === d.futures[0] ? '<span class="muted">near</span>' : signed(r.close - d.futures[0].close, fmt)}</td>
      <td>${fmtBig(r.oi)}</td><td>${signed(r.oi_change)}</td><td>${fmtBig(r.volume)}</td></tr>`).join('') +
    (d.futures.length ? '' : '<tr><td colspan="6" class="muted">No futures collected for this underlying.</td></tr>') + '</tbody>';
  $('fo-futures').querySelectorAll('tbody tr[data-s]').forEach((tr) => tr.onclick = () => openContract(tr.dataset.s));
}

function renderKeyStrikes(d) {
  if (!d.chain.length) { $('fo-keys').innerHTML = '<p class="muted">No options collected for this underlying.</p>'; return; }
  const top = (side) => [...d.chain].filter((r) => r[side + '_oi']).sort((a, b) => b[side + '_oi'] - a[side + '_oi']).slice(0, 3);
  const mx = Math.max(...d.chain.map((r) => Math.max(r.ce_oi || 0, r.pe_oi || 0)), 1);
  const block = (title, side, color, hint) => `<h4 style="margin:6px 0 4px;font-size:12px;color:var(--muted);font-weight:500">${title} <span class="muted">· ${hint}</span></h4>
    <ol class="rank">${top(side).map((r, i) => `<li data-s="${esc(r[side + '_symbol'])}"><span class="i">${i + 1}</span><span class="sym">${fmtInt(r.strike)} ${side.toUpperCase()}</span><span>${fmtBig(r[side + '_oi'])}</span>
      <span class="bar"><i style="width:${(r[side + '_oi'] / mx * 100).toFixed(1)}%;background:${color}"></i></span></li>`).join('')}</ol>`;
  $('fo-keys').innerHTML = block('Calls', 'ce', 'var(--teal)', 'often read as resistance') + block('Puts', 'pe', 'var(--orange)', 'often read as support');
  $('fo-keys').querySelectorAll('li').forEach((li) => li.onclick = () => openContract(li.dataset.s));
}

function strikesAround(d, n) {
  if (!n || !d.chain.length) return d.chain;
  let i = d.chain.findIndex((r) => r.strike === d.atm_strike);
  if (i < 0) i = Math.floor(d.chain.length / 2);
  const lo = Math.max(0, Math.min(i - n, d.chain.length - (2 * n + 1)));   // near an end of the ladder, borrow rows from the other side
  return d.chain.slice(lo, lo + 2 * n + 1);
}

function renderChain(d) {
  $('fo-chain-title').innerHTML = `Option chain<span class="hint">${esc(d.underlying)} · ${d.expiry ? niceDate(d.expiry) : 'no option expiry collected'}</span>`;
  const rows = strikesAround(d, state.range);
  const mxc = Math.max(...rows.map((r) => r.ce_oi || 0), 1), mxp = Math.max(...rows.map((r) => r.pe_oi || 0), 1);
  const hasGk = rows.some((r) => r.ce_iv != null || r.pe_iv != null || r.ce_delta != null || r.pe_delta != null);
  const cell = (sym, html, cls) => sym ? `<td class="${cls}" data-s="${esc(sym)}">${html}</td>` : `<td class="${cls} na">–</td>`;
  const body = rows.map((r) => {
    const itm = (d.spot != null ? (r.strike < d.spot ? ' itm-ce' : r.strike > d.spot ? ' itm-pe' : '') : '');
    return `<tr class="${r.strike === d.atm_strike ? 'atm' : ''}${itm}">` +
      cell(r.ce_symbol, fmtBig(r.ce_oi), 'ce oi" style="--w:' + ((r.ce_oi || 0) / mxc * 100).toFixed(1) + '%') +
      cell(r.ce_symbol, r.ce_oi_change == null ? '–' : signed(r.ce_oi_change, fmtBig), 'ce opt') +
      cell(r.ce_symbol, fmtBig(r.ce_volume), 'ce opt') +
      (hasGk ? cell(r.ce_symbol, fmt(r.ce_iv, 1), 'ce opt gk') + cell(r.ce_symbol, fmt(r.ce_delta, 2), 'ce opt gk') : '') + cell(r.ce_symbol, fmt(r.ce_close), 'ce') +
      `<td class="strike">${fmtInt(r.strike)}</td>` +
      cell(r.pe_symbol, fmt(r.pe_close), 'pe') +
      (hasGk ? cell(r.pe_symbol, fmt(r.pe_delta, 2), 'pe opt gk') + cell(r.pe_symbol, fmt(r.pe_iv, 1), 'pe opt gk') : '') + cell(r.pe_symbol, fmtBig(r.pe_volume), 'pe opt') +
      cell(r.pe_symbol, r.pe_oi_change == null ? '–' : signed(r.pe_oi_change, fmtBig), 'pe opt') +
      cell(r.pe_symbol, fmtBig(r.pe_oi), 'pe oi" style="--w:' + ((r.pe_oi || 0) / mxp * 100).toFixed(1) + '%') + '</tr>';
  }).join('');
  $('fo-chain').innerHTML = `<thead><tr><th>Call OI</th><th class="opt">OI chg</th><th class="opt">Volume</th>${hasGk ? '<th class="opt">IV</th><th class="opt">&Delta;</th>' : ''}<th>Call last</th><th class="strike">Strike</th>
    <th>Put last</th>${hasGk ? '<th class="opt">&Delta;</th><th class="opt">IV</th>' : ''}<th class="opt">Volume</th><th class="opt">OI chg</th><th>Put OI</th></tr></thead>
    <tbody>${body || '<tr><td colspan="13" class="muted">No options collected for this expiry.</td></tr>'}</tbody>`;
  $('fo-chain').querySelectorAll('td[data-s]').forEach((td) => td.onclick = () => openContract(td.dataset.s));
  centreChain();
}
function centreChain() {                               // scroll only the table (never the page) so the money row is in view
  const atm = $('fo-chain').querySelector('tr.atm'), box = $('fo-chain').parentElement;
  if (atm && box.clientHeight) box.scrollTop = Math.max(0, atm.offsetTop - box.clientHeight / 2 + atm.offsetHeight / 2);
}

function oiRows(d) {
  if (state.focus === 'all') return d.chain;
  if (state.focus === 'atm') return strikesAround(d, 10);
  const top = [...d.chain].sort((a, b) => ((b.ce_oi || 0) + (b.pe_oi || 0)) - ((a.ce_oi || 0) + (a.pe_oi || 0))).slice(0, 15);
  return top.sort((a, b) => a.strike - b.strike);
}

function renderOi(d) {
  const T = theme(), rows = oiRows(d), strikes = rows.map((r) => fmtInt(r.strike));
  const all = state.focus === 'all';
  const h = Math.max(phone() ? 360 : 420, all ? rows.length * 22 + 60 : 0);
  $('chart-oi').style.height = h + 'px';
  $('oi-scroll').classList.toggle('scroll-y', all);
  const atmLabel = d.atm_strike != null ? fmtInt(d.atm_strike) : null;
  const shapes = atmLabel && strikes.includes(atmLabel) ? [{ type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y', y0: atmLabel, y1: atmLabel, line: { color: T.acc, width: 1, dash: 'dot' } }] : [];
  const bar = (side, color, sign) => ({ type: 'bar', orientation: 'h', y: strikes, x: rows.map((r) => sign * (r[side + '_oi'] || 0)), name: side === 'ce' ? 'Calls' : 'Puts',
    marker: { color }, customdata: rows.map((r) => r[side + '_oi'] || 0), hovertemplate: `Strike %{y}<br>${side === 'ce' ? 'Call' : 'Put'} OI %{customdata:,}<extra></extra>` });
  plot('chart-oi', [bar('ce', T.teal, -1), bar('pe', T.orange, 1)], baseLayout({
    barmode: 'overlay', bargap: 0.25, hovermode: 'closest', dragmode: false, shapes, height: h,
    xaxis: valAxis({ tickformat: '~s', title: { text: '← calls · puts →', font: { size: 11 } } }),
    yaxis: valAxis({ type: 'category', categoryorder: 'array', categoryarray: strikes, tickfont: { size: 11 } }),
    margin: { l: phone() ? 58 : 72, r: 16, t: 8, b: 44 } }),
    [{ name: 'Calls', color: T.teal, kind: 'bar', traces: [0] }, { name: 'Puts', color: T.orange, kind: 'bar', traces: [1] }], Object.assign(config(), { displayModeBar: false }));
  const chg = (side, color) => ({ type: 'bar', orientation: 'h', y: strikes, x: rows.map((r) => r[side + '_oi_change'] || 0), name: side === 'ce' ? 'Calls' : 'Puts',
    marker: { color }, hovertemplate: `Strike %{y}<br>${side === 'ce' ? 'Call' : 'Put'} OI change %{x:+,}<extra></extra>` });
  const hc = Math.max(phone() ? 300 : 360, rows.length * 22 + 60);
  $('chart-oichg').style.height = hc + 'px';
  plot('chart-oichg', [chg('ce', T.teal), chg('pe', T.orange)], baseLayout({
    barmode: 'group', bargap: 0.2, hovermode: 'closest', dragmode: false, shapes, height: hc,
    xaxis: valAxis({ tickformat: '~s', zerolinecolor: T.fg2 }), yaxis: valAxis({ type: 'category', categoryorder: 'array', categoryarray: strikes, tickfont: { size: 11 } }),
    margin: { l: phone() ? 58 : 72, r: 16, t: 8, b: 30 } }),
    [{ name: 'Calls', color: T.teal, kind: 'bar', traces: [0] }, { name: 'Puts', color: T.orange, kind: 'bar', traces: [1] }], Object.assign(config(), { displayModeBar: false }));
  for (const id of ['chart-oi', 'chart-oichg']) {
    const el = $(id);
    el.removeAllListeners && el.removeAllListeners('plotly_click');
    el.on('plotly_click', (ev) => {
      const r = rows[ev.points[0].pointIndex];
      const sym = r && (ev.points[0].curveNumber === 0 ? r.ce_symbol : r.pe_symbol);
      if (sym) openContract(sym);
    });
  }
}

/* ---------------------------------------------------------------- Derivatives: one contract */
const FO_TYPE = (sym, d) => {
  const f = d.futures.find((x) => x.symbol === sym);
  if (f) return { label: `${d.underlying} future · ${niceDate(f.expiry)}`, kind: 'FUT' };
  for (const r of d.chain) {
    if (r.ce_symbol === sym) return { label: `${d.underlying} ${fmtInt(r.strike)} call · ${niceDate(d.expiry)}`, kind: 'CE' };
    if (r.pe_symbol === sym) return { label: `${d.underlying} ${fmtInt(r.strike)} put · ${niceDate(d.expiry)}`, kind: 'PE' };
  }
  return { label: sym, kind: '' };
};

function fillContracts(d) {
  const groupsOut = [];
  if (d.futures.length) groupsOut.push({ group: 'Futures', items: d.futures.map((f) => [f.symbol, `Future · ${niceDate(f.expiry)}`]) });
  const near = strikesAround(d, 20);
  const ce = near.filter((r) => r.ce_symbol).map((r) => [r.ce_symbol, `${fmtInt(r.strike)} call`]);
  const pe = near.filter((r) => r.pe_symbol).map((r) => [r.pe_symbol, `${fmtInt(r.strike)} put`]);
  if (ce.length) groupsOut.push({ group: `Calls · ${niceDate(d.expiry)}`, items: ce });
  if (pe.length) groupsOut.push({ group: `Puts · ${niceDate(d.expiry)}`, items: pe });
  const flat = groupsOut.flatMap((g) => g.items.map(([v]) => v));
  const atmCe = (d.chain.find((r) => r.strike === d.atm_strike) || {}).ce_symbol;
  const sel = flat.includes(state.foContract) ? state.foContract : (d.futures[0] ? d.futures[0].symbol : atmCe || flat[0] || null);
  fillSelect($('fo-contract'), groupsOut, sel);
  const optGroups = groupsOut.filter((g) => g.group !== 'Futures');
  const optFlat = optGroups.flatMap((g) => g.items.map(([v]) => v));
  fillSelect($('fo-greeks-contract'), optGroups, optFlat.includes(sel) ? sel : (atmCe || optFlat[0] || null));
  state.foContract = sel; state.foContractData = null; state.greeksData = null;
  if (sel && state.sub.fo === 'contract') loadFoContract(sel);
  if (state.sub.fo === 'greeks') loadGreeks();
}

function openContract(sym) {
  if (!sym) return;
  state.foContract = sym;
  if (![...$('fo-contract').options].some((o) => o.value === sym)) {
    const o = document.createElement('option'); o.value = sym; o.textContent = FO_TYPE(sym, state.foData).label; $('fo-contract').appendChild(o);
  }
  $('fo-contract').value = sym;
  if (OPTION_RE.test(sym)) {
    if (![...$('fo-greeks-contract').options].some((o) => o.value === sym)) {
      const o = document.createElement('option'); o.value = sym; o.textContent = FO_TYPE(sym, state.foData).label; $('fo-greeks-contract').appendChild(o);
    }
    $('fo-greeks-contract').value = sym;
  }
  state.greeksData = null;
  showSub('fo', 'contract');
  loadFoContract(sym);
}

async function loadFoContract(sym) {
  if (!sym) return;
  const id = ++fcReq;
  writeHash();
  setStatus('Loading ' + sym + '…', 'busy');
  try {
    const d = await api('/api/day', { date: state.day, symbol: sym, width: state.width, vol_window: state.volwin });
    if (id !== fcReq) return;
    state.foContractData = d;
    const s = d.summary;
    $('fo-contract-meta').textContent = FO_TYPE(sym, state.foData).label + ` · ${WIDTH_LABEL[d.width]} bars`;
    renderKpis('fo-contract-cards', [
      ['Last', fmt(s.close), `${signed(s.return_pct, (x) => fmt(x) + '%')} on the open`],
      ['Day high', fmt(s.high), `low ${fmt(s.low)} · VWAP ${fmt(s.vwap)}`],
      ['Volume', fmtBig(s.volume), `${fmtInt(s.bars)} bars`],
      ['Open interest', fmtBig(s.oi_last), s.oi_change == null ? '' : `${signed(s.oi_change, fmtBig)} today`],
      ['Spread', fmt(s.median_spread_bps, 2) + ' bps', `book on ${fmt(s.book_coverage_pct, 0)}%`],
      ['Realised vol', fmt(s.realised_vol_pct, 1) + '%', 'annualised'],
    ]);
    renderSeries(d, FC);
    setStatus(`${sym} · ${state.day} · ${fmtInt(d.series.ts.length)} bars of ${WIDTH_LABEL[d.width]} · times in IST`);
  } catch (e) { if (id === fcReq) fail(e); }
}

/* ---------------------------------------------------------------- Greeks (options only) */
const OPTION_RE = /_(CE|PE)_/;
let gkReq = 0;
function clearGreeks() { ['iv', 'delta', 'gamma', 'theta', 'vega', 'rho'].forEach((k) => { const el = $('gk-' + k); if (el.data) Plotly.purge(el); }); $('fo-greeks-cards').innerHTML = ''; }
async function loadGreeks() {
  const sym = $('fo-greeks-contract').value;            // always an option; the futures belong to the Contract view
  if (!state.day) return;
  if (!sym || !OPTION_RE.test(sym)) {
    clearGreeks(); $('fo-greeks-meta').textContent = '';
    $('fo-greeks-note').textContent = 'Greeks belong to options. Pick a call or put above, or click one in the option chain (this underlying has no options collected on this day).'; return;
  }
  const id = ++gkReq;
  setStatus('Loading Greeks for ' + sym + '…', 'busy');
  try {
    const d = await api('/api/fo/greeks', { date: state.day, symbol: sym, width: state.width });
    if (id !== gkReq) return;
    state.greeksData = d;
    $('fo-greeks-meta').textContent = FO_TYPE(sym, state.foData).label + ` · ${WIDTH_LABEL[d.width]} bars`;
    if (!d.available || !d.series.ts || !d.series.ts.length) {
      clearGreeks();
      $('fo-greeks-note').textContent = 'No Greeks were stored for this option on this day. They are collected from the day the feed first ran with Greeks storage on (JARVIS_GREEKS), so earlier days have none.';
      setStatus(`${sym} · ${state.day} · no Greeks stored`); return;
    }
    $('fo-greeks-note').textContent = 'Values are the last reading in each bar, as delivered by the exchange feed (not recomputed here).';
    renderGreeks(d);
    setStatus(`${sym} · ${state.day} · ${fmtInt(d.series.ts.length)} bars of ${WIDTH_LABEL[d.width]} · times in IST`);
  } catch (e) { if (id === gkReq) fail(e); }
}
function renderGreeks(d) {
  const s = d.series, t = s.ts, T = theme(), step = d.width <= 5 ? 'hv' : 'linear';
  const defs = [['iv', 'IV', T.purple, '%', ',.2f'], ['delta', 'Delta', T.acc, '', '.3f'], ['gamma', 'Gamma', T.teal, '', '.5f'],
                ['theta', 'Theta', T.down, '', ',.2f'], ['vega', 'Vega', T.yellow, '', ',.2f'], ['rho', 'Rho', T.orange, '', ',.2f']];
  const last = (a) => { for (let i = a.length - 1; i >= 0; i--) if (a[i] != null) return a[i]; return null; };
  renderKpis('fo-greeks-cards', [['IV', fmt(last(s.iv), 1) + '%', 'implied volatility'], ['Delta', fmt(last(s.delta), 3), ''], ['Gamma', fmt(last(s.gamma), 5), ''],
    ['Theta', fmt(last(s.theta), 2), 'per day'], ['Vega', fmt(last(s.vega), 2), 'per vol point'], ['Rho', fmt(last(s.rho), 2), 'per rate point']]);
  const ids = [];
  defs.forEach(([k, name, color, suffix, tf]) => {
    const id = 'gk-' + k;
    plot(id, [{ x: t, y: s[k], name, type: 'scattergl', mode: 'lines', line: { color, width: 1.6, shape: step } }],
      baseLayout({ hovermode: 'x unified', xaxis: timeAxis(), yaxis: valAxis({ tickformat: tf, ticksuffix: suffix }) }),
      [{ name, color, traces: [0] }]);
    ids.push(id);
  });
  syncGroup('gk', ids);
  requestAnimationFrame(resizeVisible);
}

/* ---------------------------------------------------------------- live five-level book (memory only, never stored) */
const bookMode = { eq: 'cursor', fc: 'cursor' };
const BOOKS = { eq: { body: 'book-body', symbol: () => state.symbol, data: () => state.data, ids: EQ },
                fc: { body: 'fo-contract-book-body', symbol: () => state.foContract, data: () => state.foContractData, ids: FC } };
const liveBusy = {};
function ladderHtml(b) {
  const mx = Math.max(1, ...b.bids.map((x) => x[1]), ...b.asks.map((x) => x[1]));
  const row = (cls, [p, q]) => `<tr class="${cls}"><td class="q" style="--w:${(q / mx * 100).toFixed(0)}%">${fmtInt(q)}</td><td class="px">${fmt(p)}</td></tr>`;
  const bb = b.bids[0], ba = b.asks[0];
  const sum = (a) => a.reduce((t, x) => t + x[1], 0), tb = sum(b.bids), ta = sum(b.asks);
  const kv = [['Spread', bb && ba ? `${fmt(ba[0] - bb[0])} · ${fmt((ba[0] - bb[0]) / ((ba[0] + bb[0]) / 2) * 1e4, 2)} bps` : '–'],
              ['Bid size (5)', fmtInt(tb)], ['Ask size (5)', fmtInt(ta)], ['Imbalance (5)', tb + ta ? fmt((tb - ta) / (tb + ta), 2) : '–'], ['Last', fmt(b.ltp)]];
  const at = new Date(b.ts).toLocaleTimeString('en-GB', { timeZone: 'Asia/Kolkata', hour12: false });
  return `<div class="ts">${at} IST · ${b.age_s < 1 ? 'just now' : fmt(b.age_s, 0) + ' s ago'} · live, not stored</div>
    <table class="ladder5"><tr class="hd"><td class="muted">Size</td><td class="px muted">Price</td></tr>${b.asks.slice(0, 5).reverse().map((x) => row('ask', x)).join('')}
    <tr class="mid"><td colspan="2">${bb && ba ? 'spread ' + fmt(ba[0] - bb[0]) : ''}</td></tr>${b.bids.slice(0, 5).map((x) => row('bid', x)).join('')}</table>
    <dl class="kv">${kv.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join('')}</dl>`;
}
async function pollLive(key) {
  const B = BOOKS[key], sym = B.symbol(), el = $(B.body);
  if (!sym || liveBusy[key]) return;
  liveBusy[key] = true;
  try {
    const b = await api('/api/live_book', { symbol: sym });
    if (bookMode[key] === 'live' && sym === B.symbol()) el.innerHTML = ladderHtml(b);
  } catch (e) {
    if (bookMode[key] === 'live') el.innerHTML = `<p class="muted">Live depth needs the feed to be running during market hours.<br>${esc(e.message)}</p>`;
  } finally { liveBusy[key] = false; }
}
function setBookMode(key, mode) {
  bookMode[key] = mode;
  document.querySelectorAll(`.seg[data-book="${key}"] button`).forEach((b) => b.setAttribute('aria-pressed', b.dataset.mode === mode));
  const B = BOOKS[key];
  if (mode === 'live') { $(B.body).innerHTML = '<p class="muted">Waiting for the first update…</p>'; pollLive(key); }
  else { const d = B.data(); if (d) bookAt(d, d.series.ts.length - 1, B.ids); else $(B.body).textContent = 'Hover a chart to see the best bid and ask at that second.'; }
}
document.querySelectorAll('.seg').forEach((seg) => seg.querySelectorAll('button').forEach((b) => b.onclick = () => setBookMode(seg.dataset.book, b.dataset.mode)));
setInterval(() => {                                        // one timer; only cards that are on screen and switched to live poll
  if (document.hidden) return;
  for (const key of Object.keys(BOOKS)) if (bookMode[key] === 'live' && $(BOOKS[key].body).offsetParent !== null) pollLive(key);
}, 1000);

/* ---------------------------------------------------------------- wiring */
$('date').onchange = (e) => { state.day = e.target.value; state.foContractData = null; state.greeksData = null; setTab(state.tab); };
$('symbol').onchange = (e) => { state.symbol = e.target.value; refreshEquity(); };
const setWidth = (v) => { state.width = +v; $('width').value = v; $('fo-width').value = v; $('fo-greeks-width').value = v; state.greeksData = null; };
const setWin = (v) => { state.volwin = +v; $('volwin').value = v; $('fo-volwin').value = v; };
$('width').onchange = (e) => { setWidth(e.target.value); refreshEquity(); };
$('volwin').onchange = (e) => { setWin(e.target.value); refreshEquity(); };
$('fo-width').onchange = (e) => { setWidth(e.target.value); loadFoContract(state.foContract); };
$('fo-volwin').onchange = (e) => { setWin(e.target.value); loadFoContract(state.foContract); };
$('heat-n').onchange = (e) => { state.heatN = +e.target.value; renderHeat(); };
$('heat-metric').onchange = (e) => { state.heatMetric = e.target.value; renderHeat(); };
$('underlying').onchange = (e) => { state.underlying = e.target.value; state.expiry = null; state.foContract = null; refreshFo(); };
$('expiry').onchange = (e) => { state.expiry = e.target.value; refreshFo(); };
$('chain-range').onchange = (e) => { state.range = +e.target.value; writeHash(); if (state.foData) renderChain(state.foData); };
$('oi-focus').onchange = (e) => { state.focus = e.target.value; writeHash(); if (state.foData) renderOi(state.foData); };
$('fo-contract').onchange = (e) => { state.foContract = e.target.value; state.greeksData = null; loadFoContract(state.foContract); };
$('fo-greeks-width').onchange = (e) => { setWidth(e.target.value); loadGreeks(); };
$('fo-greeks-contract').onchange = (e) => {
  state.foContract = e.target.value; state.foContractData = null; state.greeksData = null;
  if ([...$('fo-contract').options].some((o) => o.value === state.foContract)) $('fo-contract').value = state.foContract;
  loadGreeks();
};
for (const t of Object.keys(SUBS)) $('tab-' + t).onclick = () => setTab(t);
let lastPhone = phone();
window.addEventListener('resize', () => {                // re-layout charts when crossing the phone breakpoint
  if (phone() === lastPhone) return;
  lastPhone = phone();
  if (state.tab === 'instrument' && state.data) renderSeries(state.data, EQ);
  if (state.tab === 'overview' && state.overview.length) renderOverview();
  if (state.tab === 'fo' && state.foData) { renderFo(state.foData); if (state.foContractData) renderSeries(state.foContractData, FC); }
});
init();
