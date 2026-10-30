/* JARVIS intraday viewer: plain JS + Plotly (served locally from /vendor/plotly.min.js). */
const $ = (id) => document.getElementById(id);
const css = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
const fmt = (x, d = 2) => (x == null ? '–' : Number(x).toLocaleString('en-IN', { minimumFractionDigits: d, maximumFractionDigits: d }));
const fmtInt = (x) => (x == null ? '–' : Math.round(x).toLocaleString('en-IN'));
const WIDTH_LABEL = { 1: '1 second', 5: '5 seconds', 15: '15 seconds', 30: '30 seconds', 60: '1 minute', 300: '5 minutes' };
const VOLWIN_LABEL = { 60: '1 minute', 300: '5 minutes', 900: '15 minutes', 1800: '30 minutes' };

const state = { meta: null, day: null, symbol: null, width: 1, volwin: 300, tab: 'instrument', data: null, overview: [], sort: ['volume', -1], underlying: null, expiry: null, foData: null };
let reqId = 0;

async function api(path, params) {
  const url = path + '?' + new URLSearchParams(params);
  const r = await fetch(url);
  if (!r.ok) {
    let msg = r.statusText;
    try { msg = (await r.json()).detail || msg; } catch (e) { /* not json */ }
    throw new Error(msg);
  }
  return r.json();
}
const setStatus = (t, err = false) => { const s = $('status'); s.textContent = t; s.style.color = err ? css('--down') : css('--muted'); };
const fillSelect = (el, items, sel) => { el.innerHTML = items.map(([v, l]) => `<option value="${v}">${l}</option>`).join(''); if (sel != null) el.value = sel; };

function readHash() {
  const p = new URLSearchParams(location.hash.slice(1));
  return { day: p.get('date'), symbol: p.get('symbol'), tab: p.get('tab'), width: +p.get('width') || null, volwin: +p.get('volwin') || null,
          underlying: p.get('underlying'), expiry: p.get('expiry') };
}
function writeHash() {
  const p = new URLSearchParams({ date: state.day || '', symbol: state.symbol || '', tab: state.tab, width: state.width, volwin: state.volwin,
                                  underlying: state.underlying || '', expiry: state.expiry || '' });
  history.replaceState(null, '', '#' + p.toString());
}

async function init() {
  const h = readHash();
  try { state.meta = await api('/api/meta', {}); } catch (e) { setStatus('Cannot reach the API: ' + e.message, true); return; }
  $('demo-banner').hidden = !state.meta.demo;
  fillSelect($('width'), state.meta.widths.map((w) => [w, WIDTH_LABEL[w] || w + ' s']), (state.width = state.meta.widths.includes(h.width) ? h.width : 1));
  fillSelect($('volwin'), state.meta.vol_windows.map((w) => [w, VOLWIN_LABEL[w] || w + ' s']), (state.volwin = state.meta.vol_windows.includes(h.volwin) ? h.volwin : 300));
  if (!state.meta.days.length) { $('empty').hidden = false; $('view-instrument').hidden = true; setStatus('No data'); return; }
  fillSelect($('date'), state.meta.days.map((d) => [d, d]), (state.day = state.meta.days.includes(h.day) ? h.day : state.meta.days[0]));
  state.symbol = h.symbol; state.underlying = h.underlying; state.expiry = h.expiry;
  const tab = h.tab === 'overview' ? 'overview' : h.tab === 'fo' ? 'fo' : 'instrument';
  setTab(tab, false);
  if (tab === 'fo') await loadFoUnderlyings(); else await loadSymbols();
}

async function loadSymbols() {
  setStatus('Loading instruments…');
  const syms = await api('/api/symbols', { date: state.day });
  fillSelect($('symbol'), syms.map((s) => [s.symbol, `${s.symbol}  (${fmtInt(s.bars)} bars)`]));
  if (!syms.find((s) => s.symbol === state.symbol)) state.symbol = syms[0].symbol;
  $('symbol').value = state.symbol;
  await refresh();
}

async function refresh() {
  const id = ++reqId;
  writeHash();
  try {
    if (state.tab === 'overview') {
      setStatus('Loading overview…');
      const rows = await api('/api/overview', { date: state.day });
      if (id !== reqId) return;
      state.overview = rows; renderOverview(); setStatus(`${rows.length} instruments · ${state.day}`);
    } else {
      setStatus('Loading ' + state.symbol + '…');
      const d = await api('/api/day', { date: state.day, symbol: state.symbol, width: state.width, vol_window: state.volwin });
      if (id !== reqId) return;
      state.data = d; renderInstrument(d); setStatus(`${state.symbol} · ${state.day} · ${fmtInt(d.series.ts.length)} bars of ${WIDTH_LABEL[d.width]} (times in IST)`);
    }
  } catch (e) { if (id === reqId) setStatus(e.message, true); }
}

function setTab(tab, reload = true) {
  state.tab = tab;
  $('tab-instrument').setAttribute('aria-selected', tab === 'instrument');
  $('tab-overview').setAttribute('aria-selected', tab === 'overview');
  $('tab-fo').setAttribute('aria-selected', tab === 'fo');
  $('view-instrument').hidden = tab !== 'instrument';
  $('view-overview').hidden = tab !== 'overview';
  $('view-fo').hidden = tab !== 'fo';
  for (const id of ['symbol', 'width', 'volwin']) $(id).closest('label').style.display = tab === 'instrument' ? '' : 'none';
  for (const id of ['underlying', 'expiry']) $(id).closest('label').style.display = tab === 'fo' ? '' : 'none';
  if (reload) {
    if (tab === 'fo') loadFoUnderlyings().catch((e) => setStatus(e.message, true));
    else refresh();
  }
}

function jumpToSymbol(sym) {
  if (!sym) return;
  state.symbol = sym;
  setTab('instrument', false);
  // /api/symbols is equities-only now (F&O has its own tab), so an F&O contract would not survive a
  // loadSymbols() refresh: add it to the dropdown directly instead, same as a normal equity jump.
  if (!$('symbol').querySelector(`option[value="${sym}"]`)) {
    const opt = document.createElement('option');
    opt.value = sym; opt.textContent = sym + ' (F&O)';
    $('symbol').prepend(opt);
  }
  $('symbol').value = sym;
  refresh();
}

/* ---------- instrument view ---------- */
function cards(s) {
  const sign = (v) => (v == null ? '' : v >= 0 ? 'pos' : 'neg');
  const items = [
    ['Open', fmt(s.open)], ['High', fmt(s.high)], ['Low', fmt(s.low)], ['Close', fmt(s.close)],
    ['Return', `<span class="${sign(s.return_pct)}">${fmt(s.return_pct)}%</span>`],
    ['Volume', fmtInt(s.volume)], ['VWAP', fmt(s.vwap)],
    ['Avg spread', fmt(s.avg_spread_bps, 2) + ' bps'], ['Median spread', fmt(s.median_spread_bps, 2) + ' bps'],
    ['Realised vol', fmt(s.realised_vol_pct, 1) + '%'], ['Avg bid size', fmtInt(s.avg_bid_qty)], ['Avg ask size', fmtInt(s.avg_ask_qty)],
    ['Bars', fmtInt(s.bars)], ['Book coverage', fmt(s.book_coverage_pct, 0) + '%'],
  ];
  if (s.oi_last != null) items.push(['Open interest', fmtInt(s.oi_last)], ['OI change', `<span class="${sign(s.oi_change)}">${s.oi_change >= 0 ? '+' : ''}${fmtInt(s.oi_change)}</span>`]);
  $('cards').innerHTML = items.map(([k, v]) => `<div class="card"><b>${k}</b><span>${v}</span></div>`).join('');
}

function renderInstrument(d) {
  const s = d.series, S = d.summary;
  cards(S);
  const fg = css('--fg'), grid = css('--grid'), up = css('--up'), down = css('--down'), acc = css('--accent'), muted = css('--muted'), orange = css('--orange'), purple = css('--purple'), teal = css('--teal');
  const step = d.width <= 5 ? 'hv' : 'linear';
  // five stacked panels sharing one time axis; domains from the bottom up
  const gaps = 0.025, h = [0.30, 0.17, 0.18, 0.14, 0.14];
  const total = h.reduce((a, b) => a + b, 0);
  const doms = []; let y = 1;
  h.forEach((x) => { const span = x / total * (1 - gaps * (h.length - 1)); doms.push([y - span, y]); y -= span + gaps; });
  const t = s.ts;
  const nb = d.has_book ? true : 'legendonly';
  const p99 = (arr) => { const v = arr.filter((x) => x != null).sort((a, b) => a - b); return v.length ? v[Math.floor(v.length * 0.99)] : 1; };
  const qmax = Math.max(p99(s.bid_qty), p99(s.ask_qty), 1) * 1.15, smax = Math.max(p99(s.spread_bps), 0.1) * 1.2;
  const neg = (arr) => arr.map((v) => (v == null ? null : -v));
  const tr = [
    { x: t, y: s.bid, name: 'Best bid', type: 'scattergl', mode: 'lines', line: { color: up, width: 0.7, shape: step }, opacity: 0.8, xaxis: 'x', yaxis: 'y', visible: nb },
    { x: t, y: s.ask, name: 'Best ask', type: 'scattergl', mode: 'lines', line: { color: down, width: 0.7, shape: step }, opacity: 0.8, xaxis: 'x', yaxis: 'y', visible: nb },
    { x: t, y: s.close, name: 'Close', type: 'scattergl', mode: 'lines', line: { color: acc, width: 1.4, shape: step }, xaxis: 'x', yaxis: 'y' },
    { x: [t[0], t[t.length - 1]], y: [S.vwap, S.vwap], name: 'Day VWAP', type: 'scatter', mode: 'lines', line: { color: muted, width: 1, dash: 'dash' }, xaxis: 'x', yaxis: 'y' },
    { x: t, y: s.volume, name: 'Volume per bar', type: 'bar', marker: { color: acc, opacity: 0.6 }, xaxis: 'x', yaxis: 'y2' },
    { x: t, y: s.cum_volume, name: 'Cumulative volume', type: 'scattergl', mode: 'lines', line: { color: fg, width: 1.2 }, xaxis: 'x', yaxis: 'y3' },
    { x: t, y: s.bid_qty, name: 'Bid size (raw)', type: 'bar', marker: { color: up, opacity: 0.22 }, xaxis: 'x', yaxis: 'y4', visible: nb, showlegend: false },
    { x: t, y: neg(s.ask_qty), name: 'Ask size (raw, shown negative)', type: 'bar', marker: { color: down, opacity: 0.22 }, xaxis: 'x', yaxis: 'y4', visible: nb, showlegend: false },
    { x: t, y: s.bid_qty_avg, name: `Bid size (${VOLWIN_LABEL[d.vol_window]} avg)`, type: 'scattergl', mode: 'lines', line: { color: up, width: 1.6 }, xaxis: 'x', yaxis: 'y4', visible: nb },
    { x: t, y: neg(s.ask_qty_avg), name: `Ask size (${VOLWIN_LABEL[d.vol_window]} avg)`, type: 'scattergl', mode: 'lines', line: { color: down, width: 1.6 }, xaxis: 'x', yaxis: 'y4', visible: nb },
    { x: t, y: s.imbalance_avg, name: `Imbalance (${VOLWIN_LABEL[d.vol_window]} avg)`, type: 'scattergl', mode: 'lines', line: { color: fg, width: 1.4 }, xaxis: 'x', yaxis: 'y5', visible: nb },
    { x: t, y: s.spread_bps, name: 'Spread bps (raw)', type: 'scattergl', mode: 'lines', line: { color: orange, width: 0.5, shape: step }, opacity: 0.22, xaxis: 'x', yaxis: 'y6', visible: nb, showlegend: false },
    { x: t, y: s.spread_bps_avg, name: `Spread bps (${VOLWIN_LABEL[d.vol_window]} avg)`, type: 'scattergl', mode: 'lines', line: { color: orange, width: 1.8 }, xaxis: 'x', yaxis: 'y6', visible: nb },
    ...(d.has_oi && s.oi ? [{ x: t, y: s.oi, name: 'Open interest', type: 'scattergl', mode: 'lines', line: { color: teal, width: 1.4, shape: step }, xaxis: 'x', yaxis: 'y8' }] : []),
    { x: t, y: s.vol_ann, name: `Realised vol (${VOLWIN_LABEL[d.vol_window]} window, annualised %)`, type: 'scattergl', mode: 'lines', line: { color: purple, width: 1.5 }, xaxis: 'x', yaxis: 'y7' },
  ];
  const ax = (domain, title, extra = {}) => Object.assign({ domain, title: { text: title, font: { size: 11 } }, gridcolor: grid, zerolinecolor: grid, linecolor: grid, tickfont: { size: 10 } }, extra);
  const layout = {
    height: window.innerWidth < 600 ? 900 : 1000, margin: { l: 62, r: d.has_oi && s.oi ? 84 : 52, t: window.innerWidth >= 700 ? 70 : 10, b: 40 }, paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
    font: { color: fg, family: '-apple-system, BlinkMacSystemFont, Inter, Helvetica Neue, sans-serif' }, hoverlabel: { bgcolor: '#1d1d1f', bordercolor: 'rgba(255,255,255,.2)', font: { color: fg } }, hovermode: 'x', showlegend: window.innerWidth >= 700, legend: { orientation: 'h', yanchor: 'bottom', y: 1.0, font: { size: 10 } }, barmode: 'relative', bargap: 0,
    xaxis: { anchor: 'y7', domain: [0, 1], gridcolor: grid, linecolor: grid, tickfont: { size: 10 }, showspikes: true, spikemode: 'across', spikethickness: 1, spikecolor: muted, title: { text: 'Time (IST)', font: { size: 11 } } },
    yaxis: ax(doms[0], 'Price (₹)', { anchor: 'x' }),
    yaxis2: ax(doms[1], 'Volume / bar', { anchor: 'x' }),
    yaxis3: { overlaying: 'y2', side: 'right', showgrid: false, rangemode: 'tozero', title: { text: 'Cumulative', font: { size: 11 } }, tickfont: { size: 10 }, anchor: 'x' },
    yaxis4: ax(doms[2], 'Size at best (bid +, ask −)', { anchor: 'x', range: [-qmax, qmax] }),
    yaxis5: { overlaying: 'y4', side: 'right', range: [-1, 1], tickvals: [-1, -0.5, 0, 0.5, 1], showgrid: false, title: { text: 'Imbalance', font: { size: 11 } }, tickfont: { size: 10 }, anchor: 'x' },
    yaxis6: ax(doms[3], 'Spread (bps)', { anchor: 'x', range: [0, smax] }),
    yaxis7: ax(doms[4], 'Realised vol (%)', { anchor: 'x', rangemode: 'tozero' }),
    yaxis8: { overlaying: 'y', side: 'right', showgrid: false, title: { text: 'Open interest', font: { size: 11 }, standoff: 8 }, tickfont: { size: 10 }, anchor: 'x', visible: !!(d.has_oi && s.oi) },
  };
  // y4's overlay (imbalance) must be declared after its base axis; Plotly handles it, but cumulative uses y2's domain
  Plotly.react('chart', tr, layout, { responsive: true, displaylogo: false, scrollZoom: true, modeBarButtonsToRemove: ['select2d', 'lasso2d'] });
  const el = $('chart');
  el.removeAllListeners && el.removeAllListeners('plotly_hover');
  el.on('plotly_hover', (ev) => bookAt(ev.points[0].pointIndex));
  $('note').textContent = d.has_book ? '' : 'This day has no book data (bars from the REST candle API carry no bid/ask), so the book, spread and imbalance panels are empty.';
  bookAt(Math.floor(t.length / 2));
}

function bookAt(i) {
  const d = state.data; if (!d) return;
  const s = d.series;
  if (i == null || s.ts[i] == null) return;
  const row = (cls, label, px, q) => `<tr class="${cls}"><td>${label}</td><td>${px == null ? '–' : fmt(px)}</td><td>${q == null ? '–' : fmtInt(q)}</td></tr>`;
  $('book-body').innerHTML = `<div class="muted" style="margin-bottom:6px">${s.ts[i]} IST</div>
    <table class="ladder"><tr class="muted"><td></td><td>Price</td><td>Size</td></tr>
    ${row('ask', 'Ask', s.ask[i], s.ask_qty[i])}${row('bid', 'Bid', s.bid[i], s.bid_qty[i])}</table>
    <div style="margin-top:8px">Spread <b>${s.spread[i] == null ? '–' : fmt(s.spread[i])}</b> (${fmt(s.spread_bps[i], 2)} bps)<br>
    Mid <b>${fmt(s.mid[i])}</b> · Imbalance <b>${s.imbalance[i] == null ? '–' : fmt(s.imbalance[i], 2)}</b><br>
    Close <b>${fmt(s.close[i])}</b> · Volume <b>${fmtInt(s.volume[i])}</b><br>
    ${s.oi && s.oi[i] != null ? `Open interest <b>${fmtInt(s.oi[i])}</b><br>` : ''}Vol (${VOLWIN_LABEL[d.vol_window]}) <b>${s.vol_ann[i] == null ? '–' : fmt(s.vol_ann[i], 1) + '%'}</b></div>`;
}

/* ---------- overview ---------- */
const COLS = [
  ['symbol', 'Instrument', (v) => v], ['bars', 'Bars', fmtInt], ['volume', 'Volume', fmtInt], ['vwap', 'VWAP', fmt], ['close', 'Close', fmt],
  ['return_pct', 'Return %', fmt], ['avg_spread_bps', 'Avg spread bps', fmt], ['median_spread_bps', 'Median spread bps', fmt],
  ['realised_vol_pct', 'Realised vol %', (v) => fmt(v, 1)], ['avg_bid_qty', 'Avg bid size', fmtInt], ['avg_ask_qty', 'Avg ask size', fmtInt],
];
function renderOverview() {
  const rows = [...state.overview];
  const [key, dir] = state.sort;
  rows.sort((a, b) => (a[key] > b[key] ? 1 : a[key] < b[key] ? -1 : 0) * dir * (key === 'symbol' ? -1 : 1));
  $('table').innerHTML = '<thead><tr>' + COLS.map(([k, l]) => `<th data-k="${k}">${l}${state.sort[0] === k ? (state.sort[1] < 0 ? ' ▼' : ' ▲') : ''}</th>`).join('') + '</tr></thead><tbody>' +
    rows.map((r) => `<tr data-s="${r.symbol}">` + COLS.map(([k, , f]) => `<td>${f(r[k])}</td>`).join('') + '</tr>').join('') + '</tbody>';
  $('table').querySelectorAll('th').forEach((th) => th.onclick = () => { const k = th.dataset.k; state.sort = [k, state.sort[0] === k ? -state.sort[1] : -1]; renderOverview(); });
  $('table').querySelectorAll('tbody tr').forEach((tr) => tr.onclick = () => { state.symbol = tr.dataset.s; $('symbol').value = state.symbol; setTab('instrument'); });
  const fg = css('--fg'), grid = css('--grid'), acc = css('--accent'), purple = css('--purple');
  const byVol = [...state.overview].sort((a, b) => b.realised_vol_pct - a.realised_vol_pct);
  Plotly.react('chart-vol', [{ type: 'bar', x: byVol.map((r) => r.symbol), y: byVol.map((r) => r.realised_vol_pct), marker: { color: purple }, hovertemplate: '%{x}<br>%{y:.1f}%<extra></extra>' }],
    { title: { text: 'Realised volatility by instrument (annualised %)', font: { size: 13 } }, margin: { l: 50, r: 10, t: 36, b: 90 }, paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)', font: { color: fg, family: '-apple-system, BlinkMacSystemFont, Inter, Helvetica Neue, sans-serif' }, hoverlabel: { bgcolor: '#1d1d1f', bordercolor: 'rgba(255,255,255,.2)', font: { color: fg } }, yaxis: { gridcolor: grid }, xaxis: { tickangle: -45 } }, { responsive: true, displaylogo: false });
  const mx = Math.max(...state.overview.map((r) => r.volume), 1);
  Plotly.react('chart-scatter', [{ type: 'scatter', mode: 'markers+text', x: state.overview.map((r) => r.realised_vol_pct), y: state.overview.map((r) => r.avg_spread_bps),
    text: state.overview.map((r) => r.symbol.replace('.NS', '')), textposition: 'top center', textfont: { size: 9 },
    marker: { size: state.overview.map((r) => 10 + 30 * Math.sqrt(r.volume / mx)), color: acc, opacity: 0.6 }, hovertemplate: '%{text}<br>vol %{x:.1f}%<br>spread %{y:.2f} bps<extra></extra>' }],
    { title: { text: 'Spread vs volatility (bubble = volume)', font: { size: 13 } }, margin: { l: 55, r: 10, t: 36, b: 45 }, paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)', font: { color: fg, family: '-apple-system, BlinkMacSystemFont, Inter, Helvetica Neue, sans-serif' }, hoverlabel: { bgcolor: '#1d1d1f', bordercolor: 'rgba(255,255,255,.2)', font: { color: fg } },
      xaxis: { title: { text: 'Realised vol (%)', font: { size: 11 } }, gridcolor: grid }, yaxis: { title: { text: 'Avg spread (bps)', font: { size: 11 } }, gridcolor: grid, rangemode: 'tozero' } }, { responsive: true, displaylogo: false });
}

/* ---------- futures & options ---------- */
async function loadFoUnderlyings() {
  setStatus('Loading underlyings…');
  const meta = await api('/api/fo/meta', { date: state.day });
  if (!meta.underlyings.length) {
    fillSelect($('underlying'), []); fillSelect($('expiry'), []);
    state.foData = null; renderFo(null); setStatus('No F&O data for ' + state.day); return;
  }
  fillSelect($('underlying'), meta.underlyings.map((u) => [u, u]),
    (state.underlying = meta.underlyings.includes(state.underlying) ? state.underlying : meta.underlyings[0]));
  await refreshFo();
}

async function refreshFo() {
  const id = ++reqId;
  writeHash();
  setStatus('Loading ' + state.underlying + '…');
  try {
    const params = { date: state.day, underlying: state.underlying };
    if (state.expiry) params.expiry = state.expiry;
    const d = await api('/api/fo/chain', params);
    if (id !== reqId) return;
    state.foData = d; state.expiry = d.expiry;
    fillSelect($('expiry'), d.expiries.map((e) => [e, e]), d.expiry);
    renderFo(d);
    setStatus(`${d.underlying} · ${d.expiry || 'no option expiry'} · ${d.chain.length} strikes`);
  } catch (e) { if (id === reqId) setStatus(e.message, true); }
}

function foCards(d) {
  const items = [
    ['Spot (proxy)', fmt(d.spot)], ['ATM strike', fmtInt(d.atm_strike)],
    ['PCR (put/call OI)', d.pcr == null ? '–' : d.pcr.toFixed(2)],
    ['Total call OI', fmtInt(d.total_call_oi)], ['Total put OI', fmtInt(d.total_put_oi)],
  ];
  $('fo-cards').innerHTML = items.map(([k, v]) => `<div class="card"><b>${k}</b><span>${v}</span></div>`).join('');
}

function renderFoFutures(d) {
  const chg = (v) => v == null ? '–' : `<span class="${v >= 0 ? 'pos' : 'neg'}">${v >= 0 ? '+' : ''}${fmtInt(v)}</span>`;
  $('fo-futures').innerHTML = '<thead><tr><th>Expiry</th><th>LTP</th><th>OI</th><th>OI chg</th><th>Volume</th></tr></thead><tbody>' +
    d.futures.map((r) => `<tr data-s="${r.symbol}"><td>${r.expiry}</td><td>${fmt(r.close)}</td><td>${fmtInt(r.oi)}</td><td>${chg(r.oi_change)}</td><td>${fmtInt(r.volume)}</td></tr>`).join('') +
    '</tbody>';
  $('fo-futures').querySelectorAll('tbody tr').forEach((tr) => tr.onclick = () => jumpToSymbol(tr.dataset.s));
}

function renderFoChain(d) {
  $('fo-chain-title').textContent = d.underlying + (d.expiry ? ' · ' + d.expiry : ' · no option expiry collected');
  const cell = (sym, val, cls = '') => sym ? `<td class="${cls}" data-s="${sym}">${val}</td>` : '<td class="muted">–</td>';
  const chg = (v) => v == null ? '–' : (v >= 0 ? '+' : '') + fmtInt(v);
  const rows = d.chain.map((r) => `<tr${r.strike === d.atm_strike ? ' class="atm"' : ''}>` +
    cell(r.ce_symbol, fmtInt(r.ce_oi)) + cell(r.ce_symbol, chg(r.ce_oi_change), r.ce_oi_change >= 0 ? 'pos' : 'neg') +
    cell(r.ce_symbol, fmtInt(r.ce_volume)) + cell(r.ce_symbol, fmt(r.ce_close)) +
    `<td class="strike">${fmtInt(r.strike)}</td>` +
    cell(r.pe_symbol, fmt(r.pe_close)) + cell(r.pe_symbol, fmtInt(r.pe_volume)) +
    cell(r.pe_symbol, chg(r.pe_oi_change), r.pe_oi_change >= 0 ? 'pos' : 'neg') + cell(r.pe_symbol, fmtInt(r.pe_oi)) +
    '</tr>').join('');
  $('fo-chain').innerHTML = `<thead><tr><th colspan="4">Calls</th><th></th><th colspan="4">Puts</th></tr>
    <tr><th>OI</th><th>OI chg</th><th>Vol</th><th>LTP</th><th>Strike</th><th>LTP</th><th>Vol</th><th>OI chg</th><th>OI</th></tr></thead>
    <tbody>${rows}</tbody>`;
  $('fo-chain').querySelectorAll('td[data-s]').forEach((td) => td.onclick = () => jumpToSymbol(td.dataset.s));
}

function renderFoOiChart(d) {
  const strikes = d.chain.map((r) => String(r.strike));
  const fg = css('--fg'), grid = css('--grid'), teal = css('--teal'), orange = css('--orange');
  const tr = [
    { x: d.chain.map((r) => r.ce_oi == null ? 0 : -r.ce_oi), y: strikes, name: 'Call OI', type: 'bar', orientation: 'h',
      marker: { color: teal }, customdata: d.chain.map((r) => r.ce_oi || 0), hovertemplate: 'Strike %{y}<br>Call OI %{customdata:,}<extra></extra>' },
    { x: d.chain.map((r) => r.pe_oi == null ? 0 : r.pe_oi), y: strikes, name: 'Put OI', type: 'bar', orientation: 'h',
      marker: { color: orange }, hovertemplate: 'Strike %{y}<br>Put OI %{x:,}<extra></extra>' },
  ];
  const layout = {
    title: { text: 'Open interest by strike', font: { size: 13 } }, barmode: 'overlay', height: Math.max(400, strikes.length * 18),
    margin: { l: 70, r: 20, t: 36, b: 40 }, paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
    font: { color: fg, family: '-apple-system, BlinkMacSystemFont, Inter, Helvetica Neue, sans-serif' },
    hoverlabel: { bgcolor: '#1d1d1f', bordercolor: 'rgba(255,255,255,.2)', font: { color: fg } },
    legend: { orientation: 'h', y: 1.04 },
    xaxis: { title: { text: 'Open interest (puts →, ← calls)', font: { size: 11 } }, gridcolor: grid, zerolinecolor: grid, tickformat: ',' },
    yaxis: { type: 'category', categoryorder: 'array', categoryarray: strikes, gridcolor: grid, tickfont: { size: 10 } },
  };
  Plotly.react('chart-oi', tr, layout, { responsive: true, displaylogo: false });
}

function renderFo(d) {
  if (!d || (!d.futures.length && !d.chain.length)) { $('fo-empty').hidden = false; $('fo-body').hidden = true; return; }
  $('fo-empty').hidden = true; $('fo-body').hidden = false;
  foCards(d); renderFoFutures(d); renderFoChain(d); renderFoOiChart(d);
}

/* ---------- wiring ---------- */
$('date').onchange = (e) => {
  state.day = e.target.value;
  (state.tab === 'fo' ? loadFoUnderlyings() : loadSymbols()).catch((x) => setStatus(x.message, true));
};
$('symbol').onchange = (e) => { state.symbol = e.target.value; refresh(); };
$('width').onchange = (e) => { state.width = +e.target.value; refresh(); };
$('volwin').onchange = (e) => { state.volwin = +e.target.value; refresh(); };
$('underlying').onchange = (e) => { state.underlying = e.target.value; state.expiry = null; refreshFo().catch((x) => setStatus(x.message, true)); };
$('expiry').onchange = (e) => { state.expiry = e.target.value; refreshFo().catch((x) => setStatus(x.message, true)); };
$('tab-instrument').onclick = () => setTab('instrument');
$('tab-overview').onclick = () => setTab('overview');
$('tab-fo').onclick = () => setTab('fo');
init();
