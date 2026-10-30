/* JARVIS admin: data browser, read-only SQL console, subscription list.  Plain JS, no libraries. */
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const fmtInt = (x) => (x == null ? '–' : Math.round(x).toLocaleString('en-IN'));
const state = { tab: 'data', catalog: [], service: null, table: null, day: null, page: 1, size: 200, symbol: '', order: 'stored',
                qpage: 1, sql: '', subs: null, subSub: 'eq', eqFilter: '', eqPage: 1, plan: null };
let req = 0;

async function api(path, params, body) {
  const opt = body ? { method: 'POST', headers: { 'content-type': 'application/json', 'x-jarvis-admin': '1' }, body: JSON.stringify(body) } : {};
  const r = await fetch(path + (params ? '?' + new URLSearchParams(params) : ''), opt);
  if (!r.ok) {
    let msg = r.statusText;
    try { msg = (await r.json()).detail || msg; } catch (e) { /* not json */ }
    throw new Error(msg);
  }
  return r.json();
}
function setStatus(t, kind = 'ok') { $('status').textContent = t; $('context').className = 'context' + (kind === 'err' ? ' err' : kind === 'busy' ? ' busy' : ''); }
const fail = (e) => setStatus(e.message || String(e), 'err');
const fill = (el, items, sel) => { el.innerHTML = items.map(([v, l]) => `<option value="${esc(v)}">${esc(l)}</option>`).join(''); if (sel != null) el.value = sel; };

function readHash() { return Object.fromEntries(new URLSearchParams(location.hash.slice(1)).entries()); }
function writeHash() {
  const p = { tab: state.tab };
  if (state.tab === 'data') Object.assign(p, { service: state.service || '', table: state.table || '', day: state.day || '', page: state.page,
                                                size: state.size, symbol: state.symbol, order: state.order });
  if (state.tab === 'subs') p.sub = state.subSub;
  history.replaceState(null, '', '#' + new URLSearchParams(p).toString());
}

/* ---------------------------------------------------------------- shared grid + pager */
function renderGrid(tableEl, columns, rows, firstRow = 1) {
  if (!columns.length) { tableEl.innerHTML = '<tbody><tr><td class="null">No rows.</td></tr></tbody>'; return; }
  const numeric = columns.map((_, j) => rows.length && rows.every((r) => r[j] == null || typeof r[j] === 'number'));
  tableEl.innerHTML = `<thead><tr><th class="rn">#</th>${columns.map((c) => `<th>${esc(c)}</th>`).join('')}</tr></thead><tbody>` +
    (rows.length ? rows.map((r, i) => `<tr><td class="rn">${fmtInt(firstRow + i)}</td>` + r.map((v, j) => v == null
      ? '<td class="null">null</td>'
      : `<td class="${numeric[j] ? 'n' : ''}">${esc(typeof v === 'number' && !Number.isInteger(v) ? +v.toPrecision(12) : v)}</td>`).join('') + '</tr>').join('')
      : `<tr><td class="null" colspan="${columns.length + 1}">No rows.</td></tr>`) + '</tbody>';
}

/* numbered pager: « ‹ 1 … 4 5 [6] 7 8 … N › » ; onGo(page) fetches only that page */
function renderPager(el, page, pages, info, onGo) {
  if (!pages) { el.innerHTML = `<span class="info">${info || ''}</span>`; return; }
  const nums = new Set([1, pages, page - 2, page - 1, page, page + 1, page + 2].filter((p) => p >= 1 && p <= pages));
  const list = [...nums].sort((a, b) => a - b);
  let html = `<span class="info">${info}</span><button data-p="1" ${page === 1 ? 'disabled' : ''} aria-label="First page">«</button><button data-p="${page - 1}" ${page === 1 ? 'disabled' : ''} aria-label="Previous page">‹</button>`;
  list.forEach((p, i) => {
    if (i && p - list[i - 1] > 1) html += '<span class="gap">…</span>';
    html += `<button class="num" data-p="${p}" ${p === page ? 'aria-current="page"' : ''}>${fmtInt(p)}</button>`;
  });
  html += `<button data-p="${page + 1}" ${page === pages ? 'disabled' : ''} aria-label="Next page">›</button><button data-p="${pages}" ${page === pages ? 'disabled' : ''} aria-label="Last page">»</button>`;
  el.innerHTML = html;
  el.querySelectorAll('button[data-p]').forEach((b) => b.onclick = () => onGo(+b.dataset.p));
}

/* ---------------------------------------------------------------- sections */
function setTab(tab) {
  state.tab = tab;
  for (const t of ['data', 'query', 'subs']) { $('tab-' + t).setAttribute('aria-selected', t === tab); $('view-' + t).hidden = t !== tab; }
  writeHash();
  if (tab === 'query' && !$('schema').dataset.loaded) loadSchema().catch(fail);
  if (tab === 'subs') loadSubs().catch(fail);
  if (tab === 'data') setStatus(state.table ? `${state.service}.${state.table}` : 'Pick a table');
}
for (const t of ['data', 'query', 'subs']) $('tab-' + t).onclick = () => setTab(t);

/* ================================================================ Data */
const svc = () => state.catalog.find((s) => s.service === state.service);
const tbl = () => (svc() ? svc().tables.find((t) => t.name === state.table) : null);

function renderServiceCards() {
  const s = svc();
  $('svc-cards').innerHTML = s.tables.length ? s.tables.map((t) => `<button class="svc-card${t.available ? '' : ' off'}" data-t="${t.name}" aria-pressed="${t.name === state.table}">
      <b>${esc(t.title)}</b><small>${t.available ? (t.days.length ? `${t.days.length} day${t.days.length > 1 ? 's' : ''} · latest ${t.days[0]}` : esc(t.query_name || 'derived per day')) : 'no data yet'}</small></button>`).join('')
    : '<div class="empty" style="grid-column:1/-1"><p>No tables in this service yet. Signals and features will appear here once they are produced.</p></div>';
  $('svc-cards').querySelectorAll('.svc-card').forEach((b) => b.onclick = () => selectTable(b.dataset.t));
}

function selectService(name, keepTable) {
  state.service = name;
  $('service').value = name;
  const s = svc();
  fill($('table-name'), s.tables.map((t) => [t.name, t.title + (t.available ? '' : ' (no data)')]));
  const pick = keepTable && s.tables.find((t) => t.name === keepTable) ? keepTable : (s.tables.find((t) => t.available) || s.tables[0] || {}).name;
  renderServiceCards();
  if (pick) selectTable(pick, keepTable ? state.day : null, keepTable ? state.page : 1);
  else { state.table = null; renderGrid($('grid'), [], []); $('pager').innerHTML = ''; $('tbl-title').textContent = s.title; $('tbl-meta').textContent = ''; writeHash(); }
}

function selectTable(name, day = null, page = 1) {
  state.table = name; state.page = page;
  const t = tbl();
  $('table-name').value = name;
  $('svc-cards').querySelectorAll('.svc-card').forEach((b) => b.setAttribute('aria-pressed', b.dataset.t === name));
  $('day-field').hidden = !t.days.length;
  fill($('day'), t.days.map((d) => [d, d]), (state.day = t.days.includes(day) ? day : (t.days[0] || null)));
  loadPage();
}

async function loadPage() {
  const t = tbl();
  if (!t) return;
  const id = ++req;
  writeHash();
  $('tbl-title').innerHTML = `${esc(t.title)}<span class="hint">${esc(t.query_name || state.service + ' · derived from ticks')}</span>`;
  setStatus(`Loading ${state.service}.${state.table}…`, 'busy');
  const params = { service: state.service, name: state.table, page: state.page, size: state.size, order: state.order };
  if (state.day) params.day = state.day;
  if (state.symbol) params.symbol = state.symbol;
  try {
    const d = await api('/admin/api/table', params);
    if (id !== req) return;
    state.page = d.page;
    const first = (d.page - 1) * state.size + 1;
    renderGrid($('grid'), d.columns, d.rows, first);
    $('tbl-meta').textContent = t.about;
    const info = d.total ? `Rows ${fmtInt(first)}–${fmtInt(first + d.rows.length - 1)} of ${fmtInt(d.total)} · ${d.elapsed_ms} ms` : 'No rows';
    renderPager($('pager'), d.page, d.total ? d.pages : 0, info, (p) => { state.page = p; loadPage(); $('grid').parentElement.scrollTop = 0; });
    writeHash();
    setStatus(`${state.service}.${state.table}${state.day ? ' · ' + state.day : ''}${state.symbol ? ' · ' + state.symbol : ''} · page ${fmtInt(d.page)} of ${fmtInt(d.pages)}`);
  } catch (e) { if (id === req) { fail(e); renderGrid($('grid'), [], []); $('pager').innerHTML = `<div class="err-box">${esc(e.message)}</div>`; } }
}

$('service').onchange = (e) => selectService(e.target.value);
$('table-name').onchange = (e) => selectTable(e.target.value);
$('day').onchange = (e) => { state.day = e.target.value; state.page = 1; loadPage(); };
$('order').onchange = (e) => { state.order = e.target.value; state.page = 1; loadPage(); };
$('size').onchange = (e) => { state.size = +e.target.value; state.page = 1; loadPage(); };
let symTimer;
$('symbol').oninput = (e) => { clearTimeout(symTimer); symTimer = setTimeout(() => { state.symbol = e.target.value.trim().toUpperCase(); state.page = 1; loadPage(); }, 450); };

/* ================================================================ Query */
const EXAMPLES = [
  ['Ticks per symbol on the latest day', "SELECT symbol, count(*) AS ticks, min(ts) AS first, max(ts) AS last\nFROM market.ticks\nWHERE date = (SELECT max(date) FROM market.ticks)\nGROUP BY symbol\nORDER BY ticks DESC"],
  ['Last 50 ticks of one symbol', "SELECT ts, ltp, vol_today, bid, bid_qty, ask, ask_qty, oi\nFROM market.ticks\nWHERE date = (SELECT max(date) FROM market.ticks) AND symbol = 'RELIANCE.NS'\nORDER BY ts DESC\nLIMIT 50"],
  ['Widest average spreads today (bps)', "SELECT symbol, avg((ask - bid) / ((ask + bid) / 2) * 1e4) AS spread_bps, count(*) AS ticks\nFROM market.ticks\nWHERE date = (SELECT max(date) FROM market.ticks) AND bid > 0 AND ask >= bid\nGROUP BY symbol\nORDER BY spread_bps DESC"],
  ['Option chain contracts for one expiry', "SELECT symbol, instrument_type, strike, lot_size\nFROM reference.fo_contracts\nWHERE underlying = 'NIFTY' AND expiry = (SELECT min(expiry) FROM reference.fo_contracts WHERE underlying = 'NIFTY' AND expiry >= current_date)\nORDER BY strike, instrument_type"],
  ['Daily closes, last 20 days', "SELECT date, symbol, close, volume\nFROM aggregates.ohlc_daily\nWHERE symbol = 'RELIANCE.NS'\nORDER BY date DESC\nLIMIT 20"],
];
async function loadSchema() {
  const views = await api('/admin/api/schema');
  $('schema').dataset.loaded = '1';
  $('schema').classList.add('schema');
  $('schema').innerHTML = Object.keys(views).length ? Object.entries(views).map(([v, cols]) => `<details><summary>${esc(v)}</summary><div class="cols">${cols.map(esc).join(', ')}</div></details>`).join('')
    : '<p class="muted">No tables yet.</p>';
  $('schema').querySelectorAll('summary').forEach((s) => s.ondblclick = () => insertAtCursor(s.textContent));
  $('examples').innerHTML = EXAMPLES.map(([t], i) => `<button class="example" data-i="${i}">${esc(t)}</button>`).join('');
  $('examples').querySelectorAll('.example').forEach((b) => b.onclick = () => { $('sql').value = EXAMPLES[+b.dataset.i][1]; runQuery(1); });
  if (!state.sql) setStatus('Ready: one SELECT at a time');       // never overwrite a query that already ran
}
function insertAtCursor(text) {
  const ta = $('sql'), s = ta.selectionStart, e = ta.selectionEnd;
  ta.value = ta.value.slice(0, s) + text + ta.value.slice(e); ta.focus(); ta.selectionStart = ta.selectionEnd = s + text.length;
}
async function runQuery(page = 1) {
  state.sql = $('sql').value || ' '; state.qpage = page;
  const id = ++req;
  $('run').disabled = true;
  setStatus('Running…', 'busy');
  try {
    const d = await api('/admin/api/query', null, { sql: state.sql, page, size: 200 });
    if (id !== req) return;
    const first = (page - 1) * d.size + 1;
    renderGrid($('qgrid'), d.columns, d.rows, first);
    $('qmeta').textContent = `${d.elapsed_ms} ms`;
    const info = d.rows.length ? `Rows ${fmtInt(first)}–${fmtInt(first + d.rows.length - 1)}${d.has_more ? '' : ' (end)'}` : 'No rows';
    $('qpager').innerHTML = `<span class="info">${info}</span><button id="qprev" ${page === 1 ? 'disabled' : ''}>‹ Previous</button><span class="gap">Page ${fmtInt(page)}</span><button id="qnext" ${d.has_more ? '' : 'disabled'}>Next ›</button>`;
    $('qprev').onclick = () => runQuery(page - 1); $('qnext').onclick = () => runQuery(page + 1);
    setStatus(`Query ok · page ${page} · ${d.elapsed_ms} ms`);
  } catch (e) {
    if (id === req) { fail(e); $('qgrid').innerHTML = ''; $('qpager').innerHTML = `<div class="err-box">${esc(e.message)}</div>`; }
  } finally { $('run').disabled = false; }
}
$('run').onclick = () => runQuery(1);
$('sql').onkeydown = (e) => { if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); runQuery(1); } };

/* ================================================================ Subscriptions */
function showSubSub(sub) {
  state.subSub = sub;
  document.querySelectorAll('#view-subs .subtabs button').forEach((b) => b.setAttribute('aria-selected', b.dataset.sub === sub));
  document.querySelectorAll('#view-subs .subview').forEach((v) => { v.hidden = v.dataset.sub !== sub; });
  writeHash();
}
document.querySelectorAll('#view-subs .subtabs button').forEach((b) => b.onclick = () => showSubSub(b.dataset.sub));

async function loadSubs() {
  setStatus('Loading the subscription list…', 'busy');
  const d = await api('/admin/api/universe');
  state.subs = d;
  const eq = d.equities, keyed = eq.filter((r) => r.instrument_key).length, ticking = eq.filter((r) => r.ticks > 0).length;
  const foN = d.fo.reduce((a, r) => a + r.futures + r.calls + r.puts, 0);
  $('subs-sub').textContent = `India list from ${d.source}${d.tick_day ? ' · ticks on ' + d.tick_day : ' · no ticks collected yet'}`;
  $('subs-kpis').style.setProperty('--n', 4); $('subs-kpis').style.setProperty('--nt', 2);
  $('subs-kpis').innerHTML = [
    ['Equities subscribed', fmtInt(eq.length), `${fmtInt(keyed)} with an Upstox key`],
    ['Ticking', fmtInt(ticking), d.tick_day ? `of ${fmtInt(eq.length)} on ${d.tick_day}` : 'no ticks yet'],
    ['F&O contracts', fmtInt(foN), `${d.fo.length} underlying${d.fo.length === 1 ? '' : 's'}`],
    ['Instrument file', d.eq_table ? fmtInt(d.eq_listings) : 'not imported', d.eq_table ? 'NSE equities known' : 'run fo_instruments import'],
  ].map(([k, v, s]) => `<div class="kpi"><b>${k}</b><span>${v}</span><small>${esc(s)}</small></div>`).join('');
  renderEq();
  const cols = ['Underlying', 'Futures', 'Calls', 'Puts', 'Expiries', 'Contracts ticking', 'Ticks'];
  renderGrid($('fo-grid'), d.fo.length ? cols : [], d.fo.map((r) => [r.underlying, r.futures, r.calls, r.puts, r.expiries, r.ticking, r.ticks]));
  const wl = d.watchlist;
  renderGrid($('wl-grid'), wl.length ? Object.keys(wl[0]) : [], wl.map((r) => Object.values(r)));
  renderHistory();
  showSubSub(state.subSub);
  setStatus(`${fmtInt(eq.length)} equities · ${fmtInt(foN)} F&O contracts`);
}

function renderEq() {
  const f = state.eqFilter;
  const rows = state.subs.equities.filter((r) => !f || r.symbol.includes(f) || (r.name || '').toUpperCase().includes(f));
  const size = 200, pages = Math.max(1, Math.ceil(rows.length / size));
  state.eqPage = Math.min(state.eqPage, pages);
  const slice = rows.slice((state.eqPage - 1) * size, state.eqPage * size);
  $('eq-hint').textContent = `${fmtInt(rows.length)} shown`;
  renderGrid($('eq-grid'), ['symbol', 'name', 'isin', 'upstox_key', 'listed', 'ticks', 'first_tick', 'last_tick', 'last_price'],
    slice.map((r) => [r.symbol, r.name, r.isin, r.instrument_key, r.listed == null ? null : r.listed ? 'yes' : 'NOT IN FILE', r.ticks, r.first_tick, r.last_tick, r.last_price]),
    (state.eqPage - 1) * size + 1);
  renderPager($('eq-pager'), state.eqPage, rows.length ? pages : 0, `${fmtInt(rows.length)} instruments`, (p) => { state.eqPage = p; renderEq(); });
}
$('eq-filter').oninput = (e) => { state.eqFilter = e.target.value.trim().toUpperCase(); state.eqPage = 1; renderEq(); };

function renderHistory() {
  const h = state.subs.history;
  $('hist-grid').innerHTML = h.length ? `<thead><tr><th>Version</th><th>India instruments</th><th></th></tr></thead><tbody>` +
    h.map((v) => `<tr><td>${esc(v.name)}</td><td class="n">${fmtInt(v.india)}</td><td><button class="btn" data-v="${esc(v.name)}">Restore</button></td></tr>`).join('') + '</tbody>'
    : '<tbody><tr><td class="null">No saved versions yet: one is kept every time a list is applied.</td></tr></tbody>';
  $('hist-grid').querySelectorAll('button[data-v]').forEach((b) => b.onclick = async () => {
    if (!confirm(`Make ${b.dataset.v} the active list? The current list is saved first.`)) return;
    try { await api('/admin/api/universe/restore', null, { name: b.dataset.v }); await loadSubs(); setStatus('Restored ' + b.dataset.v); } catch (e) { fail(e); }
  });
}

async function tickerText() {
  const f = $('csv').files[0];
  if (f) {
    if (f.size > 1_000_000) throw new Error('file larger than 1 MB');
    return await f.text();
  }
  return $('paste').value;
}
const tags = (list, cls) => list.length ? `<div class="tags">${list.slice(0, 120).map((s) => `<span class="tag ${cls}">${esc(s.replace(/\.NS$/, ''))}</span>`).join('')}${list.length > 120 ? `<span class="muted">+${list.length - 120} more</span>` : ''}</div>` : '<span class="muted">none</span>';
$('preview').onclick = async () => {
  try {
    const text = await tickerText();
    if (!text.trim()) { fail(new Error('choose a CSV file or paste tickers first')); return; }
    setStatus('Matching tickers…', 'busy');
    const p = await api('/admin/api/universe/preview', null, { tickers: text, mode: $('mode').value });
    state.plan = { text, mode: $('mode').value, india: p.india.length, added: p.added.length, removed: p.removed.length };
    const unk = p.unknown.map((u) => u.ticker + (u.suggestions.length ? ` → ${u.suggestions.join(' / ')}?` : ''));
    $('plan').innerHTML = `<div class="plan">
      <div class="big">${fmtInt(p.rows.length)} <span class="muted" style="font-size:13px;font-weight:400">of ${fmtInt(p.requested)} tickers matched${p.eq_table ? '' : ' (instrument file not imported: only tickers already in the list can match)'}</span></div>
      <p class="muted">${p.mode === 'replace' ? 'The India list becomes' : 'The India list grows to'} <b>${fmtInt(p.india.length)}</b> instruments.</p>
      <h4>Added (${p.added.length})</h4>${tags(p.added, 'add')}
      <h4>Removed (${p.removed.length})</h4>${tags(p.removed, 'rm')}
      <h4>Not matched (${unk.length + p.invalid.length}), skipped</h4>${tags(unk.concat(p.invalid), 'unk')}</div>`;
    $('apply').disabled = !p.rows.length;
    $('up-meta').textContent = p.rows.length ? 'Review, then Apply.' : '';
    setStatus(`Preview: ${p.rows.length} matched, ${p.added.length} added, ${p.removed.length} removed`);
  } catch (e) { fail(e); }
};
$('apply').onclick = async () => {
  if (!state.plan) return;
  const pl = state.plan;
  if (!confirm(`Set the India list to ${pl.india} instruments (+${pl.added} added, −${pl.removed} removed)? The current list is saved under History first.`)) return;
  try {
    const r = await api('/admin/api/universe/apply', null, { tickers: state.plan.text, mode: state.plan.mode });
    $('apply').disabled = true; state.plan = null;
    $('up-meta').textContent = `Saved: ${r.india} India instruments (+${r.added.length} / −${r.removed.length}). Takes effect at the next session start.`;
    await loadSubs(); showSubSub('upload');
    setStatus('Subscription list updated');
  } catch (e) { fail(e); }
};
['csv', 'paste', 'mode'].forEach((id) => { $(id).addEventListener('change', () => { $('apply').disabled = true; state.plan = null; }); });
$('paste').addEventListener('input', () => { $('apply').disabled = true; state.plan = null; });

/* ---------------------------------------------------------------- start-up */
async function init() {
  const h = readHash();
  try { state.catalog = await api('/admin/api/catalog'); } catch (e) { fail(e); return; }
  fill($('service'), state.catalog.map((s) => [s.service, s.title]));
  if (['50', '100', '200', '500'].includes(h.size)) { state.size = +h.size; $('size').value = h.size; }
  if (['stored', 'asc', 'desc'].includes(h.order)) { state.order = h.order; $('order').value = h.order; }
  if (h.symbol) { state.symbol = h.symbol.toUpperCase(); $('symbol').value = state.symbol; }
  if (h.sub) state.subSub = h.sub;
  const service = state.catalog.find((s) => s.service === h.service) ? h.service : 'market';
  state.day = h.day || null; state.page = +h.page || 1;
  selectService(service, h.table || null);
  setTab(['data', 'query', 'subs'].includes(h.tab) ? h.tab : 'data');
}
init();
