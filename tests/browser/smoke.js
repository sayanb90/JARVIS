// Real-browser smoke test of the viewer.  Usage: node smoke.js <base-url>
// Runs against the synthetic demo lake with derivatives (3 days, 10 stocks, NIFTY and BANKNIFTY chains).
// Exits non-zero (and prints why) if the page is broken.  Needs the `playwright` npm package and a Chromium.
const { chromium } = require('playwright');
const fail = (m) => { console.error('FAIL: ' + m); process.exitCode = 1; };
const status = (p) => p.textContent('#status');
const plotted = (p, id) => p.evaluate((i) => { const d = document.getElementById(i); return !!(d && d.data && d.querySelector('.main-svg')); }, id);
(async () => {
  const base = process.argv[2];
  const browser = await chromium.launch();
  const errors = [];
  const watch = (p) => { p.on('pageerror', (e) => errors.push('pageerror ' + e.message)); p.on('console', (m) => { if (m.type() === 'error') errors.push(m.text()); }); };
  const open = async (vw, vh, hash = '') => {
    const p = await (await browser.newContext({ viewport: { width: vw, height: vh } })).newPage(); watch(p);
    await p.goto(base + '/' + hash);
    return p;
  };

  // ---------------- Equities (desktop) ----------------
  const page = await open(1400, 1000);
  await page.waitForSelector('#eq-price .main-svg', { timeout: 20000 });
  const days = await page.$$eval('#date option', (o) => o.map((x) => x.value));
  const syms = await page.$$eval('#symbol option', (o) => o.map((x) => x.value));
  if (days.length !== 3 || days[0] < days[1]) fail('dates not newest first: ' + days);
  if (syms.length !== 10) fail('expected 10 equities (derivatives listed separately), got ' + syms.length);
  if (await page.isHidden('#demo-banner')) fail('demo banner not shown');
  if ((await page.$$eval('#cards .kpi', (c) => c.length)) !== 6) fail('expected 6 key figures');
  // every chart has its own legend and Plotly's shared legend is off
  for (const id of ['eq-price', 'eq-volume']) {
    if (await page.evaluate((i) => document.getElementById(i).layout.showlegend, id)) fail(id + ': Plotly legend should be off');
    if ((await page.$$eval(`.legend[data-for="${id}"] button`, (b) => b.length)) < 2) fail(id + ': per-chart legend missing');
  }
  // raw bid/ask starts hidden; its legend entry shows it
  const bidVisible = () => page.evaluate(() => document.getElementById('eq-price').data[1].visible);
  if ((await bidVisible()) !== false) fail('bid/ask should start hidden at 1-second resolution');
  await page.click('.legend[data-for="eq-price"] button:nth-child(2)');
  if ((await bidVisible()) === false) fail('legend click did not show bid/ask');

  // sub-tabs: order book and volatility render their own charts; hover updates the book card
  await page.click('.subtabs[data-tab="instrument"] button[data-sub="book"]');
  await page.waitForSelector('#eq-depth .main-svg');
  for (const id of ['eq-depth', 'eq-imb', 'eq-spread']) if (!(await plotted(page, id))) fail('order book chart missing: ' + id);
  if (await page.isVisible('#eq-price')) fail('price chart should be hidden on the order book tab');
  const box = await (await page.$('#eq-depth .nsewdrag')).boundingBox();
  await page.mouse.move(box.x + box.width * 0.3, box.y + 40);
  await page.mouse.move(box.x + box.width * 0.31, box.y + 45);
  await page.waitForTimeout(400);
  const book = await page.textContent('#book-body');
  if (!/Bid/.test(book) || !/Ask/.test(book) || !/IST/.test(book)) fail('book card not updated: ' + book.slice(0, 80));
  await page.click('.subtabs[data-tab="instrument"] button[data-sub="vol"]');
  await page.waitForSelector('#eq-vol .main-svg');
  if (!/sub=vol/.test(page.url())) fail('sub-tab not in the URL: ' + page.url());

  // change instrument and width: charts re-render and the URL follows
  await page.selectOption('#symbol', 'INFY.NS');
  await page.waitForFunction(() => /INFY\.NS/.test(document.getElementById('status').textContent));
  await page.selectOption('#width', '60');
  await page.waitForFunction(() => /1 minute/.test(document.getElementById('status').textContent));
  const n = await page.evaluate(() => document.getElementById('eq-vol').data[0].x.length);
  if (n > 375 || n < 100) fail('1-minute bars out of range: ' + n);
  if (!/symbol=INFY.NS/.test(page.url()) || !/width=60/.test(page.url())) fail('hash not updated: ' + page.url());

  // ---------------- Market ----------------
  await page.click('#tab-overview');
  await page.waitForSelector('#chart-heat .main-svg');
  const tiles = await page.evaluate(() => document.getElementById('chart-heat').data[0].labels.length);
  if (tiles !== 10) fail('heat map tiles ' + tiles);
  await page.selectOption('#heat-n', '10');
  if ((await page.$$eval('#lead-vol li', (l) => l.length)) !== 5) fail('most volatile list should have 5 rows');
  const top = await page.textContent('#lead-vol li:first-child .sym');
  const vols = await page.evaluate(() => document.getElementById('chart-heat').data[0].customdata.map((c) => c[0]));
  if (vols.join() !== [...vols].sort((a, b) => b - a).join()) fail('heat map is not ranked by volatility');
  await page.click('.subtabs[data-tab="overview"] button[data-sub="table"]');
  await page.waitForSelector('#table tbody tr');
  if ((await page.$$eval('#table tbody tr', (r) => r.length)) !== 10) fail('table rows');
  await page.click('th[data-k="realised_vol_pct"]');
  const first = await page.textContent('#table tbody tr:first-child td:first-child');
  if (first.trim() !== top.trim()) fail(`table sorted by volatility starts with ${first}, list with ${top}`);
  await page.click('#table tbody tr:first-child');
  await page.waitForSelector('#view-instrument:not([hidden])');
  await page.waitForFunction((s) => document.getElementById('status').textContent.includes(s), top.trim());

  // ---------------- Derivatives ----------------
  const fo = await open(1400, 1000, '#tab=fo');
  await fo.waitForSelector('#fo-body:not([hidden])', { timeout: 20000 });
  const unds = await fo.$$eval('#underlying option', (o) => o.map((x) => x.value));
  if (unds.join() !== 'BANKNIFTY,NIFTY') fail('underlyings ' + unds);
  await fo.selectOption('#underlying', 'NIFTY');
  await fo.waitForFunction(() => /^NIFTY ·/.test(document.getElementById('status').textContent));
  if ((await fo.$$eval('#fo-cards .kpi', (c) => c.length)) !== 6) fail('derivatives key figures');
  if ((await fo.$$eval('#fo-futures tbody tr', (r) => r.length)) !== 2) fail('NIFTY should list 2 futures');
  await fo.click('.subtabs[data-tab="fo"] button[data-sub="chain"]');
  const chainRows = await fo.$$eval('#fo-chain tbody tr', (r) => r.length);
  if (chainRows !== 21) fail('ATM ± 10 should show 21 strikes, got ' + chainRows);
  if ((await fo.$$eval('#fo-chain tr.atm', (r) => r.length)) !== 1) fail('exactly one at-the-money row');
  await fo.selectOption('#chain-range', '0');
  if ((await fo.$$eval('#fo-chain tbody tr', (r) => r.length)) !== 25) fail('all strikes should show 25 rows');
  await fo.click('.subtabs[data-tab="fo"] button[data-sub="oi"]');
  await fo.waitForSelector('#chart-oi .main-svg');
  const bars = await fo.evaluate(() => document.getElementById('chart-oi').data[0].y.length);
  if (bars !== 15) fail('most active should chart 15 strikes, got ' + bars);
  if (await fo.evaluate(() => document.getElementById('chart-oi').layout.showlegend)) fail('OI chart: Plotly legend should be off');
  // clicking a chain cell opens that contract with price + open interest
  await fo.click('.subtabs[data-tab="fo"] button[data-sub="chain"]');
  await fo.click('#fo-chain tr.atm td.ce[data-s]:not(.oi)');
  await fo.waitForSelector('#fc-price .main-svg', { timeout: 20000 });
  await fo.waitForFunction(() => /_CE_/.test(document.getElementById('status').textContent));
  const oiTrace = await fo.evaluate(() => document.getElementById('fc-price').data.some((t) => t.name === 'Open interest'));
  if (!oiTrace) fail('contract chart has no open-interest series');
  if (!/sub=contract/.test(fo.url()) || !/contract=NIFTY_/.test(fo.url())) fail('contract not in the URL: ' + fo.url());
  // deep link straight to a contract
  const fo2 = await open(1400, 1000, '#tab=fo&sub=contract&underlying=BANKNIFTY&contract=BANKNIFTY_FUT_24_NOV_26');
  await fo2.waitForFunction(() => /BANKNIFTY_FUT_24_NOV_26/.test(document.getElementById('status').textContent), null, { timeout: 20000 });

  // ---------------- every section on a phone: no sideways scrolling ----------------
  const views = ['#tab=instrument&sub=price', '#tab=instrument&sub=book', '#tab=overview&sub=heat', '#tab=overview&sub=table',
                 '#tab=fo&sub=summary&underlying=NIFTY', '#tab=fo&sub=chain&underlying=NIFTY', '#tab=fo&sub=oi&underlying=NIFTY'];
  for (const h of views) {
    const ph = await open(390, 844, h);
    await ph.waitForFunction(() => !document.getElementById('context').classList.contains('busy'), null, { timeout: 20000 });
    await ph.waitForTimeout(300);
    if (await ph.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 1)) fail('horizontal scroll on a phone: ' + h);
    await ph.context().close();
  }

  // ---------------- bad input falls back instead of a blank page ----------------
  const p3 = await open(1200, 900, '#date=2026-10-23&symbol=NOSUCH.NS');
  await p3.waitForSelector('#eq-price .main-svg', { timeout: 20000 });

  if (errors.length) fail('browser errors: ' + JSON.stringify(errors.slice(0, 5)));
  await browser.close();
  if (!process.exitCode) console.log('OK');
})().catch((e) => { console.error('FAIL: ' + e.message); process.exit(1); });
