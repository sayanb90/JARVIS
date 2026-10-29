// Real-browser smoke test of the viewer.  Usage: node smoke.js <base-url>
// Exits non-zero (and prints why) if the page is broken.  Needs the `playwright` npm package and a Chromium.
const { chromium } = require('playwright');
const fail = (m) => { console.error('FAIL: ' + m); process.exitCode = 1; };
(async () => {
  const base = process.argv[2];
  const browser = await chromium.launch();
  const errors = [];
  const watch = (p) => { p.on('pageerror', (e) => errors.push('pageerror ' + e.message)); p.on('console', (m) => { if (m.type() === 'error') errors.push(m.text()); }); };

  // ---- desktop ----
  const page = await (await browser.newContext({ viewport: { width: 1400, height: 1000 } })).newPage(); watch(page);
  await page.goto(base);
  await page.waitForSelector('#chart .main-svg', { timeout: 20000 });
  const days = await page.$$eval('#date option', (o) => o.map((x) => x.value));
  const syms = await page.$$eval('#symbol option', (o) => o.map((x) => x.value));
  if (days.length !== 3 || days[0] < days[1]) fail('dates not newest first: ' + days);
  if (syms.length !== 10) fail('expected 10 instruments, got ' + syms.length);
  if (await page.isHidden('#demo-banner')) fail('demo banner not shown');
  const traces = await page.evaluate(() => document.getElementById('chart').data.length);
  if (traces < 10) fail('too few traces: ' + traces);
  const yaxes = await page.evaluate(() => Object.keys(document.getElementById('chart').layout).filter((k) => k.startsWith('yaxis')).length);
  if (yaxes < 7) fail('expected stacked panels, y axes = ' + yaxes);
  const cards = await page.$$eval('#cards .card', (c) => c.length);
  if (cards < 10) fail('summary cards missing: ' + cards);

  // hover updates the book card
  const box = await (await page.$('#chart .nsewdrag')).boundingBox();
  await page.mouse.move(box.x + box.width * 0.3, box.y + 40);
  await page.mouse.move(box.x + box.width * 0.31, box.y + 45);
  await page.waitForTimeout(400);
  const hoverTxt = await page.textContent('#book-body');
  if (!/Bid/.test(hoverTxt) || !/Ask/.test(hoverTxt) || !/IST/.test(hoverTxt)) fail('book card not updated: ' + hoverTxt.slice(0, 80));

  // change instrument and width: chart re-renders with new data and the URL hash follows
  await page.selectOption('#symbol', 'INFY.NS');
  await page.waitForFunction(() => /INFY\.NS/.test(document.getElementById('status').textContent));
  await page.selectOption('#width', '60');
  await page.waitForFunction(() => /1 minute/.test(document.getElementById('status').textContent));
  const n = await page.evaluate(() => document.getElementById('chart').data[2].x.length);
  if (n > 375 || n < 100) fail('1-minute bars out of range: ' + n);
  if (!/symbol=INFY.NS/.test(page.url()) || !/width=60/.test(page.url())) fail('hash not updated: ' + page.url());

  // overview tab: table + both charts, sorting, click-through
  await page.click('#tab-overview');
  await page.waitForSelector('#table tbody tr');
  const rows = await page.$$eval('#table tbody tr', (r) => r.length);
  if (rows !== 10) fail('overview rows ' + rows);
  await page.click('th[data-k="realised_vol_pct"]');
  const vols = await page.$$eval('#table tbody tr td:nth-child(9)', (c) => c.map((x) => parseFloat(x.textContent)));
  if (vols.join() !== [...vols].sort((a, b) => b - a).join()) fail('sort by volatility is not descending: ' + vols);
  if (!(await page.$('#chart-vol .main-svg')) || !(await page.$('#chart-scatter .main-svg'))) fail('overview charts missing');
  await page.click('#table tbody tr:first-child');
  await page.waitForSelector('#view-instrument:not([hidden])');

  // deep link restores the state
  const p2 = await (await browser.newContext({ viewport: { width: 1400, height: 1000 } })).newPage(); watch(p2);
  await p2.goto(base + '#date=2026-10-22&symbol=ITC.NS&tab=instrument&width=5');
  await p2.waitForFunction(() => /ITC\.NS · 2026-10-22/.test(document.getElementById('status').textContent), null, { timeout: 20000 });
  if ((await p2.inputValue('#width')) !== '5') fail('deep link did not restore the bar width');

  // ---- phone ----
  const ph = await (await browser.newContext({ viewport: { width: 390, height: 844 } })).newPage(); watch(ph);
  await ph.goto(base);
  await ph.waitForSelector('#chart .main-svg', { timeout: 20000 });
  if (await ph.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 1)) fail('horizontal scroll on a phone');

  // ---- bad input surfaces as a message, not a blank page ----
  const p3 = await (await browser.newContext()).newPage(); watch(p3);
  await p3.goto(base + '#date=2026-10-23&symbol=NOSUCH.NS');
  await p3.waitForSelector('#chart .main-svg', { timeout: 20000 });      // unknown symbol falls back to the first real one

  if (errors.length) fail('browser errors: ' + JSON.stringify(errors.slice(0, 5)));
  await browser.close();
  if (!process.exitCode) console.log('OK');
})().catch((e) => { console.error('FAIL: ' + e.message); process.exit(1); });
