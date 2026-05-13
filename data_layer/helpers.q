// =============================================================================
// helpers.q  —  JARVIS KDB+ data extraction helper functions
//
// Prerequisite: schema.q must be loaded and db/ must be open.
//   q)  \l data_layer/schema.q
//   q)  \l db
//   q)  \l data_layer/helpers.q
//
// Or load all at once:
//   q data_layer/helpers.q    (loads schema + db automatically)
//
// Function index:
//   Universe queries
//     getSymbols[mkt]               all tickers for a market (` = both)
//     getMarkets[]                  distinct markets in pillarMetrics
//
//   Price (OHLC) queries
//     getLatestPrice[sym]           most recent close price
//     getLatestPrices[mkt]          close prices for all tickers in a market
//     getOhlc[sym; startDt; endDt]  OHLC rows for a symbol over a date range
//     getOhlcMarket[mkt; startDt; endDt]  OHLC for all symbols in a market
//
//   Fundamental queries
//     getIncome[sym]                all annual income rows for a symbol
//     getBalance[sym]               all annual balance sheet rows
//     getCashFlow[sym]              all annual cash flow rows
//     getRatios[sym]                all annual ratio rows
//     getFundamentals[sym]          dict of all four fundamental tables
//     getLatestFundamentals[sym]    most recent year only (dict)
//
//   Pillar metrics
//     getPillarMetrics[mkt]         pillar metrics for market (` = both)
//     getPillarRow[sym]             single pillar metrics row for a ticker
//
//   Scoring / screening
//     scoreAll[mkt]                 score every ticker in a market vs latest price
//     topByScore[mkt; n]            top N tickers by pillar score
//     compareMarkets[minScore]      side-by-side US vs IN scores above threshold
//
//   Cross-market utilities
//     priceMapForMarket[mkt]        sym→price dict needed by screenUniverse
//     sectorSummary[mkt]            average pillar score (no sector data yet)
// =============================================================================

// Auto-load schema and db if not already loaded
if[not `pillarMetrics in tables[];
  \l data_layer/schema.q
 ];
if[not `ohlc in tables[];
  \l db
 ];

// ---------------------------------------------------------------------------
// Universe queries
// ---------------------------------------------------------------------------

// All tickers for a given market; ` returns both
// Example: getSymbols[`IN]  →  `RELIANCE.NS`TCS.NS ...
getSymbols:{[mkt]
  $[mkt~`;
    exec symbol from pillarMetrics;
    exec symbol from pillarMetrics where market=mkt
  ]
 }

// Distinct markets present in pillarMetrics
// Example: getMarkets[]  →  `IN`US
getMarkets:{[]
  exec distinct market from pillarMetrics
 }

// ---------------------------------------------------------------------------
// Price (OHLC) queries
// ---------------------------------------------------------------------------

// Latest close price for a single symbol
// Example: getLatestPrice[`AAPL]  →  182.5
getLatestPrice:{[sym]
  t: select close from ohlc where symbol=sym, date=max date;
  $[0=count t; 0n; first t`close]
 }

// Latest close prices for all symbols in a market (sym→price dict)
// Example: getLatestPrices[`US]
getLatestPrices:{[mkt]
  syms: getSymbols[mkt];
  syms ! getLatestPrice each syms
 }

// OHLC rows for a symbol between two dates (inclusive)
// Example: getOhlc[`INFY.NS; 2024.01.01; 2024.12.31]
getOhlc:{[sym; startDt; endDt]
  select date, symbol, market, open, high, low, close, volume
  from ohlc
  where symbol=sym, date within (startDt; endDt)
 }

// OHLC for all symbols in a market over a date range
// Example: getOhlcMarket[`US; 2024.01.01; 2024.12.31]
getOhlcMarket:{[mkt; startDt; endDt]
  select date, symbol, market, open, high, low, close, volume
  from ohlc
  where market=mkt, date within (startDt; endDt)
 }

// Latest N trading days of OHLC for a symbol
// Example: getRecentOhlc[`MSFT; 30]
getRecentOhlc:{[sym; nDays]
  t: select date, open, high, low, close, volume from ohlc where symbol=sym;
  neg[nDays] sublist `date xdesc t
 }

// ---------------------------------------------------------------------------
// Fundamental queries
// ---------------------------------------------------------------------------

// All annual income statement rows for a ticker, newest first
// Example: getIncome[`RELIANCE.NS]
getIncome:{[sym]
  `date xdesc select date, market, revenue, netIncome, ebit
  from incomeStatement where symbol=sym
 }

// All annual balance sheet rows, newest first
getBalance:{[sym]
  `date xdesc select date, market, totalAssets, totalLiabilities,
    longTermDebt, totalEquity
  from balanceSheet where symbol=sym
 }

// All annual cash flow rows, newest first
getCashFlow:{[sym]
  `date xdesc select date, market, freeCashFlow
  from cashFlow where symbol=sym
 }

// All annual ratio rows, newest first
getRatios:{[sym]
  `date xdesc select date, market, roic, sharesOutstanding
  from ratios where symbol=sym
 }

// All four fundamental tables for a symbol as a dict of tables
// Example: d: getFundamentals[`TCS.NS];  d`income
getFundamentals:{[sym]
  `income`balance`cashflow`ratios !
  (getIncome[sym]; getBalance[sym]; getCashFlow[sym]; getRatios[sym])
 }

// Most recent year's fundamentals as a flat dict
// Example: getLatestFundamentals[`AAPL]
getLatestFundamentals:{[sym]
  inc: first getIncome[sym];
  bal: first getBalance[sym];
  cf:  first getCashFlow[sym];
  rat: first getRatios[sym];
  `symbol`market`date`revenue`netIncome`ebit`totalAssets`totalLiabilities`longTermDebt`totalEquity`freeCashFlow`roic`sharesOutstanding !
  (sym; inc`market; inc`date;
   inc`revenue; inc`netIncome; inc`ebit;
   bal`totalAssets; bal`totalLiabilities; bal`longTermDebt; bal`totalEquity;
   cf`freeCashFlow; rat`roic; rat`sharesOutstanding)
 }

// ---------------------------------------------------------------------------
// Pillar metrics
// ---------------------------------------------------------------------------

// Pillar metrics table for a market (` = both)
// Example: getPillarMetrics[`US]
getPillarMetrics:{[mkt]
  $[mkt~`;
    select from pillarMetrics;
    select from pillarMetrics where market=mkt
  ]
 }

// Single pillar metrics row for one ticker (as a dict)
// Example: getPillarRow[`NVDA]
getPillarRow:{[sym]
  first select from pillarMetrics where symbol=sym
 }

// ---------------------------------------------------------------------------
// Scoring / screening
// ---------------------------------------------------------------------------

// Score all tickers in a market against their latest price
// Returns table: symbol, market, price, score
// Example: scoreAll[`IN]
scoreAll:{[mkt]
  syms: getSymbols[mkt];
  prices: getLatestPrice each syms;
  scores: scoreTicker'[syms; prices];
  `score xdesc ([] symbol:syms; market:mkt; price:prices; score:scores)
 }

// Top N tickers by pillar score in a market
// Example: topByScore[`US; 10]
topByScore:{[mkt; n]
  n sublist scoreAll[mkt]
 }

// Side-by-side comparison: US vs IN tickers at or above minScore
// Returns table: symbol, market, price, score — sorted by score desc
// Example: compareMarkets[5]
compareMarkets:{[minScore]
  usScores: select from scoreAll[`US] where score >= minScore;
  inScores: select from scoreAll[`IN] where score >= minScore;
  `score xdesc usScores, inScores
 }

// ---------------------------------------------------------------------------
// Cross-market utilities
// ---------------------------------------------------------------------------

// Build a sym→price dict for use with screenUniverse[]
// Example: pm: priceMapForMarket[`];  screenUniverse[pm; 6; `US]
priceMapForMarket:{[mkt]
  syms: getSymbols[mkt];
  syms ! getLatestPrice each syms
 }

// Summary statistics per market: ticker count, mean/median score
// Example: marketSummary[]
marketSummary:{[]
  mkts: getMarkets[];
  t: {[mkt]
    sc: scoreAll[mkt];
    scores: sc`score;
    ([] market:enlist mkt;
        count_tickers: enlist count scores;
        avg_score: enlist avg scores;
        med_score: enlist med scores;
        pct_score6plus: enlist (sum scores>=6) % count scores)
  } each mkts;
  {x,y}/[t]
 }

// Revenue growth (%) year-over-year for a symbol
// Example: revenueGrowth[`MSFT]
revenueGrowth:{[sym]
  t: getIncome[sym];
  if[1>=count t; :([] date:(); growthPct:())];
  dates: t`date;
  revs:  t`revenue;
  // pairwise growth: (rev[n-1] - rev[n]) / rev[n]   (newest first ordering)
  gPct: 100 * (neg 1 _ revs - 1 _ revs) % abs 1 _ revs;
  ([] date: neg[1] _ dates; growthPct: gPct)
 }

// Net debt for a symbol (latest balance sheet)
// Example: netDebt[`JPM]
netDebt:{[sym]
  b: first getBalance[sym];
  b[`longTermDebt] - (b[`totalAssets] - b[`totalLiabilities] - b[`longTermDebt])
 }

// ---------------------------------------------------------------------------
show "helpers.q loaded — use ?[helpers] to list all functions"
// ---------------------------------------------------------------------------
