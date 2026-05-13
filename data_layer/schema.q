// =============================================================================
// schema.q  —  JARVIS KDB+ table definitions
//
// Supports the 8 Pillar fundamental model + OHLC price data for both:
//   market=`IN  → Nifty 50   (symbols: RELIANCE.NS, INFY.NS, ...)
//   market=`US  → S&P 500    (symbols: AAPL, MSFT, ...)
//
// The `market` column on every table allows both universes to coexist in the
// same splayed / partitioned on-disk tables.
// Designed for kdb+ 32-bit (4 GB RAM limit).
//
// Load order: schema.q must be loaded before kdb_loader.q
//   q schema.q
// =============================================================================

// ---------------------------------------------------------------------------
// 1. FUNDAMENTALS  — annual per-ticker rows (splayed, keyed on date+symbol)
//
// Columns: date, symbol, market, <metric floats>
//   symbol : ticker as-returned by FMP (e.g. `RELIANCE.NS or `AAPL)
//   market : `IN (NSE/Nifty 50) or `US (NYSE/NASDAQ S&P 500)
// ---------------------------------------------------------------------------

// Income Statement  (Pillars 1, 2, 6)
incomeStatement:(
  [date:`date$(); symbol:`symbol$()]
  market:`symbol$();
  revenue:`float$();
  netIncome:`float$();
  ebit:`float$()
)

// Balance Sheet  (Pillars 4, 5, 6)
balanceSheet:(
  [date:`date$(); symbol:`symbol$()]
  market:`symbol$();
  totalAssets:`float$();
  totalLiabilities:`float$();
  longTermDebt:`float$();
  totalEquity:`float$()
)

// Cash Flow Statement  (Pillar 8)
cashFlow:(
  [date:`date$(); symbol:`symbol$()]
  market:`symbol$();
  freeCashFlow:`float$()
)

// Financial Ratios  (Pillars 3, 7)
ratios:(
  [date:`date$(); symbol:`symbol$()]
  market:`symbol$();
  roic:`float$();
  sharesOutstanding:`float$()
)

// ---------------------------------------------------------------------------
// 2. OHLC  — daily price data (partitioned by year via kdb_loader.q)
//    Volume as long (64-bit) to handle large NSE and NYSE volumes.
// ---------------------------------------------------------------------------
ohlc:(
  [date:`date$(); symbol:`symbol$()]
  market:`symbol$();
  open:`float$();
  high:`float$();
  low:`float$();
  close:`float$();
  volume:`long$()
)

// ---------------------------------------------------------------------------
// 3. PILLAR METRICS  — pre-aggregated, one row per ticker
//    Python layer writes this; kdb+ reads it for screening queries.
// ---------------------------------------------------------------------------
pillarMetrics:(
  [symbol:`symbol$()]
  market:`symbol$();
  avgNetIncome5y:`float$();
  avgFcf5y:`float$();
  latestRevenue:`float$();
  latestEbit:`float$();
  latestTotalAssets:`float$();
  latestTotalLiabilities:`float$();
  latestLongTermDebt:`float$();
  latestTotalEquity:`float$();
  latestRoic:`float$();
  latestSharesOutstanding:`float$()
)

// ---------------------------------------------------------------------------
// 4. Fund metadata  — tracks when each ticker's fundamentals were last fetched
//    Written by Python (data/fund_meta.csv) and loaded here for DB querying.
//    Used by the scheduler to prioritise the stalest tickers each day.
// ---------------------------------------------------------------------------
fundMeta:(
  [symbol:`symbol$()]
  market:`symbol$();
  lastUpdated:`date$()
)

// ---------------------------------------------------------------------------
// 5. Derived views
// ---------------------------------------------------------------------------

netDebtView:{
  select date, symbol, market,
    netDebt: longTermDebt - (totalAssets - totalLiabilities - longTermDebt)
  from balanceSheet
}

equityRatioView:{
  select date, symbol, market,
    equityRatio: totalEquity % totalAssets
  from balanceSheet
}

// ---------------------------------------------------------------------------
// 5. Pillar scoring function
//    scoreTicker[sym; lastPrice]  →  integer 0–8
//
//    The 8 pillars (all data sourced from FMP via fmp_client.py):
//      1. 5-yr avg P/E    < 22.5   mktCap / avgNetIncome5y
//      2. ROIC            > 9%     latestRoic (returnOnCapitalEmployed from FMP ratios)
//      3. 5-yr Revenue Growth      latest revenue > earliest in incomeStatement
//      4. 5-yr Net Income Growth   latest netIncome > earliest in incomeStatement
//      5. Shares Decrease          sharesOutstanding falling over period (ratios table)
//      6. LT Debt / avg FCF < 5    latestLongTermDebt / avgFcf5y  (balanceSheet + cashFlow)
//      7. 5-yr FCF Growth          latest freeCashFlow > earliest in cashFlow
//      8. 5-yr avg P/FCF  < 20     mktCap / avgFcf5y
// ---------------------------------------------------------------------------
scoreTicker:{[sym; lastPrice]
  m: first select from pillarMetrics where symbol=sym;
  if[0=count m; :0];

  shares: m`latestSharesOutstanding;
  mktCap: lastPrice * shares;

  // Pillar 1: 5-yr avg P/E < 22.5
  peRatio: $[m[`avgNetIncome5y]>0; mktCap % m`avgNetIncome5y; 0w];

  // Pillar 8: 5-yr avg P/FCF < 20
  pfcfRatio: $[m[`avgFcf5y]>0; mktCap % m`avgFcf5y; 0w];

  // Pillar 3: 5-yr revenue growth  (newest row first → index 0 = latest)
  revRows: select revenue from incomeStatement where symbol=sym;
  revGrowthPass: $[1<count revRows;
    first[revRows`revenue] > last[revRows`revenue];
    0b];

  // Pillar 4: 5-yr net income growth
  niRows: select netIncome from incomeStatement where symbol=sym;
  niGrowthPass: $[1<count niRows;
    first[niRows`netIncome] > last[niRows`netIncome];
    0b];

  // Pillar 5: decrease in shares outstanding over the period
  shareRows: select sharesOutstanding from ratios where symbol=sym;
  buybackPass: $[1<count shareRows;
    last[shareRows`sharesOutstanding] < first[shareRows`sharesOutstanding];
    0b];

  // Pillar 6: long-term debt / 5-yr avg FCF < 5
  ltDebtFcfPass: $[m[`avgFcf5y]>0;
    (m[`latestLongTermDebt] % m`avgFcf5y) < 5;
    0b];

  // Pillar 7: 5-yr FCF growth
  fcfRows: select freeCashFlow from cashFlow where symbol=sym;
  fcfGrowthPass: $[1<count fcfRows;
    first[fcfRows`freeCashFlow] > last[fcfRows`freeCashFlow];
    0b];

  pillars: (
    peRatio      < 22.5;    // 1. 5-yr avg P/E < 22.5
    m[`latestRoic] > 0.09;  // 2. ROIC > 9%
    revGrowthPass;           // 3. 5-yr revenue growth
    niGrowthPass;            // 4. 5-yr net income growth
    buybackPass;             // 5. shares decreasing
    ltDebtFcfPass;           // 6. LT debt / avg FCF < 5
    fcfGrowthPass;           // 7. 5-yr FCF growth
    pfcfRatio    < 20.0      // 8. 5-yr avg P/FCF < 20
  );

  sum pillars
}

// Screen one market universe or both (market=` for all)
// Returns table: symbol, market, score
screenUniverse:{[priceMap; minScore; mkt]
  syms: $[mkt~`;
    exec symbol from pillarMetrics;
    exec symbol from pillarMetrics where market=mkt
  ];
  scores: syms !{[s] scoreTicker[s; priceMap s]} each syms;
  mkts:   syms ! (exec market from pillarMetrics where symbol in syms);
  t: ([] symbol: key scores; market: mkts[key scores]; score: value scores);
  select from t where score >= minScore
}

// ---------------------------------------------------------------------------
.z.pi:{show "schema.q loaded — tables: incomeStatement balanceSheet cashFlow ratios ohlc pillarMetrics";}
// ---------------------------------------------------------------------------
