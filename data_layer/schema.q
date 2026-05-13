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
// 4. Derived views
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
//    Pillar thresholds:
//      1. P/E    < 22.5   (normalised 5y earnings)
//      2. P/B    < 1.5    (price / book)
//      3. Buybacks        (shares declining over period)
//      4. Low Debt        (longTermDebt / totalEquity < 0.5)
//      5. Current Ratio   (placeholder — needs current assets data)
//      6. Revenue Growth  (latest > prior year)
//      7. ROIC    > 12%
//      8. P/FCF   < 22.5  (normalised 5y FCF)
// ---------------------------------------------------------------------------
scoreTicker:{[sym; lastPrice]
  m: first select from pillarMetrics where symbol=sym;
  if[0=count m; :0];

  shares: m`latestSharesOutstanding;
  mktCap: lastPrice * shares;

  peRatio:   $[m[`avgNetIncome5y]>0; mktCap % m`avgNetIncome5y; 0w];
  pfcfRatio: $[m[`avgFcf5y]>0;      mktCap % m`avgFcf5y;       0w];
  pbRatio:   $[m[`latestTotalEquity]>0; mktCap % m`latestTotalEquity; 0w];
  debtRatio: $[m[`latestTotalEquity]>0;
               m[`latestLongTermDebt] % m`latestTotalEquity; 0w];

  shareRows: select sharesOutstanding from ratios where symbol=sym;
  buybackPass: $[1<count shareRows;
    last[shareRows`sharesOutstanding] < first[shareRows`sharesOutstanding];
    0b];

  revRows: select revenue from incomeStatement where symbol=sym;
  revGrowthPass: $[1<count revRows;
    first[revRows`revenue] > last[revRows`revenue];
    0b];

  pillars: (
    peRatio   < 22.5;
    pbRatio   < 1.5;
    buybackPass;
    debtRatio < 0.5;
    1b;                           // Pillar 5 placeholder (current ratio)
    revGrowthPass;
    m[`latestRoic] > 0.12;
    pfcfRatio < 22.5
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
