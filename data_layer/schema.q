// =============================================================================
// schema.q  —  JARVIS KDB+ table definitions
//
// Supports the 8 Pillar fundamental model + OHLC price data for Nifty 50.
// Designed for kdb+ 32-bit (4 GB RAM limit) using splayed on-disk tables.
//
// Load order: schema.q must be loaded before kdb_loader.q
//   q schema.q
// =============================================================================

// ---------------------------------------------------------------------------
// 1. FUNDAMENTALS  — annual per-ticker rows (splayed, keyed on date+symbol)
// ---------------------------------------------------------------------------

// Income Statement  (Pillars 1, 2, 6)
// Revenue  → revenue quality / growth screening
// Net Income → Pillar 1 normalised P/E denominator
// EBIT     → enterprise value multiples
incomeStatement:(
  [date:`date$(); symbol:`symbol$()]
  revenue:`float$();
  netIncome:`float$();
  ebit:`float$()
)

// Balance Sheet  (Pillars 4, 5, 6)
// Equity / debt structure for capital-allocation and moat scoring
balanceSheet:(
  [date:`date$(); symbol:`symbol$()]
  totalAssets:`float$();
  totalLiabilities:`float$();
  longTermDebt:`float$();
  totalEquity:`float$()
)

// Cash Flow Statement  (Pillar 8)
// Free cash flow is the primary metric for Pillar 8 P/FCF < 22.5
cashFlow:(
  [date:`date$(); symbol:`symbol$()]
  freeCashFlow:`float$()
)

// Financial Ratios  (Pillars 3, 7)
// ROIC → capital efficiency (Pillar 7)
// sharesOutstanding → buyback tracking (Pillar 3: declining share count = good)
ratios:(
  [date:`date$(); symbol:`symbol$()]
  roic:`float$();
  sharesOutstanding:`float$()
)

// ---------------------------------------------------------------------------
// 2. OHLC  — daily price data (partitioned by year via kdb_loader.q)
// ---------------------------------------------------------------------------
// Stored as a flat splayed table; kdb_loader partitions by date.
// Volume as long (64-bit int) to handle large NSE volumes without overflow.
ohlc:(
  [date:`date$(); symbol:`symbol$()]
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
// Pillar 1  : peRatio        = latestPrice / avgNetIncome5y * sharesOutstanding
// Pillar 8  : pfcfRatio      = latestPrice / avgFcf5y       * sharesOutstanding
// Both ratios are computed at query time against live price from ohlc table.
pillarMetrics:(
  [symbol:`symbol$()]
  avgNetIncome5y:`float$();        // 5-year mean net income (INR)
  avgFcf5y:`float$();              // 5-year mean free cash flow (INR)
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
// 4. Derived views  — computed on load; not persisted to avoid 4 GB pressure
// ---------------------------------------------------------------------------

// Net debt: used for enterprise value and leverage scoring (Pillar 5)
// Usage: select symbol, netDebt from netDebtView where date=max date
netDebtView:{
  select date, symbol,
    netDebt: longTermDebt - (totalAssets - totalLiabilities - longTermDebt)
  from balanceSheet
}

// Equity/Assets ratio: quick balance-sheet quality check
equityRatioView:{
  select date, symbol,
    equityRatio: totalEquity % totalAssets
  from balanceSheet
}

// ---------------------------------------------------------------------------
// 5. Pillar scoring function  (called from analytics layer)
//    Scores a ticker 1–8 based on which pillars pass.
//    Requires live lastPrice (INR) passed as argument.
//
//    Pillar thresholds:
//      1. P/E    < 22.5   (normalised 5y earnings)
//      2. P/B    < 1.5    (price / book)
//      3. Buybacks        (shares declining YoY)
//      4. Low Debt        (longTermDebt / totalEquity < 0.5)
//      5. Current Ratio   (placeholder — needs current assets data)
//      6. Revenue Growth  (latestRevenue > prior year)
//      7. ROIC    > 12%
//      8. P/FCF   < 22.5  (normalised 5y FCF)
// ---------------------------------------------------------------------------
scoreTicker:{[sym; lastPrice]
  m: first select from pillarMetrics where symbol=sym;
  if[0=count m; :0];                          // ticker not found → 0

  shares: m`latestSharesOutstanding;
  mktCap: lastPrice * shares;

  peRatio:   $[m[`avgNetIncome5y]>0; mktCap % m`avgNetIncome5y; 0w];
  pfcfRatio: $[m[`avgFcf5y]>0;      mktCap % m`avgFcf5y;       0w];
  pbRatio:   $[m[`latestTotalEquity]>0; mktCap % m`latestTotalEquity; 0w];
  debtRatio: $[m[`latestTotalEquity]>0;
               m[`latestLongTermDebt] % m`latestTotalEquity; 0w];

  // Share count trend: compare latest vs earliest annual row
  shareRows: select sharesOutstanding from ratios where symbol=sym;
  buybackPass: $[1<count shareRows;
    last[shareRows`sharesOutstanding] < first[shareRows`sharesOutstanding];
    0b];

  // Revenue trend: latest vs prior year
  revRows: select revenue from incomeStatement where symbol=sym;
  revGrowthPass: $[1<count revRows;
    first[revRows`revenue] > last[revRows`revenue];
    0b];

  pillars: (
    peRatio   < 22.5;                     // 1. Earnings yield
    pbRatio   < 1.5;                      // 2. Price to book
    buybackPass;                           // 3. Share buybacks
    debtRatio < 0.5;                       // 4. Low leverage
    1b;                                    // 5. Placeholder (current ratio)
    revGrowthPass;                         // 6. Revenue growth
    m[`latestRoic] > 0.12;                // 7. ROIC > 12 %
    pfcfRatio < 22.5                       // 8. P/FCF
  );

  sum pillars  // score out of 8
}

// Quick screen: return all tickers with score >= minScore given a price map
screenUniverse:{[priceMap; minScore]
  syms: exec symbol from pillarMetrics;
  scores: syms !{[s] scoreTicker[s; priceMap s]} each syms;
  select from ([] symbol: key scores; score: value scores) where score >= minScore
}

// ---------------------------------------------------------------------------
.z.pi:{show "schema.q loaded — tables: incomeStatement balanceSheet cashFlow ratios ohlc pillarMetrics";}
// ---------------------------------------------------------------------------
