// =============================================================================
// kdb_loader.q  —  JARVIS CSV → KDB+ ingestion
//
// Reads market-specific CSVs produced by fmp_client.py and writes splayed /
// date-partitioned tables under db/.  Both IN (Nifty 50) and US (S&P 500)
// data are merged into the same tables, distinguished by the `market` column.
//
// CSV naming convention (written by Python layer):
//   data/fundamentals/income_statement_in.csv   ← Indian stocks
//   data/fundamentals/income_statement_us.csv   ← US stocks
//   data/ohlc/ohlc_YYYY_in.csv
//   data/ohlc/ohlc_YYYY_us.csv
//
// Usage:
//   q data_layer/kdb_loader.q              // load all markets, all tables
//   q data_layer/kdb_loader.q -load ohlc   // only OHLC
//   q data_layer/kdb_loader.q -load income
//   q data_layer/kdb_loader.q -market US   // only US data
//   q data_layer/kdb_loader.q -market IN   // only Indian data
//
// On-disk layout:
//   db/
//   ├── fundamentals/
//   │   ├── incomeStatement/
//   │   ├── balanceSheet/
//   │   ├── cashFlow/
//   │   ├── ratios/
//   │   └── pillarMetrics/
//   └── YYYY/ohlc/          (date-partitioned, one dir per year)
// =============================================================================

\l data_layer/schema.q

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------
FUND_CSV: `$":data/fundamentals/"
OHLC_CSV: `$":data/ohlc/"
DB:       `$":db/"

// ---------------------------------------------------------------------------
// CSV column type strings
//   D=date  S=symbol  F=float  J=long
//
// Column order mirrors Python CSV writers in fmp_client.py:
//   fundamentals: date, symbol, market, <floats>
//   ohlc:         date, symbol, market, open, high, low, close, volume
//   pillar:       symbol, market, <9 floats>
// ---------------------------------------------------------------------------
INCOME_TYPES:  "DSSFFF"      // date, symbol, market, revenue, netIncome, ebit
BALANCE_TYPES: "DSSFFFF"     // date, symbol, market, totalAssets, totalLiabilities, longTermDebt, totalEquity
CASHFLOW_TYPES:"DSSF"        // date, symbol, market, freeCashFlow
RATIOS_TYPES:  "DSSFF"       // date, symbol, market, roic, sharesOutstanding
OHLC_TYPES:    "DSSFFFFJ"    // date, symbol, market, open, high, low, close, volume
PILLAR_TYPES:  "SSFFFFFFFFF" // symbol, market, 9 floats

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

loadCsv:{[types; path]
  (types; enlist ",") 0: path
}

// Concatenate two tables; handles the case where either may be empty
safeCat:{[t1; t2]
  $[0=count t1; t2;
    0=count t2; t1;
    t1, t2]
}

// Load one CSV if it exists, return empty table otherwise
loadIfExists:{[types; path]
  $[()~key hsym path;
    (enlist"")!enlist();   // missing file → placeholder; caller checks count
    loadCsv[types; hsym path]
  ]
}

ensureDir:{[dir] if[not (type key dir)=9h; .[`; (); {[d] hsym `$string[d]}; dir]]}

splaySave:{[dir; tbl]
  p: hsym `$string[dir];
  if[()~key p;
    @[p; tbl; ,]
  ; @[p; tbl; ,:]
  ]
}

// ---------------------------------------------------------------------------
// Resolve which market suffixes to load
// ---------------------------------------------------------------------------
MARKETS: `IN`US                    // both by default

// If -market flag passed on CLI, restrict to that suffix
if[`market in key .z.x;
  MARKETS: enlist `$upper .z.x`market
 ]

// ---------------------------------------------------------------------------
// Generic fundamental loader
//   name     : table name atom (e.g. `incomeStatement)
//   baseName : CSV file stem   (e.g. "income_statement")
//   types    : kdb+ type string
//   dir      : on-disk splay directory path string
// ---------------------------------------------------------------------------
loadFundamental:{[name; baseName; types; dir]
  show "Loading ", baseName, " …";
  rows: ();
  {[mkts; baseName; types]
    sfx: lower string mkts;
    path: `$":data/fundamentals/", baseName, "_", sfx, ".csv";
    $[()~key hsym path;
      show "  [SKIP] not found: ", string[path];
      [
        t: loadCsv[types; hsym path];
        t: update date:`date$date, market:`symbol$market from t;
        show "  [", upper sfx, "] ", string[count t], " rows";
        rows,: enlist t
      ]
    ]
  }[; baseName; types] each MARKETS;
  if[0=count rows; :()];
  combined: $[1=count rows; first rows; (ssr/)[; (" ";""); raze] rows];  // concat all
  combined: $[1=count rows; first rows; {x,y}/[rows]];
  combined: update date:`date$date from combined;
  dbDir: ` sv (DB; `$"fundamentals/", string[dir], "/");
  ensureDir dbDir;
  (` sv dbDir, `.) set .Q.en[DB] combined;
  show "  → ", string[count combined], " total rows written to db/fundamentals/", string[dir]
 }

// ---------------------------------------------------------------------------
// Fundamental loaders
// ---------------------------------------------------------------------------

loadIncomeStatement:{[]
  show "=== incomeStatement ===";
  all: ();
  {[sfx]
    path: hsym `$":data/fundamentals/income_statement_", sfx, ".csv";
    $[()~key path;
      show "  [SKIP] income_statement_", sfx, ".csv not found";
      [
        t: loadCsv[INCOME_TYPES; path];
        t: update date:`date$date, market:`symbol$market from t;
        show "  [", upper sfx, "] ", string[count t], " rows";
        all,: enlist t
      ]
    ]
  }[;] each string lower each MARKETS;
  if[0=count all; :()];
  combined: {x,y}/[all];
  dir: ` sv (DB; `fundamentals/incomeStatement/);
  ensureDir dir;
  (` sv dir, `.) set .Q.en[DB] combined;
  show "  total → ", string[count combined], " rows"
 }

loadBalanceSheet:{[]
  show "=== balanceSheet ===";
  all: ();
  {[sfx]
    path: hsym `$":data/fundamentals/balance_sheet_", sfx, ".csv";
    $[()~key path;
      show "  [SKIP] balance_sheet_", sfx, ".csv not found";
      [
        t: loadCsv[BALANCE_TYPES; path];
        t: update date:`date$date, market:`symbol$market from t;
        show "  [", upper sfx, "] ", string[count t], " rows";
        all,: enlist t
      ]
    ]
  }[;] each string lower each MARKETS;
  if[0=count all; :()];
  combined: {x,y}/[all];
  dir: ` sv (DB; `fundamentals/balanceSheet/);
  ensureDir dir;
  (` sv dir, `.) set .Q.en[DB] combined;
  show "  total → ", string[count combined], " rows"
 }

loadCashFlow:{[]
  show "=== cashFlow ===";
  all: ();
  {[sfx]
    path: hsym `$":data/fundamentals/cash_flow_", sfx, ".csv";
    $[()~key path;
      show "  [SKIP] cash_flow_", sfx, ".csv not found";
      [
        t: loadCsv[CASHFLOW_TYPES; path];
        t: update date:`date$date, market:`symbol$market from t;
        show "  [", upper sfx, "] ", string[count t], " rows";
        all,: enlist t
      ]
    ]
  }[;] each string lower each MARKETS;
  if[0=count all; :()];
  combined: {x,y}/[all];
  dir: ` sv (DB; `fundamentals/cashFlow/);
  ensureDir dir;
  (` sv dir, `.) set .Q.en[DB] combined;
  show "  total → ", string[count combined], " rows"
 }

loadRatios:{[]
  show "=== ratios ===";
  all: ();
  {[sfx]
    path: hsym `$":data/fundamentals/ratios_", sfx, ".csv";
    $[()~key path;
      show "  [SKIP] ratios_", sfx, ".csv not found";
      [
        t: loadCsv[RATIOS_TYPES; path];
        t: update date:`date$date, market:`symbol$market from t;
        show "  [", upper sfx, "] ", string[count t], " rows";
        all,: enlist t
      ]
    ]
  }[;] each string lower each MARKETS;
  if[0=count all; :()];
  combined: {x,y}/[all];
  dir: ` sv (DB; `fundamentals/ratios/);
  ensureDir dir;
  (` sv dir, `.) set .Q.en[DB] combined;
  show "  total → ", string[count combined], " rows"
 }

loadFundMeta:{[]
  path: hsym `$":data/fund_meta.csv";
  $[()~key path;
    show "  [SKIP] fund_meta.csv not found — run fundamentals job first";
    [
      show "=== fundMeta ===";
      // Columns: symbol, market, last_updated  (types: S S D)
      t: ("SSD"; enlist ",") 0: path;
      t: update lastUpdated:`date$lastUpdated from t;
      dir: ` sv (DB; `fundamentals/fundMeta/);
      ensureDir dir;
      (` sv dir, `.) set .Q.en[DB] t;
      show "  fundMeta → ", string[count t], " rows"
    ]
  ]
 }

loadPillarMetrics:{[]
  show "=== pillarMetrics ===";
  all: ();
  {[sfx]
    path: hsym `$":data/fundamentals/pillar_metrics_", sfx, ".csv";
    $[()~key path;
      show "  [SKIP] pillar_metrics_", sfx, ".csv not found";
      [
        t: loadCsv[PILLAR_TYPES; path];
        t: update market:`symbol$market from t;
        show "  [", upper sfx, "] ", string[count t], " rows";
        all,: enlist t
      ]
    ]
  }[;] each string lower each MARKETS;
  if[0=count all; :()];
  combined: {x,y}/[all];
  dir: ` sv (DB; `fundamentals/pillarMetrics/);
  ensureDir dir;
  (` sv dir, `.) set .Q.en[DB] combined;
  show "  total → ", string[count combined], " rows"
 }

// ---------------------------------------------------------------------------
// OHLC loader  — date-partitioned, one directory per year
//
// Discovers all ohlc_YYYY_in.csv and ohlc_YYYY_us.csv files, groups by year,
// merges IN+US rows for the same year, then writes a single db/YYYY/ohlc/ slab.
// ---------------------------------------------------------------------------

loadOhlcYear:{[yr; sfxList]
  show "=== ohlc year ", string[yr], " ===";
  all: ();
  {[yr; sfx]
    path: hsym `$":data/ohlc/ohlc_", string[yr], "_", sfx, ".csv";
    $[()~key path;
      show "  [SKIP] ohlc_", string[yr], "_", sfx, ".csv not found";
      [
        t: loadCsv[OHLC_TYPES; path];
        t: update date:`date$date, market:`symbol$market from t;
        show "  [", upper sfx, "] ", string[count t], " rows";
        all,: enlist t
      ]
    ]
  }[yr;] each sfxList;
  if[0=count all; :()];
  combined: {x,y}/[all];
  combined: `date xasc combined;
  partDir: ` sv (DB; `$string[yr]; `ohlc/);
  ensureDir partDir;
  (` sv partDir, `.) set .Q.en[DB] combined;
  show "  year ", string[yr], " total → ", string[count combined], " rows"
 }

loadAllOhlc:{[]
  sfxList: string lower each MARKETS;
  // Discover all ohlc_YYYY_*.csv files and collect unique years
  allFiles: key ` sv (OHLC_CSV; `);
  ohlcFiles: string allFiles where allFiles like "ohlc_????_*.csv";
  if[0=count ohlcFiles;
    show "WARNING: no ohlc_YYYY_<market>.csv files found in data/ohlc/";
    :()
  ];
  // Extract years: "ohlc_2023_in.csv" → 2023
  years: asc distinct `int${"_" vs x}[;1] each ohlcFiles;
  loadOhlcYear[; sfxList] each years;
 }

// ---------------------------------------------------------------------------
// Daily incremental OHLC append
//
// Reads data/ohlc/ohlc_daily_in.csv and/or ohlc_daily_us.csv (written by
// python -m data_layer.fmp_client --mode daily), groups rows by year, and
// appends to the correct db/YYYY/ohlc/ partitions.
// Duplicate date+symbol rows are silently dropped (idempotent re-runs).
//
// Usage:
//   q data_layer/kdb_loader.q -mode daily
// ---------------------------------------------------------------------------

appendOhlcDaily:{[]
  show "=== appendOhlcDaily ===";
  sfxList: string lower each MARKETS;

  // Collect all new rows from daily CSV files
  allNew: ();
  {[sfx]
    path: hsym `$":data/ohlc/ohlc_daily_", sfx, ".csv";
    $[()~key path;
      show "  [SKIP] ohlc_daily_", sfx, ".csv not found";
      [
        t: loadCsv[OHLC_TYPES; path];
        t: update date:`date$date, market:`symbol$market from t;
        show "  [", upper sfx, "] ", string[count t], " new rows";
        allNew,: enlist t
      ]
    ]
  }[;] each sfxList;

  if[0=count allNew;
    show "  No daily OHLC files found — nothing to append";
    :()
  ];

  combined: {x,y}/[allNew];

  // Determine which year-partitions are affected
  years: asc distinct `year$combined`date;

  {[yr; newRows]
    yearNew: select from newRows where `year$date = yr;
    partDir: ` sv (DB; `$string[yr]; `ohlc/);

    $[()~key partDir;
      // Partition does not exist — create it fresh
      [
        yearNew: `date xasc yearNew;
        ensureDir partDir;
        (` sv partDir, `.) set .Q.en[DB] yearNew;
        show "  Created partition ", string[yr], " → ", string[count yearNew], " rows"
      ];
      // Partition exists — load, merge, dedup on date+symbol, re-write
      [
        existing: get ` sv partDir, `.;
        // Key on date+symbol; new rows overwrite on conflict (handles corrections)
        merged: (`date`symbol xkey existing) upsert (`date`symbol xkey yearNew);
        merged: `date xasc 0!merged;
        (` sv partDir, `.) set .Q.en[DB] merged;
        show "  Updated partition ", string[yr],
             " → ", string[count[merged] - count[existing]], " new rows",
             " (total: ", string[count merged], ")"
      ]
    ]
  }[; combined] each years;

  .Q.dpft[DB; `ohlc; `date; `symbol];
  show "Daily OHLC append complete"
 }

// ---------------------------------------------------------------------------
// Full load sequence
// ---------------------------------------------------------------------------

loadAll:{[]
  show "=== JARVIS KDB+ loader starting (markets: ", " " sv string MARKETS, ") ===";
  loadIncomeStatement[];
  loadBalanceSheet[];
  loadCashFlow[];
  loadRatios[];
  loadPillarMetrics[];
  loadFundMeta[];
  loadAllOhlc[];
  .Q.dpft[DB; `ohlc; `date; `symbol];
  show "=== Load complete. Run: \\l db ==="
 }

// ---------------------------------------------------------------------------
// Post-load: open DB in session
// ---------------------------------------------------------------------------

openDb:{[]
  \l db
  show "Tables available: ", " " sv string tables[]
 }

// ---------------------------------------------------------------------------
// Memory usage report
// ---------------------------------------------------------------------------

memReport:{[]
  show "--- Memory report ---";
  show "Heap used (MB): ", string .Q.w[][`heap] % 1e6;
  show "Mapped (MB):    ", string .Q.w[][`mapped] % 1e6;
  show "Physical (MB):  ", string .Q.w[][`physical] % 1e6;
  if[.Q.w[][`heap] > 3e9;
    show "WARNING: heap > 3 GB — approaching 32-bit limit!"
  ]
 }

// ---------------------------------------------------------------------------
// CLI entry
// ---------------------------------------------------------------------------

$[`mode in key .z.x;
  // -mode daily  →  incremental OHLC append only
  $[(.z.x`mode)~"daily";
    appendOhlcDaily[];
    show "Unknown -mode: ", .z.x`mode
  ]
; `load in key .z.x;
  // -load <table>  →  selective full reload
  [target: .z.x`load;
   $[target~"ohlc";         loadAllOhlc[];
     target~"income";       loadIncomeStatement[];
     target~"balance";      loadBalanceSheet[];
     target~"cashflow";     loadCashFlow[];
     target~"ratios";       loadRatios[];
     target~"pillars";      loadPillarMetrics[];
     target~"fundmeta";     loadFundMeta[];
     target~"fundamentals"; [loadIncomeStatement[];loadBalanceSheet[];
                              loadCashFlow[];loadRatios[];
                              loadPillarMetrics[];loadFundMeta[]];
     show "Unknown target: ", target]
  ]
; // default: full load everything
  loadAll[]
 ]

memReport[];
