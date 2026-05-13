// =============================================================================
// kdb_loader.q  —  JARVIS CSV → KDB+ ingestion
//
// Reads the CSVs produced by fmp_client.py and writes splayed / date-partitioned
// tables that stay well within the kdb+ 32-bit 4 GB RAM limit.
//
// Usage:
//   q data_layer/kdb_loader.q [-p 5000]      // loads everything, optional port
//   q data_layer/kdb_loader.q -load ohlc     // load only OHLC partitions
//
// On-disk layout created under db/:
//   db/
//   ├── fundamentals/               ← splayed tables (columnar, mmap'd lazily)
//   │   ├── incomeStatement/
//   │   ├── balanceSheet/
//   │   ├── cashFlow/
//   │   ├── ratios/
//   │   └── pillarMetrics/
//   └── ohlc/                       ← date-partitioned (one dir per year)
//       ├── 2021/ohlc/
//       ├── 2022/ohlc/
//       ├── 2023/ohlc/
//       ├── 2024/ohlc/
//       └── 2025/ohlc/
// =============================================================================

\l data_layer/schema.q

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------
FUND_CSV:  `$":data/fundamentals/"
OHLC_CSV:  `$":data/ohlc/"
DB:        `$":db/"

// ---------------------------------------------------------------------------
// CSV column type strings (kdb+ .h.cd / 0: format)
//   D = date, S = symbol, F = float, J = long
// ---------------------------------------------------------------------------
INCOME_TYPES:   "DSFFFF"
BALANCE_TYPES:  "DSFFFF"
CASHFLOW_TYPES: "DSF"
RATIOS_TYPES:   "DSFF"
OHLC_TYPES:     "DSFFFFJ"
PILLAR_TYPES:   "SFFFFFFFFF"   // symbol + 9 floats (no date column)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Parse a CSV with given type string, first row = header
loadCsv:{[types; path]
  (types; enlist ",") 0: path
}

// Ensure parent directory exists before splay
ensureDir:{[dir] if[not (type key dir)=9h; .[`; (); {[d] hsym `$string[d]}; dir]]}

// Append rows to a splayed table, or create it if absent.
// kdb+ 32-bit: use `append` (upsert into on-disk col files) for incremental load
splaySave:{[dir; tbl]
  p: hsym `$string[dir];
  if[()~key p;
    @[p; tbl; ,]           // first write: create splay
  ; @[p; tbl; ,:]          // subsequent: append columns
  ]
}

// ---------------------------------------------------------------------------
// Fundamental loaders
// ---------------------------------------------------------------------------

loadIncomeStatement:{[]
  path: ` sv (FUND_CSV; `income_statement.csv);
  show "Loading income_statement.csv …";
  t: loadCsv[INCOME_TYPES; path];
  // Cast kdb+ date column (already YYYY.MM.DD from Python)
  t: update date:`date$date from t;
  dir: ` sv (DB; `fundamentals/incomeStatement/);
  ensureDir dir;
  (` sv dir, `.) set .Q.en[DB] t;          // enumerate symbols, splay
  show "  income_statement → ", string[count t], " rows";
 }

loadBalanceSheet:{[]
  path: ` sv (FUND_CSV; `balance_sheet.csv);
  show "Loading balance_sheet.csv …";
  t: loadCsv[BALANCE_TYPES; path];
  t: update date:`date$date from t;
  dir: ` sv (DB; `fundamentals/balanceSheet/);
  ensureDir dir;
  (` sv dir, `.) set .Q.en[DB] t;
  show "  balance_sheet → ", string[count t], " rows";
 }

loadCashFlow:{[]
  path: ` sv (FUND_CSV; `cash_flow.csv);
  show "Loading cash_flow.csv …";
  t: loadCsv[CASHFLOW_TYPES; path];
  t: update date:`date$date from t;
  dir: ` sv (DB; `fundamentals/cashFlow/);
  ensureDir dir;
  (` sv dir, `.) set .Q.en[DB] t;
  show "  cash_flow → ", string[count t], " rows";
 }

loadRatios:{[]
  path: ` sv (FUND_CSV; `ratios.csv);
  show "Loading ratios.csv …";
  t: loadCsv[RATIOS_TYPES; path];
  t: update date:`date$date from t;
  dir: ` sv (DB; `fundamentals/ratios/);
  ensureDir dir;
  (` sv dir, `.) set .Q.en[DB] t;
  show "  ratios → ", string[count t], " rows";
 }

loadPillarMetrics:{[]
  path: ` sv (FUND_CSV; `pillar_metrics.csv);
  show "Loading pillar_metrics.csv …";
  t: loadCsv[PILLAR_TYPES; path];
  dir: ` sv (DB; `fundamentals/pillarMetrics/);
  ensureDir dir;
  (` sv dir, `.) set .Q.en[DB] t;
  show "  pillar_metrics → ", string[count t], " rows";
 }

// ---------------------------------------------------------------------------
// OHLC loader  — date-partitioned for memory efficiency
//
// Each ohlc_YYYY.csv is written into db/YYYY/ohlc/ so that kdb+ only
// maps the partitions actually queried into memory (critical for 32-bit).
// ---------------------------------------------------------------------------

loadOhlcYear:{[csvFile]
  // Extract year from filename: ohlc_2023.csv → 2023
  yr: `long$ string[csvFile] except "ohlc_.csv:";  // naive but reliable
  // Re-derive using file stem
  stem: last ` vs csvFile;
  yr:   `int$ last "_" vs string stem;

  show "Loading ", string[csvFile], " (year=", string[yr], ") …";
  t: loadCsv[OHLC_TYPES; hsym csvFile];
  t: update date:`date$date from t;

  // Sort by date within each symbol for efficient kdb+ queries
  t: `date xasc t;

  // Write partition: db/YYYY/ohlc/
  partDir: ` sv (DB; `$string[yr]; `ohlc/);
  ensureDir partDir;
  (` sv partDir, `.) set .Q.en[DB] t;
  show "  year ", string[yr], " → ", string[count t], " rows";
 }

loadAllOhlc:{[]
  // Discover all ohlc_*.csv files in data/ohlc/
  csvFiles: key ` sv (OHLC_CSV; `);
  ohlcFiles: csvFiles where csvFiles like "ohlc_????.csv";
  if[0=count ohlcFiles;
    show "WARNING: no ohlc_YYYY.csv files found in data/ohlc/";
    :()
  ];
  loadOhlcYear each ` sv/: (OHLC_CSV;) each ohlcFiles;
 }

// ---------------------------------------------------------------------------
// Full load sequence
// ---------------------------------------------------------------------------

loadAll:{[]
  show "=== JARVIS KDB+ loader starting ===";
  loadIncomeStatement[];
  loadBalanceSheet[];
  loadCashFlow[];
  loadRatios[];
  loadPillarMetrics[];
  loadAllOhlc[];

  // Write partition metadata so \l db works correctly for partitioned ohlc
  .Q.dpft[DB; `ohlc; `date; `symbol];

  show "=== Load complete. Run: \\l db ===";
 }

// ---------------------------------------------------------------------------
// Post-load: bring tables into session from disk
// ---------------------------------------------------------------------------

openDb:{[]
  \l db
  show "Tables available: ", " " sv string tables[];
 }

// ---------------------------------------------------------------------------
// Memory usage report  (critical for 32-bit 4 GB cap)
// ---------------------------------------------------------------------------

memReport:{[]
  show "--- Memory report ---";
  show "Heap used (MB): ", string .Q.w[][`heap] % 1e6;
  show "Mapped (MB):    ", string .Q.w[][`mapped] % 1e6;
  show "Physical (MB):  ", string .Q.w[][`physical] % 1e6;
  if[.Q.w[][`heap] > 3e9;
    show "WARNING: heap > 3 GB — approaching 32-bit limit!";
  ]
 }

// ---------------------------------------------------------------------------
// CLI entry  — run loadAll when script is executed directly
// ---------------------------------------------------------------------------

if[`load in key .z.x;
  // selective load mode: q kdb_loader.q -load ohlc
  target: .z.x `load;
  $[target~"ohlc";   loadAllOhlc[];
    target~"income";  loadIncomeStatement[];
    target~"balance"; loadBalanceSheet[];
    target~"cashflow";loadCashFlow[];
    target~"ratios";  loadRatios[];
    target~"pillars"; loadPillarMetrics[];
    show "Unknown target: ", target]
; // default: load everything
  loadAll[];
 ]

memReport[];
