-- Analytics marts: WAU by cohort, DAU/MAU trend, and D1/D3/D7 retention.
--
-- These back the /analytics page. Unlike migrations/006's deletion matviews,
-- these are plain TABLEs populated incrementally by
-- src/lib/analysis/refresh-analytics-marts.ts (npm run analysis:marts) —
-- historical rows are stable once written and are never recomputed; each run
-- only touches a small trailing window. A full materialized-view rebuild was
-- tried first and rejected: the DAU/MAU trailing-28-day distinct-count costs
-- ~2.5 min PER DATE on this table (195M rows), so recomputing all history on
-- every refresh is a non-starter — see the refresh script for the real numbers
-- and the incremental strategy.

CREATE TABLE IF NOT EXISTS analysis.wau_by_cohort (
  week_start date NOT NULL,
  age_bucket text NOT NULL,
  wau        int  NOT NULL,
  PRIMARY KEY (week_start, age_bucket)
);

CREATE TABLE IF NOT EXISTS analysis.dau_mau_daily (
  date        text    NOT NULL PRIMARY KEY, -- matches activity.did_activity_daily's text date column
  dau         int     NOT NULL,
  mau         int     NOT NULL,
  dau_mau_pct float8  NOT NULL
);

CREATE TABLE IF NOT EXISTS analysis.retention_cohorts (
  creation_date date NOT NULL,
  pds_type      text NOT NULL,
  cohort_size   int  NOT NULL,
  d1_n          int  NOT NULL,
  d1_pct        float8 NOT NULL,
  d3_n          int  NOT NULL,
  d3_pct        float8 NOT NULL,
  d7_n          int  NOT NULL,
  d7_pct        float8 NOT NULL,
  PRIMARY KEY (creation_date, pds_type)
);
