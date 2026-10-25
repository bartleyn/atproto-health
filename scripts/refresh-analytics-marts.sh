#!/bin/zsh
# Weekly refresh for the /analytics page: rebuild cohort_base/excluded_dids
# from the current plc tables, incrementally update the marts, then republish
# cache/analytics.json. Invoked by
# ~/Library/LaunchAgents/local.atproto-health.refresh-analytics-marts.plist.
set -euo pipefail
cd /Users/nathanbartley/srv/atproto-health
npx tsx --env-file .env src/lib/analysis/build-analysis-db.ts
npx tsx --env-file .env src/lib/analysis/refresh-analytics-marts.ts
npx tsx --env-file .env src/lib/analysis/analytics-cache.ts
