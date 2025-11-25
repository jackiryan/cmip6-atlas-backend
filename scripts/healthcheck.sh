#!/bin/bash
set -e

# Check if PostgreSQL is ready
pg_isready -U postgres -d cmip6_atlas || exit 1

# Check if all required tables exist (meaning seeding is complete)
TABLE_COUNT=$(psql -U postgres -d cmip6_atlas -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('regions', 'metrics', 'scenarios', 'climate_data', 'climate_averages');")

if [ "$TABLE_COUNT" -eq 5 ]; then
    exit 0
else
    exit 1
fi
