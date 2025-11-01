#!/bin/bash
set -e

echo "Seeding regions data..."

# Use the POSTGRES_* environment variables that the entrypoint sets
# During init, connect via the Unix socket, not TCP
PGPASSWORD="${POSTGRES_PASSWORD}" /usr/local/bin/seed_cmip6_atlas.py \
    /data/sources/global_regions.geojson \
    --database "${POSTGRES_DB}" \
    --host /var/run/postgresql \
    --user "${POSTGRES_USER}" \
    --password "${POSTGRES_PASSWORD}"

echo "Seeding climate metrics..."
PGPASSWORD="${POSTGRES_PASSWORD}" /usr/local/bin/seed_climate_metrics.py \
    /data/sources/ \
    --database "${POSTGRES_DB}" \
    --host /var/run/postgresql \
    --user "${POSTGRES_USER}" \
    --password "${POSTGRES_PASSWORD}"

echo "Database seeding completed!"
