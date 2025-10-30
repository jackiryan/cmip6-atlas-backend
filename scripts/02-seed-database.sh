#!/bin/bash
set -e

echo "Running database seeding script..."

# Use the POSTGRES_* environment variables that the entrypoint sets
# During init, we connect via the Unix socket, not TCP
PGPASSWORD="${POSTGRES_PASSWORD}" /usr/local/bin/seed_cmip6_atlas.py \
    /data/sources/global_regions.geojson \
    --database "${POSTGRES_DB}" \
    --host /var/run/postgresql \
    --user "${POSTGRES_USER}" \
    --password "${POSTGRES_PASSWORD}"

echo "Database seeding completed!"
