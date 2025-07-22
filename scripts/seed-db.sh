#!/bin/bash
set -e

until pg_isready; do
    echo "Waiting for PostgreSQL to start..."
    sleep 1
done

/usr/local/bin/seed_cmip6_atlas.py \
    /data/sources/global_regions.geojson \
    --database cmip6_atlas --host localhost --user postgres --password ${POSTGRES_PASSWORD}