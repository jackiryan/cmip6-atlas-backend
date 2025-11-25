FROM postgres:18-trixie

RUN apt-get update && apt-get install -y \
    postgis \
    postgresql-18-postgis-3 \
    postgresql-18-postgis-3-scripts \
    python3 \
    python3-pip \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --break-system-packages psycopg2-binary>=2.9.9 shapely

# Copy initialization scripts to auto-enable PostGIS and seed data
COPY scripts/01-init-postgis.sql /docker-entrypoint-initdb.d/
COPY scripts/02-seed-database.sh /docker-entrypoint-initdb.d/

# Copy configuration and scripts
COPY scripts/ /usr/local/bin/

COPY data/ /data/

RUN chmod +x /usr/local/bin/seed_cmip6_atlas.py /usr/local/bin/seed_climate_metrics.py /usr/local/bin/healthcheck.sh /docker-entrypoint-initdb.d/02-seed-database.sh
# Convert line endings to Unix format and set executable permissions
RUN sed -i 's/\r$//' /docker-entrypoint-initdb.d/02-seed-database.sh \
    && sed -i 's/\r$//' /usr/local/bin/seed_cmip6_atlas.py \
    && sed -i 's/\r$//' /usr/local/bin/seed_climate_metrics.py \
    && sed -i 's/\r$//' /usr/local/bin/healthcheck.sh

CMD ["postgres"]