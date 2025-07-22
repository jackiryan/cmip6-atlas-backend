FROM postgres:17-bullseye

RUN apt-get update && apt-get install -y \
    postgis \
    postgresql-17-postgis-3 \
    postgresql-17-postgis-3-scripts \
    python3 \
    python3-pip \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install psycopg2-binary>=2.9.9 shapely

# Copy configuration and scripts
COPY scripts/ /usr/local/bin/

COPY data/ /data/

RUN chmod +x /usr/local/bin/seed-db.sh /usr/local/bin/seed_cmip6_atlas.py

CMD ["postgres"]