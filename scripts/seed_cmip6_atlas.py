#!/usr/bin/env python3
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection as PostgresConnection
from shapely.geometry import shape
import sys
import argparse

def create_table(connection: PostgresConnection) -> None:
    """Create the regions table with all necessary columns."""
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS regions CASCADE;")
        
        # Create table with all fields from the global regions GeoJSON file
        create_table_sql = """
        CREATE TABLE regions (
            id SERIAL PRIMARY KEY,
            region_id INTEGER,
            region_identifier VARCHAR(255),
            source_country_code VARCHAR(10),
            source_country_name VARCHAR(255),
            source_admin_level INTEGER,
            source_filename VARCHAR(255),
            gid_0 VARCHAR(50),
            country VARCHAR(255),
            gid_1 VARCHAR(50),
            name_1 VARCHAR(255),
            varname_1 VARCHAR(255),
            nl_name_1 VARCHAR(255),
            type_1 VARCHAR(100),
            engtype_1 VARCHAR(100),
            cc_1 VARCHAR(50),
            hasc_1 VARCHAR(50),
            iso_1 VARCHAR(50),
            gid_2 VARCHAR(50),
            name_2 VARCHAR(255),
            varname_2 VARCHAR(255),
            nl_name_2 VARCHAR(255),
            type_2 VARCHAR(100),
            engtype_2 VARCHAR(100),
            cc_2 VARCHAR(50),
            hasc_2 VARCHAR(50),
            geom GEOMETRY(GEOMETRY, 4326)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create spatial index
        cursor.execute("CREATE INDEX idx_regions_geom ON regions USING GIST (geom);")
        
        # Create indexes on commonly queried fields
        cursor.execute("CREATE INDEX idx_regions_region_id ON regions (region_id);")
        cursor.execute("CREATE INDEX idx_regions_country_code ON regions (source_country_code);")
        cursor.execute("CREATE INDEX idx_regions_admin_level ON regions (source_admin_level);")
        cursor.execute("CREATE INDEX idx_regions_name_1 ON regions (name_1);")
        
        connection.commit()
        print("✅ Table 'regions' created successfully")

def insert_features(conn, geojson_file):
    """Insert features from GeoJSON file into PostGIS."""
    # Load GeoJSON file
    with open(geojson_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if data['type'] != 'FeatureCollection':
        raise ValueError("Input file must be a GeoJSON FeatureCollection")
    
    features = data['features']
    total = len(features)
    print(f"Found {total} features to import")
    
    with conn.cursor() as cur:
        insert_sql = """
        INSERT INTO regions (
            region_id, region_identifier, source_country_code, source_country_name,
            source_admin_level, source_filename, gid_0, country, gid_1, name_1,
            varname_1, nl_name_1, type_1, engtype_1, cc_1, hasc_1, iso_1,
            gid_2, name_2, varname_2, nl_name_2, type_2, engtype_2, cc_2, hasc_2,
            geom
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326)
        )
        """
        
        batch_size = 100
        for i, feature in enumerate(features):
            props = feature['properties']
            
            # Convert geometry to WKT
            geom = shape(feature['geometry'])
            geom_wkt = geom.wkt
            
            # Prepare values, handling None/null values
            values = (
                props.get('region_id'),
                props.get('region_identifier'),
                props.get('source_country_code'),
                props.get('source_country_name'),
                props.get('source_admin_level'),
                props.get('source_filename'),
                props.get('GID_0'),
                props.get('COUNTRY'),
                props.get('GID_1'),
                props.get('NAME_1'),
                props.get('VARNAME_1'),
                props.get('NL_NAME_1'),
                props.get('TYPE_1'),
                props.get('ENGTYPE_1'),
                props.get('CC_1'),
                props.get('HASC_1'),
                props.get('ISO_1'),
                props.get('GID_2'),
                props.get('NAME_2'),
                props.get('VARNAME_2'),
                props.get('NL_NAME_2'),
                props.get('TYPE_2'),
                props.get('ENGTYPE_2'),
                props.get('CC_2'),
                props.get('HASC_2'),
                geom_wkt
            )
            
            cur.execute(insert_sql, values)
            
            # Commit in batches
            if (i + 1) % batch_size == 0:
                conn.commit()
                print(f"✅ Imported {i + 1}/{total} features", end='\r')
        
        conn.commit()
        print(f"\n✅ Successfully imported all {total} features")

def validate_import(conn):
    """Validate the import by checking row count and geometry validity."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Check total count
        cur.execute("SELECT COUNT(*) as count FROM regions;")
        total = cur.fetchone()['count']
        print(f"\n📊 Validation Results:")
        print(f"   Total features: {total}")
        
        # Check admin level distribution
        cur.execute("""
            SELECT source_admin_level, COUNT(*) as count 
            FROM regions 
            GROUP BY source_admin_level 
            ORDER BY source_admin_level;
        """)
        print("   Features by admin level:")
        for row in cur.fetchall():
            level = row['source_admin_level'] or 'NULL'
            print(f"     Level {level}: {row['count']}")
        
        # Check for invalid geometries
        cur.execute("SELECT COUNT(*) as count FROM regions WHERE NOT ST_IsValid(geom);")
        invalid = cur.fetchone()['count']
        if invalid > 0:
            print(f"   ⚠️  Warning: {invalid} invalid geometries found")

            # Get details about invalid geometries
            cur.execute("""
                SELECT
                    region_id,
                    region_identifier,
                    source_country_name,
                    name_1,
                    name_2,
                    ST_IsValidReason(geom) as invalid_reason
                FROM regions
                WHERE NOT ST_IsValid(geom)
                ORDER BY region_id;
            """)
            print("   \n   Invalid geometries details:")
            for row in cur.fetchall():
                region_name = row['name_2'] or row['name_1'] or row['region_identifier'] or f"Region {row['region_id']}"
                country = row['source_country_name'] or 'Unknown'
                print(f"     • {region_name} ({country}) - {row['invalid_reason']}")

            # Attempt to fix invalid geometries
            cur.execute("UPDATE regions SET geom = ST_MakeValid(geom) WHERE NOT ST_IsValid(geom);")
            conn.commit()
            print("\n   ✅ Invalid geometries fixed using ST_MakeValid()")
        else:
            print("   ✅ All geometries are valid")
        
        # Check geometry types
        cur.execute("""
            SELECT ST_GeometryType(geom) as geom_type, COUNT(*) as count 
            FROM regions 
            GROUP BY ST_GeometryType(geom);
        """)
        print("   Geometry types:")
        for row in cur.fetchall():
            print(f"     {row['geom_type']}: {row['count']}")

def main():
    parser = argparse.ArgumentParser(description='Load GeoJSON into PostGIS for Martin tile serving')
    parser.add_argument('geojson_file', help='Path to GeoJSON file')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host (default: localhost)')
    parser.add_argument('--port', default=5432, type=int, help='PostgreSQL port (default: 5432)')
    parser.add_argument('--database', default='cmip6_atlas', help='Database name (default: regions_db)')
    parser.add_argument('--user', default='postgres', help='Database user (default: postgres)')
    parser.add_argument('--password', default='postgres', help='Database password (default: postgres)')
    
    args = parser.parse_args()
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password
        )
        print(f"✅ Connected to PostgreSQL database '{args.database}'")
        
        # Enable PostGIS extension
        with conn.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            conn.commit()
            print("✅ PostGIS extension enabled")
        
        # Create table
        create_table(conn)
        
        # Insert features
        insert_features(conn, args.geojson_file)
        
        # Validate
        validate_import(conn)
        
        print("\n✅ Import completed successfully!")
        print(f"   pg_tileserv should now serve tiles at: http://localhost:7800/public.regions.json")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close() # type: ignore

if __name__ == "__main__":
    main()
