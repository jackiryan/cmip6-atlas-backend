#!/usr/bin/env python3
"""
Seed CMIP6 climate data from JSON files into PostGIS database.
This script processes JSON files with naming convention:
<metric>_global_<scenario>_<start_year>-<end_year>.json
"""
import json
import psycopg2
from psycopg2.extras import execute_values
import re
import sys
import argparse
from pathlib import Path

class ClimateDataSeeder:
    def __init__(self, conn):
        self.conn = conn
        self.metric_cache = {}
        self.scenario_cache = {}
        
    def create_tables(self):
        """Create climate data tables if they don't exist."""
        with self.conn.cursor() as cur:
            # Drop existing tables for clean restart (optional - remove in production)
            cur.execute("DROP TABLE IF EXISTS climate_data CASCADE;")
            cur.execute("DROP TABLE IF EXISTS metrics CASCADE;")
            cur.execute("DROP TABLE IF EXISTS scenarios CASCADE;")
            
            # Create metrics table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    id SERIAL PRIMARY KEY,
                    metric_code VARCHAR(50) UNIQUE NOT NULL,
                    metric_name VARCHAR(255) NOT NULL,
                    unit VARCHAR(50),
                    description TEXT
                );
            """)
            
            # Create scenarios table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scenarios (
                    id SERIAL PRIMARY KEY,
                    scenario_code VARCHAR(20) UNIQUE NOT NULL,
                    scenario_name VARCHAR(255) NOT NULL,
                    description TEXT
                );
            """)
            
            # Create climate data table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS climate_data (
                    id SERIAL PRIMARY KEY,
                    region_id INTEGER NOT NULL,
                    metric_id INTEGER NOT NULL REFERENCES metrics(id),
                    scenario_id INTEGER NOT NULL REFERENCES scenarios(id),
                    year INTEGER NOT NULL,
                    value NUMERIC(12,4),
                    UNIQUE(region_id, metric_id, scenario_id, year)
                );
            """)

            # Create table for averages, this will be populated later
            cur.execute("""
                CREATE TABLE IF NOT EXISTS climate_averages (
                    id SERIAL PRIMARY KEY,
                    region_id INTEGER NOT NULL,
                    metric_id INTEGER NOT NULL REFERENCES metrics(id),
                    scenario_id INTEGER NOT NULL REFERENCES scenarios(id),
                    start_year INTEGER NOT NULL,
                    end_year INTEGER NOT NULL,
                    avg_value NUMERIC(12,4),
                    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_points_count INTEGER, -- Track how many years went into the average
                    UNIQUE(region_id, metric_id, scenario_id, start_year, end_year)
                );
            """)
            
            # Create indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_climate_data_region ON climate_data(region_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_climate_data_metric ON climate_data(metric_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_climate_data_scenario ON climate_data(scenario_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_climate_data_year ON climate_data(year);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_climate_data_composite ON climate_data(region_id, metric_id, scenario_id, year);")
            cur.execute("CREATE INDEX idx_climate_averages_lookup ON climate_averages(region_id, metric_id, scenario_id, start_year, end_year);")
            
            # Create index for performance without foreign key
            cur.execute("CREATE INDEX IF NOT EXISTS idx_climate_data_region_id ON climate_data(region_id);")
            
            self.conn.commit()
            print("✅ Climate data tables created successfully")
    
    def seed_reference_data(self):
        """Seed metrics and scenarios reference data."""
        with self.conn.cursor() as cur:
            # Common metrics mapping
            metrics_data = [
                ('annual_precip', 'Total Annual Precipitation', 'mm', 'Total precipitation accumulated over a year'),
                ('annual_temp', 'Annual Average Temperature', '°C', 'Average temperature over a year'),
                ('summer_temp', 'Summer Average Temperature', '°C', 'Average temperature during summer months'),
                ('winter_temp', 'Winter Average Temperature', '°C', 'Average temperature during winter months'),
                ('extreme_heat_days', 'Extreme Heat Days', 'days', 'Number of days exceeding heat threshold'),
                ('drought_index', 'Drought Index', 'index', 'Standardized drought severity index'),
            ]
            
            for metric_code, name, unit, description in metrics_data:
                cur.execute("""
                    INSERT INTO metrics (metric_code, metric_name, unit, description)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (metric_code) DO UPDATE
                    SET metric_name = EXCLUDED.metric_name,
                        unit = EXCLUDED.unit,
                        description = EXCLUDED.description
                    RETURNING id;
                """, (metric_code, name, unit, description))
                self.metric_cache[metric_code] = cur.fetchone()[0]
            
            # SSP scenarios mapping
            scenarios_data = [
                ('ssp126', 'SSP1-2.6', 'Sustainability – Taking the Green Road (Low challenges to mitigation and adaptation)'),
                ('ssp245', 'SSP2-4.5', 'Middle of the Road (Medium challenges to mitigation and adaptation)'),
                ('ssp370', 'SSP3-7.0', 'Regional Rivalry – A Rocky Road (High challenges to mitigation and adaptation)'),
                ('ssp585', 'SSP5-8.5', 'Fossil-fueled Development (High challenges to mitigation, low challenges to adaptation)'),
                ('historical', 'Historical', 'Historical observed and modeled data'),
            ]
            
            for scenario_code, name, description in scenarios_data:
                cur.execute("""
                    INSERT INTO scenarios (scenario_code, scenario_name, description)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (scenario_code) DO UPDATE
                    SET scenario_name = EXCLUDED.scenario_name,
                        description = EXCLUDED.description
                    RETURNING id;
                """, (scenario_code, name, description))
                self.scenario_cache[scenario_code] = cur.fetchone()[0]
            
            self.conn.commit()
            print(f"✅ Seeded {len(self.metric_cache)} metrics and {len(self.scenario_cache)} scenarios")
    
    def parse_filename(self, filename: str) -> tuple[str, str, int, int]:
        """
        Parse filename to extract metric, scenario, start_year, and end_year.
        Format: <metric>_global_<scenario>_<start_year>-<end_year>.json
        """
        pattern = r'^(.+?)_global_(.+?)_(\d{4})-(\d{4})\.json$'
        match = re.match(pattern, filename)
        if not match:
            raise ValueError(f"Filename {filename} doesn't match expected pattern")
        
        metric = match.group(1)
        scenario = match.group(2)
        start_year = int(match.group(3))
        end_year = int(match.group(4))
        
        return metric, scenario, start_year, end_year
    
    def process_json_file(self, filepath: Path) -> int:
        """Process a single JSON file and insert data into database."""
        filename = filepath.name
        print(f"Processing {filename}...")
        
        try:
            metric_code, scenario_code, start_year, end_year = self.parse_filename(filename)
        except ValueError as e:
            print(f"⚠️  Skipping {filename}: {e}")
            return 0
        
        # Get or create metric and scenario IDs
        if metric_code not in self.metric_cache:
            # Auto-create metric if not in reference data
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO metrics (metric_code, metric_name, unit, description)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (metric_code) DO UPDATE
                    SET metric_code = EXCLUDED.metric_code
                    RETURNING id;
                """, (metric_code, metric_code.replace('_', ' ').title(), 'unknown', f'Auto-generated entry for {metric_code}'))
                self.metric_cache[metric_code] = cur.fetchone()[0]
                self.conn.commit()
        
        if scenario_code not in self.scenario_cache:
            # Auto-create scenario if not in reference data
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO scenarios (scenario_code, scenario_name, description)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (scenario_code) DO UPDATE
                    SET scenario_code = EXCLUDED.scenario_code
                    RETURNING id;
                """, (scenario_code, scenario_code.upper(), f'Auto-generated entry for {scenario_code}'))
                self.scenario_cache[scenario_code] = cur.fetchone()[0]
                self.conn.commit()
        
        metric_id = self.metric_cache[metric_code]
        scenario_id = self.scenario_cache[scenario_code]
        
        # Load JSON data
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Prepare data for bulk insert
        insert_data = []
        for region in data:
            region_id = region['region_id']
            
            # Extract year-value pairs from the region object
            for key, value in region.items():
                # Look for keys that match pattern: <metric>_<year>
                if key.startswith(f'{metric_code}_') or key.startswith(f'total_{metric_code}_') or key.startswith(f'mean_{metric_code}_'):
                    year_match = re.search(r'_(\d{4})$', key)
                    if year_match:
                        year = int(year_match.group(1))
                        if start_year <= year <= end_year and value is not None:
                            insert_data.append((region_id, metric_id, scenario_id, year, value))
        
        # Bulk insert using execute_values for efficiency
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO climate_data (region_id, metric_id, scenario_id, year, value)
                VALUES %s
                ON CONFLICT (region_id, metric_id, scenario_id, year) 
                DO UPDATE SET value = EXCLUDED.value;
                """,
                insert_data,
                template="(%s, %s, %s, %s, %s)",
                page_size=1000
            )
        
        self.conn.commit()
        print(f"✅ Inserted {len(insert_data)} data points from {filename}")
        return len(insert_data)
    
    def process_directory(self, directory: Path) -> None:
        """Process all JSON files in the specified directory."""
        json_files = list(directory.glob('*_global_*.json'))
        
        if not json_files:
            print(f"⚠️  No matching JSON files found in {directory}")
            return
        
        print(f"Found {len(json_files)} JSON files to process")
        
        total_records = 0
        for json_file in json_files:
            records = self.process_json_file(json_file)
            total_records += records
        
        print(f"\n✅ Successfully imported {total_records:,} climate data records")
    
    def validate_import(self):
        """Validate the imported data."""
        with self.conn.cursor() as cur:
            # Get summary statistics
            cur.execute("SELECT COUNT(*) FROM climate_data;")
            total_records = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(DISTINCT region_id) FROM climate_data;")
            total_regions = cur.fetchone()[0]
            
            cur.execute("""
                SELECT m.metric_name, COUNT(*) as count
                FROM climate_data cd
                JOIN metrics m ON cd.metric_id = m.id
                GROUP BY m.metric_name
                ORDER BY count DESC;
            """)
            metrics_summary = cur.fetchall()
            
            cur.execute("""
                SELECT s.scenario_name, COUNT(*) as count
                FROM climate_data cd
                JOIN scenarios s ON cd.scenario_id = s.id
                GROUP BY s.scenario_name
                ORDER BY count DESC;
            """)
            scenarios_summary = cur.fetchall()
            
            cur.execute("SELECT MIN(year), MAX(year) FROM climate_data;")
            year_range = cur.fetchone()
            
            print("\n📊 Data Validation Summary:")
            print(f"   Total records: {total_records:,}")
            print(f"   Total regions: {total_regions}")
            print(f"   Year range: {year_range[0]} - {year_range[1]}")
            
            print("\n   Records by metric:")
            for metric, count in metrics_summary:
                print(f"     {metric}: {count:,}")
            
            print("\n   Records by scenario:")
            for scenario, count in scenarios_summary:
                print(f"     {scenario}: {count:,}")

def main():
    parser = argparse.ArgumentParser(description='Load CMIP6 climate data JSON files into PostGIS')
    parser.add_argument('data_directory', help='Directory containing JSON files')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--port', default=5432, type=int, help='PostgreSQL port')
    parser.add_argument('--database', default='cmip6_atlas', help='Database name')
    parser.add_argument('--user', default='postgres', help='Database user')
    parser.add_argument('--password', default='postgres', help='Database password')
    
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
        
        seeder = ClimateDataSeeder(conn)
        
        # Create tables
        seeder.create_tables()
        
        # Seed reference data
        seeder.seed_reference_data()
        
        # Process JSON files
        data_dir = Path(args.data_directory)
        if not data_dir.exists():
            print(f"❌ Directory {data_dir} does not exist")
            sys.exit(1)
        
        seeder.process_directory(data_dir)
        
        # Validate import
        seeder.validate_import()
        
        print("\n✅ Climate data import completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close() # type: ignore

if __name__ == "__main__":
    main()