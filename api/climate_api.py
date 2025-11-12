#!/usr/bin/env python3
"""
FastAPI application for serving CMIP6 climate data.
This API provides endpoints to query climate data by metric, scenario, and year,
returning data that can be joined client-side with the geometry tiles.
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime

# Configuration
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'cmip6-atlas-db'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'cmip6_atlas'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

# Pydantic models
class MetricInfo(BaseModel):
    id: int
    metric_code: str
    metric_name: str
    unit: str | None
    description: str | None

class ScenarioInfo(BaseModel):
    id: int
    scenario_code: str
    scenario_name: str
    description: str | None

class ClimateDataPoint(BaseModel):
    region_id: int
    value: float
    year: int | None = None

class ClimateDataResponse(BaseModel):
    metric: MetricInfo
    scenario: ScenarioInfo
    year: int
    data: list[ClimateDataPoint]
    summary: dict[str, float]

class TimeSeriesPoint(BaseModel):
    year: int
    value: float

class TimeSeriesResponse(BaseModel):
    region_id: int
    region_identifier: str | None
    metric: MetricInfo
    scenario: ScenarioInfo
    data: list[TimeSeriesPoint]

class YearRange(BaseModel):
    min_year: int
    max_year: int

# Database connection management
@contextmanager
def get_db_connection():
    """Create a database connection context manager."""
    conn = psycopg2.connect(**DATABASE_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

# Cache for reference data
class ReferenceDataCache:
    def __init__(self):
        self.metrics = {}
        self.scenarios = {}
        self.last_refresh = None
        
    def refresh(self):
        """Refresh the cache from database."""
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Load metrics
                cur.execute("SELECT * FROM metrics ORDER BY metric_code;")
                self.metrics = {row['metric_code']: dict(row) for row in cur.fetchall()}
                
                # Load scenarios
                cur.execute("SELECT * FROM scenarios ORDER BY scenario_code;")
                self.scenarios = {row['scenario_code']: dict(row) for row in cur.fetchall()}
                
                self.last_refresh = datetime.now()

# Initialize cache
cache = ReferenceDataCache()

# Lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize cache on startup and cleanup on shutdown."""
    # Startup
    cache.refresh()
    print("✅ Reference data cache initialized")
    yield
    # Shutdown (if needed in the future)

# Initialize FastAPI app
app = FastAPI(
    title="CMIP6 Atlas Climate Data API",
    description="API for retrieving climate projection data from CMIP6 models",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Unit conversion functions
def celsius_to_fahrenheit(celsius: float) -> float:
    """Convert Celsius to Fahrenheit."""
    return (float(celsius) * 9/5) + 32

def mm_to_inches(mm: float) -> float:
    """Convert millimeters to inches."""
    return float(mm) / 25.4

def convert_to_american_units(value: float, unit: str | None) -> tuple[float, str | None]:
    """
    Convert a value to American units if applicable.
    Returns tuple of (converted_value, new_unit).
    """
    if not unit:
        return value, unit

    unit_lower = unit.lower()

    # Temperature conversions
    if 'celsius' in unit_lower or unit_lower == '°c' or unit_lower == 'c':
        return celsius_to_fahrenheit(value), unit.replace('Celsius', 'Fahrenheit').replace('°C', '°F').replace('C', 'F')

    # Precipitation/length conversions
    if unit_lower == 'mm' or 'millimeter' in unit_lower:
        return mm_to_inches(value), unit.replace('mm', 'inches').replace('millimeter', 'inch')

    # No conversion needed
    return value, unit

# API Endpoints

@app.get("/", tags=["Health"])
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "CMIP6 Climate Data API",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/metrics", response_model=list[MetricInfo], tags=["Reference Data"])
async def get_metrics():
    """Get all available climate metrics."""
    if not cache.metrics:
        cache.refresh()
    return list(cache.metrics.values())

@app.get("/scenarios", response_model=list[ScenarioInfo], tags=["Reference Data"])
async def get_scenarios():
    """Get all available climate scenarios."""
    if not cache.scenarios:
        cache.refresh()
    return list(cache.scenarios.values())

@app.get("/years/{metric_code}/{scenario_code}", response_model=YearRange, tags=["Reference Data"])
async def get_available_years(metric_code: str, scenario_code: str):
    """Get the range of years available for a specific metric and scenario."""
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT MIN(cd.year) as min_year, MAX(cd.year) as max_year
                FROM climate_data cd
                JOIN metrics m ON cd.metric_id = m.id
                JOIN scenarios s ON cd.scenario_id = s.id
                WHERE m.metric_code = %s AND s.scenario_code = %s;
            """, (metric_code, scenario_code))
            
            result = cur.fetchone()
            if not result or result['min_year'] is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for metric '{metric_code}' and scenario '{scenario_code}'"
                )
            
            return YearRange(min_year=result['min_year'], max_year=result['max_year'])

@app.get("/climate/{metric_code}/{scenario_code}/{year}",
         response_model=ClimateDataResponse,
         tags=["Climate Data"])
async def get_climate_data(
    metric_code: str,
    scenario_code: str,
    year: int,
    region_ids: list[int] | None = Query(None, description="Filter by specific region IDs"),
    include_summary: bool = Query(True, description="Include statistical summary"),
    american: bool = Query(False, description="Convert values to American units (Fahrenheit, inches)")
):
    """
    Get climate data for a specific metric, scenario, and year.

    This endpoint returns data for all regions (or filtered regions) that can be
    joined client-side with the geometry tiles for visualization.
    """
    if metric_code not in cache.metrics:
        raise HTTPException(status_code=404, detail=f"Metric '{metric_code}' not found")
    
    if scenario_code not in cache.scenarios:
        raise HTTPException(status_code=404, detail=f"Scenario '{scenario_code}' not found")
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Build query
            query = """
                SELECT cd.region_id, cd.value
                FROM climate_data cd
                JOIN metrics m ON cd.metric_id = m.id
                JOIN scenarios s ON cd.scenario_id = s.id
                WHERE m.metric_code = %s 
                    AND s.scenario_code = %s 
                    AND cd.year = %s
            """
            params = [metric_code, scenario_code, year]
            
            # Add region filter if provided
            if region_ids:
                query += " AND cd.region_id = ANY(%s)"
                params.append(region_ids)
            
            query += " ORDER BY cd.region_id;"
            
            cur.execute(query, params)
            results = cur.fetchall()
            
            if not results:
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for {metric_code}/{scenario_code}/{year}"
                )

            # Get metric info and determine unit conversion
            metric_info = MetricInfo(**cache.metrics[metric_code])
            converted_unit = metric_info.unit

            # Prepare response with unit conversion if requested
            data_points = []
            for row in results:
                value = row['value']
                if american:
                    value, converted_unit = convert_to_american_units(value, metric_info.unit)
                data_points.append(ClimateDataPoint(region_id=row['region_id'], value=value))

            # Update metric info with converted unit
            if american:
                metric_info.unit = converted_unit

            # Calculate summary statistics
            summary = {}
            if include_summary and data_points:
                values = [dp.value for dp in data_points]
                summary = {
                    "min": min(values),
                    "max": max(values),
                    "mean": sum(values) / len(values),
                    "count": len(values)
                }

            return ClimateDataResponse(
                metric=metric_info,
                scenario=ScenarioInfo(**cache.scenarios[scenario_code]),
                year=year,
                data=data_points,
                summary=summary
            )

@app.get("/climate/bulk/{year}",
         tags=["Climate Data"])
async def get_bulk_climate_data(
    year: int,
    metrics: list[str] = Query(..., description="List of metric codes"),
    scenarios: list[str] = Query(..., description="List of scenario codes"),
    region_ids: list[int] | None = Query(None, description="Filter by specific region IDs")
):
    """
    Get climate data for multiple metric/scenario combinations for a single year.
    Useful for creating comparison views or dashboards.
    """
    results = {}
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for metric_code in metrics:
                if metric_code not in cache.metrics:
                    continue
                    
                for scenario_code in scenarios:
                    if scenario_code not in cache.scenarios:
                        continue
                    
                    query = """
                        SELECT cd.region_id, cd.value
                        FROM climate_data cd
                        JOIN metrics m ON cd.metric_id = m.id
                        JOIN scenarios s ON cd.scenario_id = s.id
                        WHERE m.metric_code = %s 
                            AND s.scenario_code = %s 
                            AND cd.year = %s
                    """
                    params = [metric_code, scenario_code, year]
                    
                    if region_ids:
                        query += " AND cd.region_id = ANY(%s)"
                        params.append(region_ids)
                    
                    cur.execute(query, params)
                    data = cur.fetchall()
                    
                    if data:
                        key = f"{metric_code}_{scenario_code}"
                        results[key] = {
                            "metric": cache.metrics[metric_code],
                            "scenario": cache.scenarios[scenario_code],
                            "year": year,
                            "data": {row['region_id']: row['value'] for row in data}
                        }
    
    return results

@app.get("/timeseries/{metric_code}/{scenario_code}/{region_id}",
         response_model=TimeSeriesResponse,
         tags=["Time Series"])
async def get_timeseries(
    metric_code: str,
    scenario_code: str,
    region_id: int,
    start_year: int | None = Query(None, description="Start year for time range"),
    end_year: int | None = Query(None, description="End year for time range"),
    american: bool = Query(False, description="Convert values to American units (Fahrenheit, inches)")
):
    """
    Get time series data for a specific region, metric, and scenario.
    Useful for displaying temporal trends in charts.
    """
    if metric_code not in cache.metrics:
        raise HTTPException(status_code=404, detail=f"Metric '{metric_code}' not found")
    
    if scenario_code not in cache.scenarios:
        raise HTTPException(status_code=404, detail=f"Scenario '{scenario_code}' not found")
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get region identifier
            cur.execute("SELECT region_identifier FROM regions WHERE region_id = %s;", (region_id,))
            region_result = cur.fetchone()
            region_identifier = region_result['region_identifier'] if region_result else None
            
            # Build time series query
            query = """
                SELECT cd.year, cd.value
                FROM climate_data cd
                JOIN metrics m ON cd.metric_id = m.id
                JOIN scenarios s ON cd.scenario_id = s.id
                WHERE m.metric_code = %s 
                    AND s.scenario_code = %s 
                    AND cd.region_id = %s
            """
            params = [metric_code, scenario_code, region_id]
            
            if start_year:
                query += " AND cd.year >= %s"
                params.append(start_year)
            
            if end_year:
                query += " AND cd.year <= %s"
                params.append(end_year)
            
            query += " ORDER BY cd.year;"
            
            cur.execute(query, params)
            results = cur.fetchall()

            if not results:
                raise HTTPException(
                    status_code=404,
                    detail=f"No time series data found for region {region_id}"
                )

            # Get metric info and determine unit conversion
            metric_info = MetricInfo(**cache.metrics[metric_code])
            converted_unit = metric_info.unit

            # Prepare time series data with unit conversion if requested
            time_series_data = []
            for row in results:
                value = row['value']
                if american:
                    value, converted_unit = convert_to_american_units(value, metric_info.unit)
                time_series_data.append(TimeSeriesPoint(year=row['year'], value=value))

            # Update metric info with converted unit
            if american:
                metric_info.unit = converted_unit

            return TimeSeriesResponse(
                region_id=region_id,
                region_identifier=region_identifier,
                metric=metric_info,
                scenario=ScenarioInfo(**cache.scenarios[scenario_code]),
                data=time_series_data
            )

@app.get("/regions/{region_id}/all",
         tags=["Region Data"])
async def get_all_region_data(
    region_id: int,
    year: int | None = Query(None, description="Filter by specific year")
):
    """
    Get all available climate data for a specific region.
    Useful for region-specific dashboards or detailed views.
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get region info
            cur.execute("""
                SELECT region_id, region_identifier, source_country_name, 
                       source_admin_level, name_1, name_2
                FROM regions 
                WHERE region_id = %s;
            """, (region_id,))
            
            region_info = cur.fetchone()
            if not region_info:
                raise HTTPException(status_code=404, detail=f"Region {region_id} not found")
            
            # Get all climate data for this region
            query = """
                SELECT 
                    m.metric_code,
                    m.metric_name,
                    m.unit,
                    s.scenario_code,
                    s.scenario_name,
                    cd.year,
                    cd.value
                FROM climate_data cd
                JOIN metrics m ON cd.metric_id = m.id
                JOIN scenarios s ON cd.scenario_id = s.id
                WHERE cd.region_id = %s
            """
            params = [region_id]
            
            if year:
                query += " AND cd.year = %s"
                params.append(year)
            
            query += " ORDER BY m.metric_code, s.scenario_code, cd.year;"
            
            cur.execute(query, params)
            climate_data = cur.fetchall()
            
            # Organize data by metric and scenario
            organized_data = {}
            for row in climate_data:
                metric_key = row['metric_code']
                scenario_key = row['scenario_code']
                
                if metric_key not in organized_data:
                    organized_data[metric_key] = {
                        "metric_name": row['metric_name'],
                        "unit": row['unit'],
                        "scenarios": {}
                    }
                
                if scenario_key not in organized_data[metric_key]["scenarios"]:
                    organized_data[metric_key]["scenarios"][scenario_key] = {
                        "scenario_name": row['scenario_name'],
                        "data": []
                    }
                
                organized_data[metric_key]["scenarios"][scenario_key]["data"].append({
                    "year": row['year'],
                    "value": row['value']
                })
            
            return {
                "region": region_info,
                "climate_data": organized_data
            }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)