#!/usr/bin/env python3
"""
FastAPI application for serving CMIP6 climate data.
This API provides endpoints to query climate data by metric, scenario, and year,
returning data that can be joined client-side with the geometry tiles.
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2 # type: ignore [import-untyped]
from psycopg2.extras import RealDictCursor # type: ignore [import-untyped]
import os
import asyncio
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

class MultiYearAverageResponse(BaseModel):
    region_id: int
    metric: MetricInfo
    scenario: ScenarioInfo
    start_year: int
    end_year: int
    average_value: float
    data_points_count: int
    cached: bool
    computed_at: str

class MultiYearAverageDataPoint(BaseModel):
    region_id: int
    average_value: float
    data_points_count: int

class MultiYearAverageAllRegionsResponse(BaseModel):
    metric: MetricInfo
    scenario: ScenarioInfo
    start_year: int
    end_year: int
    data: list[MultiYearAverageDataPoint]
    summary: dict[str, float]
    cached_count: int
    computed_count: int

class RegionCenterResponse(BaseModel):
    region_id: int
    longitude: float
    latitude: float

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
    # Startup - retry database connection with exponential backoff
    max_retries = 10
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            print(f"🔄 Attempting to initialize cache (attempt {attempt + 1}/{max_retries})...")
            cache.refresh()
            print("✅ Reference data cache initialized")
            break
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Database not ready: {e}. Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay = int(min(retry_delay * 1.5, 30))  # Exponential backoff, max 30s
            else:
                print(f"❌ Failed to connect to database after {max_retries} attempts")
                raise

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

# Helper functions for multi-year averaging
def get_metric_and_scenario_ids(metric_code: str, scenario_code: str) -> tuple[int, int]:
    """
    Look up metric_id and scenario_id from their codes.
    Raises HTTPException if not found.
    """
    if metric_code not in cache.metrics:
        raise HTTPException(status_code=404, detail=f"Metric '{metric_code}' not found")

    if scenario_code not in cache.scenarios:
        raise HTTPException(status_code=404, detail=f"Scenario '{scenario_code}' not found")

    metric_id = cache.metrics[metric_code]['id']
    scenario_id = cache.scenarios[scenario_code]['id']

    return metric_id, scenario_id

def get_cached_average(cur, region_id: int, metric_id: int, scenario_id: int,
                       start_year: int, end_year: int) -> dict | None:
    """
    Check if a cached average exists in the climate_averages table.
    Returns the cached record or None if not found.
    """
    cur.execute("""
        SELECT
            avg_value,
            data_points_count,
            computed_at
        FROM climate_averages
        WHERE region_id = %s
            AND metric_id = %s
            AND scenario_id = %s
            AND start_year = %s
            AND end_year = %s;
    """, (region_id, metric_id, scenario_id, start_year, end_year))

    result = cur.fetchone()
    return dict(result) if result else None

def compute_average(cur, region_id: int, metric_id: int, scenario_id: int,
                   start_year: int, end_year: int) -> tuple[float, int] | None:
    """
    Compute the average value from the climate_data table.
    Returns (avg_value, data_points_count) or None if no data exists.
    """
    cur.execute("""
        WITH yearly_data AS (
            SELECT
                cd.year,
                cd.value
            FROM climate_data cd
            WHERE cd.region_id = %s
                AND cd.metric_id = %s
                AND cd.scenario_id = %s
                AND cd.year BETWEEN %s AND %s
                AND cd.value IS NOT NULL
        )
        SELECT
            AVG(value) as avg_value,
            COUNT(*) as data_points_count
        FROM yearly_data;
    """, (region_id, metric_id, scenario_id, start_year, end_year))

    result = cur.fetchone()

    if result and result['avg_value'] is not None and result['data_points_count'] > 0:
        return float(result['avg_value']), int(result['data_points_count'])

    return None

def store_computed_average(cur, conn, region_id: int, metric_id: int, scenario_id: int,
                          start_year: int, end_year: int, avg_value: float,
                          data_points_count: int):
    """
    Store the computed average in the climate_averages table.
    """
    cur.execute("""
        INSERT INTO climate_averages
            (region_id, metric_id, scenario_id, start_year, end_year,
             avg_value, data_points_count, computed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (region_id, metric_id, scenario_id, start_year, end_year)
        DO UPDATE SET
            avg_value = EXCLUDED.avg_value,
            data_points_count = EXCLUDED.data_points_count,
            computed_at = CURRENT_TIMESTAMP;
    """, (region_id, metric_id, scenario_id, start_year, end_year,
          avg_value, data_points_count))
    conn.commit()

def get_all_cached_averages(cur, metric_id: int, scenario_id: int,
                            start_year: int, end_year: int) -> dict[int, dict]:
    """
    Get all cached averages for all regions for the specified parameters.
    Returns a dictionary mapping region_id to cached data.
    """
    cur.execute("""
        SELECT
            region_id,
            avg_value,
            data_points_count,
            computed_at
        FROM climate_averages
        WHERE metric_id = %s
            AND scenario_id = %s
            AND start_year = %s
            AND end_year = %s;
    """, (metric_id, scenario_id, start_year, end_year))

    results = cur.fetchall()
    return {row['region_id']: dict(row) for row in results}

def compute_all_averages(cur, metric_id: int, scenario_id: int,
                        start_year: int, end_year: int) -> dict[int, tuple[float, int]]:
    """
    Compute averages for all regions in a single query.
    Returns a dictionary mapping region_id to (avg_value, data_points_count).
    """
    cur.execute("""
        WITH yearly_data AS (
            SELECT
                cd.region_id,
                cd.year,
                cd.value
            FROM climate_data cd
            WHERE cd.metric_id = %s
                AND cd.scenario_id = %s
                AND cd.year BETWEEN %s AND %s
                AND cd.value IS NOT NULL
        )
        SELECT
            region_id,
            AVG(value) as avg_value,
            COUNT(*) as data_points_count
        FROM yearly_data
        GROUP BY region_id
        HAVING COUNT(*) > 0;
    """, (metric_id, scenario_id, start_year, end_year))

    results = cur.fetchall()
    return {
        row['region_id']: (float(row['avg_value']), int(row['data_points_count']))
        for row in results
    }

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

@app.get("/climate/average/{metric_code}/{scenario_code}/{region_id}",
         response_model=MultiYearAverageResponse,
         tags=["Climate Data"])
async def get_multi_year_average(
    metric_code: str,
    scenario_code: str,
    region_id: int,
    start_year: int = Query(..., ge=1991, le=2100, description="Start year of the range (1991-2100)"),
    end_year: int = Query(..., ge=1991, le=2100, description="End year of the range (1991-2100)"),
    force_recompute: bool = Query(False, description="Force recomputation even if cached value exists"),
    american: bool = Query(False, description="Convert values to American units (Fahrenheit, inches)")
):
    """
    Get multi-year average climate data for a specific region, metric, and scenario.

    This endpoint computes and caches the average value over a specified year range.
    Cached values are returned by default for improved performance. Use force_recompute
    to recalculate the average from raw data.

    Parameters:
    - metric_code: Climate metric identifier (e.g., 'tas', 'pr')
    - scenario_code: Climate scenario identifier (e.g., 'ssp245', 'ssp585')
    - region_id: Numeric identifier for the region
    - start_year: Beginning year of the averaging period (1991-2100)
    - end_year: End year of the averaging period (1991-2100)
    - force_recompute: Force recalculation even if a cached value exists
    - american: Convert values to American units (Fahrenheit, inches)

    Returns:
    - region_id: The region identifier
    - metric: Full metric information including unit
    - scenario: Full scenario information
    - start_year: Start of the year range
    - end_year: End of the year range
    - average_value: The computed average value
    - data_points_count: Number of data points used in the calculation
    - cached: Whether this result was retrieved from cache
    - computed_at: Timestamp when the average was computed
    """
    # Validate year range
    if start_year > end_year:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid year range: start_year ({start_year}) must be <= end_year ({end_year})"
        )

    # Look up metric_id and scenario_id
    metric_id, scenario_id = get_metric_and_scenario_ids(metric_code, scenario_code)

    # Get metric info and determine unit conversion
    metric_info = MetricInfo(**cache.metrics[metric_code])
    converted_unit = metric_info.unit

    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cached_result = None
            is_cached = False

            # Check for cached value if not forcing recompute
            if not force_recompute:
                cached_result = get_cached_average(
                    cur, region_id, metric_id, scenario_id, start_year, end_year
                )

            if cached_result:
                # Return cached value
                is_cached = True
                avg_value = float(cached_result['avg_value'])
                data_points_count = int(cached_result['data_points_count'])
                computed_at = cached_result['computed_at'].isoformat()
            else:
                # Compute new average
                result = compute_average(
                    cur, region_id, metric_id, scenario_id, start_year, end_year
                )

                if result is None:
                    raise HTTPException(
                        status_code=404,
                        detail=f"No data found for region {region_id}, metric '{metric_code}', "
                               f"scenario '{scenario_code}' in year range {start_year}-{end_year}"
                    )

                avg_value, data_points_count = result

                # Store the computed average
                store_computed_average(
                    cur, conn, region_id, metric_id, scenario_id,
                    start_year, end_year, avg_value, data_points_count
                )

                # Get the timestamp of the newly stored record
                cur.execute("""
                    SELECT computed_at
                    FROM climate_averages
                    WHERE region_id = %s
                        AND metric_id = %s
                        AND scenario_id = %s
                        AND start_year = %s
                        AND end_year = %s;
                """, (region_id, metric_id, scenario_id, start_year, end_year))

                timestamp_result = cur.fetchone()
                computed_at = timestamp_result['computed_at'].isoformat()
                is_cached = False

            # Apply unit conversion if requested
            if american:
                avg_value, converted_unit = convert_to_american_units(avg_value, metric_info.unit)
                metric_info.unit = converted_unit

            return MultiYearAverageResponse(
                region_id=region_id,
                metric=metric_info,
                scenario=ScenarioInfo(**cache.scenarios[scenario_code]),
                start_year=start_year,
                end_year=end_year,
                average_value=avg_value,
                data_points_count=data_points_count,
                cached=is_cached,
                computed_at=computed_at
            )

@app.get("/average-all/{metric_code}/{scenario_code}",
         response_model=MultiYearAverageAllRegionsResponse,
         tags=["Climate Data"])
async def get_multi_year_average_all_regions(
    metric_code: str,
    scenario_code: str,
    start_year: int = Query(..., ge=1991, le=2100, description="Start year of the range (1991-2100)"),
    end_year: int = Query(..., ge=1991, le=2100, description="End year of the range (1991-2100)"),
    region_ids: list[int] | None = Query(None, description="Filter by specific region IDs"),
    force_recompute: bool = Query(False, description="Force recomputation even if cached values exist"),
    include_summary: bool = Query(True, description="Include statistical summary"),
    american: bool = Query(False, description="Convert values to American units (Fahrenheit, inches)")
):
    """
    Get multi-year average climate data for all regions (or filtered regions).

    This endpoint computes and caches average values over a specified year range for
    all regions, similar to how the /climate endpoint provides single-year data for all regions.
    Cached values are returned by default for improved performance.

    Parameters:
    - metric_code: Climate metric identifier (e.g., 'tas', 'pr')
    - scenario_code: Climate scenario identifier (e.g., 'ssp245', 'ssp585')
    - start_year: Beginning year of the averaging period (1991-2100)
    - end_year: End year of the averaging period (1991-2100)
    - region_ids: Optional list of region IDs to filter results
    - force_recompute: Force recalculation even if cached values exist
    - include_summary: Include statistical summary of the averages
    - american: Convert values to American units (Fahrenheit, inches)

    Returns:
    - metric: Full metric information including unit
    - scenario: Full scenario information
    - start_year: Start of the year range
    - end_year: End of the year range
    - data: List of average values per region
    - summary: Statistical summary (min, max, mean, count)
    - cached_count: Number of results retrieved from cache
    - computed_count: Number of results computed on-the-fly
    """
    # Validate year range
    if start_year > end_year:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid year range: start_year ({start_year}) must be <= end_year ({end_year})"
        )

    # Look up metric_id and scenario_id
    metric_id, scenario_id = get_metric_and_scenario_ids(metric_code, scenario_code)

    # Get metric info and determine unit conversion
    metric_info = MetricInfo(**cache.metrics[metric_code])
    converted_unit = metric_info.unit

    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cached_data = {}
            cached_count = 0
            computed_count = 0

            # Get cached averages if not forcing recompute
            if not force_recompute:
                cached_data = get_all_cached_averages(
                    cur, metric_id, scenario_id, start_year, end_year
                )

            # Compute averages for all regions
            computed_data = compute_all_averages(
                cur, metric_id, scenario_id, start_year, end_year
            )

            if not computed_data:
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for metric '{metric_code}', scenario '{scenario_code}' "
                           f"in year range {start_year}-{end_year}"
                )

            # Merge cached and computed data
            data_points = []
            regions_to_cache = []

            for region_id, (avg_value, data_points_count) in computed_data.items():
                # Filter by region_ids if provided
                if region_ids is not None and region_id not in region_ids:
                    continue

                # Check if we should use cached value
                if not force_recompute and region_id in cached_data:
                    cached_count += 1
                    avg_value = float(cached_data[region_id]['avg_value'])
                    data_points_count = int(cached_data[region_id]['data_points_count'])
                else:
                    computed_count += 1
                    # Mark this region for caching
                    regions_to_cache.append((region_id, avg_value, data_points_count))

                # Apply unit conversion if requested
                display_value = avg_value
                if american:
                    display_value, converted_unit = convert_to_american_units(avg_value, metric_info.unit)

                data_points.append(MultiYearAverageDataPoint(
                    region_id=region_id,
                    average_value=display_value,
                    data_points_count=data_points_count
                ))

            # Store newly computed averages in cache
            for region_id, avg_value, data_points_count in regions_to_cache:
                store_computed_average(
                    cur, conn, region_id, metric_id, scenario_id,
                    start_year, end_year, avg_value, data_points_count
                )

            if not data_points:
                raise HTTPException(
                    status_code=404,
                    detail="No data found for the specified region filter"
                )

            # Calculate summary statistics
            summary = {}
            if include_summary:
                values = [dp.average_value for dp in data_points]
                summary = {
                    "min": min(values),
                    "max": max(values),
                    "mean": sum(values) / len(values),
                    "count": len(values)
                }

            # Update metric info with converted unit
            if american:
                metric_info.unit = converted_unit

            return MultiYearAverageAllRegionsResponse(
                metric=metric_info,
                scenario=ScenarioInfo(**cache.scenarios[scenario_code]),
                start_year=start_year,
                end_year=end_year,
                data=data_points,
                summary=summary,
                cached_count=cached_count,
                computed_count=computed_count
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

@app.get("/regions/{region_id}/center",
         response_model=RegionCenterResponse,
         tags=["Region Data"])
async def get_region_center(region_id: int):
    """
    Get the center coordinate (centroid) of a region by its region_id.

    This endpoint retrieves the geometry from the regions table and calculates
    the centroid using PostGIS ST_Centroid function. The result is returned
    as longitude and latitude coordinates in WGS84 (EPSG:4326).

    Parameters:
    - region_id: Numeric identifier for the region

    Returns:
    - region_id: The region identifier
    - longitude: X coordinate of the centroid
    - latitude: Y coordinate of the centroid
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Query to get the centroid of the region geometry
            cur.execute("""
                SELECT
                    region_id,
                    ST_X(ST_Centroid(geom)) as longitude,
                    ST_Y(ST_Centroid(geom)) as latitude
                FROM regions
                WHERE region_id = %s;
            """, (region_id,))

            result = cur.fetchone()

            if not result:
                raise HTTPException(
                    status_code=404,
                    detail=f"Region with region_id {region_id} not found"
                )

            return RegionCenterResponse(
                region_id=result['region_id'],
                longitude=result['longitude'],
                latitude=result['latitude']
            )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)