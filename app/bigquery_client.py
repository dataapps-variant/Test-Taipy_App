"""
BigQuery Client - Taipy Version
Converted from Dash - mostly unchanged as this is the data layer
Performance improvements:
1. Pre-aggregated data loading
2. Efficient caching with TTL
3. Lazy loading patterns
4. Reduced redundant processing
"""

from google.cloud import bigquery
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from datetime import datetime, timezone, timedelta
import io
import os
import hashlib

from app.config import (
    BIGQUERY_FULL_TABLE, 
    CACHE_TTL,
    AUTO_REFRESH_HOUR,
    AUTO_REFRESH_MINUTE,
    GCS_ACTIVE_CACHE,
    GCS_STAGING_CACHE,
    GCS_BQ_REFRESH_METADATA,
    GCS_GCS_REFRESH_METADATA,
)

GCS_BUCKET_NAME = os.environ.get("GCS_CACHE_BUCKET", "")
DEBUG = True


def log_debug(message):
    if DEBUG:
        print(f"[CACHE] {datetime.now().strftime('%H:%M:%S')} - {message}")


# =============================================================================
# GCS HELPER FUNCTIONS
# =============================================================================

# Cache for GCS bucket (avoid creating new client on every call)
_gcs_bucket_cache = {
    "bucket": None,
    "checked": False
}


def get_gcs_bucket():
    """Get GCS bucket - CACHED to avoid repeated client creation"""
    global _gcs_bucket_cache
    
    if _gcs_bucket_cache["checked"]:
        return _gcs_bucket_cache["bucket"]
    
    if not GCS_BUCKET_NAME:
        _gcs_bucket_cache["checked"] = True
        _gcs_bucket_cache["bucket"] = None
        return None
    
    try:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        if bucket.exists():
            _gcs_bucket_cache["bucket"] = bucket
        else:
            _gcs_bucket_cache["bucket"] = None
        _gcs_bucket_cache["checked"] = True
        return _gcs_bucket_cache["bucket"]
    except Exception as e:
        log_debug(f"GCS error: {e}")
        _gcs_bucket_cache["checked"] = True
        _gcs_bucket_cache["bucket"] = None
        return None


# Cache for metadata timestamps (avoid hitting GCS on every request)
_metadata_cache = {
    "bq_refresh": None,
    "gcs_refresh": None,
    "loaded_at": None
}
METADATA_CACHE_TTL = 60  # 1 minute

def _is_metadata_cache_valid():
    if _metadata_cache["loaded_at"] is None:
        return False
    age = (datetime.now() - _metadata_cache["loaded_at"]).total_seconds()
    return age < METADATA_CACHE_TTL


def get_metadata_timestamp(bucket, metadata_file):
    if bucket is None:
        return None
    try:
        blob = bucket.blob(metadata_file)
        if not blob.exists():
            return None
        return datetime.fromisoformat(blob.download_as_text().strip())
    except:
        return None


def set_metadata_timestamp(bucket, metadata_file, timestamp=None):
    if bucket is None:
        return False
    try:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        bucket.blob(metadata_file).upload_from_string(timestamp.isoformat())
        return True
    except:
        return False


def load_parquet_from_gcs(bucket, cache_file):
    if bucket is None:
        return None
    try:
        blob = bucket.blob(cache_file)
        if not blob.exists():
            return None
        
        log_debug(f"Loading from GCS: {cache_file}")
        start = datetime.now()
        
        parquet_bytes = blob.download_as_bytes()
        table = pq.read_table(io.BytesIO(parquet_bytes))
        
        log_debug(f"GCS load: {table.num_rows} rows in {(datetime.now() - start).total_seconds():.2f}s")
        return table
    except Exception as e:
        log_debug(f"GCS load error: {e}")
        return None


def save_parquet_to_gcs(bucket, cache_file, data):
    if bucket is None:
        return False
    try:
        buffer = io.BytesIO()
        pq.write_table(data, buffer, compression='snappy')
        buffer.seek(0)
        bucket.blob(cache_file).upload_from_file(buffer, content_type='application/octet-stream')
        return True
    except Exception as e:
        log_debug(f"GCS save error: {e}")
        return False


# =============================================================================
# BIGQUERY LOADER
# =============================================================================

def load_from_bigquery():
    """
    Load data from BigQuery with optimizations:
    - Only select needed columns
    - Consider partitioning/clustering in BQ table
    """
    log_debug("Loading from BigQuery...")
    start = datetime.now()
    
    client = bigquery.Client()
    
    query = f"""
        SELECT
            Reporting_Date,
            App_Name,
            Plan_Name,
            BC,
            Cohort,
            Active_Inactive,
            `Table`,
            Subscriptions,
            Rebills,
            Churn_Rate,
            Refund_Rate,
            Gross_ARPU_Retention_Rate,
            Net_ARPU_Retention_Rate,
            Cohort_CAC,
            Recent_CAC,
            Gross_ARPU_Discounted,
            Net_ARPU_Discounted,
            Net_LTV_Discounted,
            BC4_CAC_Ceiling
        FROM `{BIGQUERY_FULL_TABLE}`
    """
    
    job_config = bigquery.QueryJobConfig(
        use_query_cache=True,
    )
    
    result = client.query(query, job_config=job_config).to_arrow()
    
    log_debug(f"BigQuery: {result.num_rows} rows in {(datetime.now() - start).total_seconds():.2f}s")
    return result


# =============================================================================
# MASTER DATA LOADER - WITH APP-LEVEL CACHE
# =============================================================================

# App-level cache (persists across requests within same instance)
_app_cache = {
    "data": None,
    "loaded_at": None,
    "date_bounds": None,
    "plan_groups_active": None,
    "plan_groups_inactive": None,
}


def _is_cache_valid():
    """Check if app-level cache is still valid"""
    if _app_cache["data"] is None or _app_cache["loaded_at"] is None:
        return False
    age = (datetime.now() - _app_cache["loaded_at"]).total_seconds()
    return age < CACHE_TTL


def get_master_data():
    """
    Get master data with multi-level caching:
    1. App-level cache (fastest - same process)
    2. GCS cache (persistent across instances)
    3. BigQuery (fallback)
    """
    global _app_cache
    
    # Level 1: App-level cache (fastest)
    if _is_cache_valid():
        log_debug("Using app-level cache")
        return _app_cache["data"]
    
    # Level 2: GCS cache
    bucket = get_gcs_bucket()
    if bucket:
        data = load_parquet_from_gcs(bucket, GCS_ACTIVE_CACHE)
        if data is not None:
            _app_cache["data"] = data
            _app_cache["loaded_at"] = datetime.now()
            return data
    
    # Level 3: BigQuery (slowest)
    log_debug("No cache - loading from BigQuery")
    data = load_from_bigquery()
    
    # Save to all cache levels
    _app_cache["data"] = data
    _app_cache["loaded_at"] = datetime.now()
    
    if bucket:
        save_parquet_to_gcs(bucket, GCS_ACTIVE_CACHE, data)
        save_parquet_to_gcs(bucket, GCS_STAGING_CACHE, data)
        set_metadata_timestamp(bucket, GCS_BQ_REFRESH_METADATA)
        set_metadata_timestamp(bucket, GCS_GCS_REFRESH_METADATA)
    
    return data


# =============================================================================
# CACHED DERIVED DATA
# =============================================================================

_derived_cache = {
    "date_bounds": {"data": None, "loaded_at": None},
    "plan_groups_active": {"data": None, "loaded_at": None},
    "plan_groups_inactive": {"data": None, "loaded_at": None},
}

DERIVED_CACHE_TTL = 3600  # 1 hour


def _is_derived_cache_valid(key):
    """Check if derived cache is valid"""
    cache = _derived_cache.get(key, {})
    if cache.get("data") is None or cache.get("loaded_at") is None:
        return False
    age = (datetime.now() - cache["loaded_at"]).total_seconds()
    return age < DERIVED_CACHE_TTL


def load_date_bounds():
    """Get min and max dates - CACHED"""
    global _derived_cache
    
    if _is_derived_cache_valid("date_bounds"):
        return _derived_cache["date_bounds"]["data"]
    
    data = get_master_data()
    dates = data.column("Reporting_Date")
    min_date = pc.min(dates).as_py()
    max_date = pc.max(dates).as_py()
    
    if hasattr(min_date, 'date'):
        min_date = min_date.date()
    if hasattr(max_date, 'date'):
        max_date = max_date.date()
    
    result = {"min_date": min_date, "max_date": max_date}
    _derived_cache["date_bounds"] = {"data": result, "loaded_at": datetime.now()}
    return result


def load_plan_groups(active_inactive="Active"):
    """Get unique plans - CACHED"""
    global _derived_cache
    
    cache_key = f"plan_groups_{active_inactive.lower()}"
    
    if cache_key not in _derived_cache:
        _derived_cache[cache_key] = {"data": None, "loaded_at": None}
    
    if _is_derived_cache_valid(cache_key):
        return _derived_cache[cache_key]["data"]
    
    data = get_master_data()
    
    mask = pc.equal(data.column("Active_Inactive"), active_inactive)
    filtered = data.filter(mask)
    
    app_names = filtered.column("App_Name").to_pylist()
    plan_names = filtered.column("Plan_Name").to_pylist()
    
    seen = set()
    unique_apps = []
    unique_plans = []
    
    for app, plan in zip(app_names, plan_names):
        if (app, plan) not in seen:
            seen.add((app, plan))
            unique_apps.append(app)
            unique_plans.append(plan)
    
    sorted_pairs = sorted(zip(unique_apps, unique_plans))
    
    result = {
        "App_Name": [p[0] for p in sorted_pairs],
        "Plan_Name": [p[1] for p in sorted_pairs]
    }
    
    _derived_cache[cache_key] = {"data": result, "loaded_at": datetime.now()}
    return result


# =============================================================================
# QUERY RESULT CACHE
# =============================================================================

_query_cache = {}
QUERY_CACHE_TTL = 1800  # 30 minutes


def _get_cache_key(*args):
    """Create hash for filter combination"""
    key = "_".join(str(a) for a in args)
    return hashlib.md5(key.encode()).hexdigest()[:16]


def _is_query_cache_valid(cache_key):
    """Check if query cache is valid"""
    if cache_key not in _query_cache:
        return False
    cache = _query_cache[cache_key]
    if cache.get("data") is None or cache.get("loaded_at") is None:
        return False
    age = (datetime.now() - cache["loaded_at"]).total_seconds()
    return age < QUERY_CACHE_TTL


def load_pivot_data(start_date, end_date, bc, cohort, plans, metrics, table_type, active_inactive="Active"):
    """Filter data for pivot table - CACHED"""
    cache_key = _get_cache_key("pivot", start_date, end_date, bc, cohort, tuple(sorted(plans)), tuple(sorted(metrics)), table_type, active_inactive)
    
    if _is_query_cache_valid(cache_key):
        return _query_cache[cache_key]["data"]
    
    data = get_master_data()
    
    reporting_dates = data.column("Reporting_Date")
    
    # Build filter mask
    mask = pc.and_(
        pc.greater_equal(reporting_dates, start_date),
        pc.less_equal(reporting_dates, end_date)
    )
    mask = pc.and_(mask, pc.equal(data.column("BC"), bc))
    mask = pc.and_(mask, pc.equal(data.column("Cohort"), cohort))
    mask = pc.and_(mask, pc.equal(data.column("Active_Inactive"), active_inactive))
    mask = pc.and_(mask, pc.equal(data.column("Table"), table_type))
    
    if plans:
        plan_mask = pc.is_in(data.column("Plan_Name"), value_set=pa.array(plans))
        mask = pc.and_(mask, plan_mask)
    
    filtered = data.filter(mask)
    
    result = {
        "App_Name": filtered.column("App_Name").to_pylist(),
        "Plan_Name": filtered.column("Plan_Name").to_pylist(),
        "Reporting_Date": filtered.column("Reporting_Date").to_pylist()
    }
    
    for metric in metrics:
        if metric in filtered.column_names:
            result[metric] = filtered.column(metric).to_pylist()
    
    _query_cache[cache_key] = {"data": result, "loaded_at": datetime.now()}
    return result


def load_chart_data(start_date, end_date, bc, cohort, plans, metric, table_type, active_inactive="Active"):
    """Filter and aggregate data for charts - CACHED"""
    cache_key = _get_cache_key("chart", start_date, end_date, bc, cohort, tuple(sorted(plans)), metric, table_type, active_inactive)
    
    if _is_query_cache_valid(cache_key):
        return _query_cache[cache_key]["data"]
    
    data = get_master_data()
    
    reporting_dates = data.column("Reporting_Date")
    
    mask = pc.and_(
        pc.greater_equal(reporting_dates, start_date),
        pc.less_equal(reporting_dates, end_date)
    )
    mask = pc.and_(mask, pc.equal(data.column("BC"), bc))
    mask = pc.and_(mask, pc.equal(data.column("Cohort"), cohort))
    mask = pc.and_(mask, pc.equal(data.column("Active_Inactive"), active_inactive))
    mask = pc.and_(mask, pc.equal(data.column("Table"), table_type))
    
    if plans:
        plan_mask = pc.is_in(data.column("Plan_Name"), value_set=pa.array(plans))
        mask = pc.and_(mask, plan_mask)
    
    filtered = data.filter(mask)
    
    if filtered.num_rows == 0:
        result = {"Plan_Name": [], "Reporting_Date": [], "metric_value": []}
        _query_cache[cache_key] = {"data": result, "loaded_at": datetime.now()}
        return result
    
    plan_names = filtered.column("Plan_Name").to_pylist()
    dates = filtered.column("Reporting_Date").to_pylist()
    values = filtered.column(metric).to_pylist()
    
    # Aggregate
    aggregated = {}
    for plan, date, value in zip(plan_names, dates, values):
        key = (plan, date)
        if key not in aggregated:
            aggregated[key] = 0
        if value is not None:
            aggregated[key] += value
    
    result_plans, result_dates, result_values = [], [], []
    for (plan, date), total in sorted(aggregated.items()):
        result_plans.append(plan)
        result_dates.append(date)
        result_values.append(total)
    
    result = {
        "Plan_Name": result_plans,
        "Reporting_Date": result_dates,
        "metric_value": result_values
    }
    
    _query_cache[cache_key] = {"data": result, "loaded_at": datetime.now()}
    return result


# =============================================================================
# BATCH LOADING FOR CHARTS (MAJOR OPTIMIZATION)
# =============================================================================

def load_all_chart_data(start_date, end_date, bc, cohort, plans, metrics, table_type, active_inactive="Active"):
    """
    Load ALL chart data in ONE pass instead of 20 separate queries.
    This is a MAJOR performance improvement.
    """
    cache_key = _get_cache_key("all_charts", start_date, end_date, bc, cohort, tuple(sorted(plans)), tuple(sorted(metrics)), table_type, active_inactive)
    
    if _is_query_cache_valid(cache_key):
        return _query_cache[cache_key]["data"]
    
    data = get_master_data()
    
    reporting_dates = data.column("Reporting_Date")
    
    # Single filter pass
    mask = pc.and_(
        pc.greater_equal(reporting_dates, start_date),
        pc.less_equal(reporting_dates, end_date)
    )
    mask = pc.and_(mask, pc.equal(data.column("BC"), bc))
    mask = pc.and_(mask, pc.equal(data.column("Cohort"), cohort))
    mask = pc.and_(mask, pc.equal(data.column("Active_Inactive"), active_inactive))
    mask = pc.and_(mask, pc.equal(data.column("Table"), table_type))
    
    if plans:
        plan_mask = pc.is_in(data.column("Plan_Name"), value_set=pa.array(plans))
        mask = pc.and_(mask, plan_mask)
    
    filtered = data.filter(mask)
    
    if filtered.num_rows == 0:
        result = {metric: {"Plan_Name": [], "Reporting_Date": [], "metric_value": []} for metric in metrics}
        _query_cache[cache_key] = {"data": result, "loaded_at": datetime.now()}
        return result
    
    plan_names = filtered.column("Plan_Name").to_pylist()
    dates = filtered.column("Reporting_Date").to_pylist()
    
    results = {}
    for metric in metrics:
        if metric not in filtered.column_names:
            results[metric] = {"Plan_Name": [], "Reporting_Date": [], "metric_value": []}
            continue
            
        values = filtered.column(metric).to_pylist()
        
        # Aggregate
        aggregated = {}
        for plan, date, value in zip(plan_names, dates, values):
            key = (plan, date)
            if key not in aggregated:
                aggregated[key] = 0
            if value is not None:
                aggregated[key] += value
        
        result_plans, result_dates, result_values = [], [], []
        for (plan, date), total in sorted(aggregated.items()):
            result_plans.append(plan)
            result_dates.append(date)
            result_values.append(total)
        
        results[metric] = {
            "Plan_Name": result_plans,
            "Reporting_Date": result_dates,
            "metric_value": result_values
        }
    
    _query_cache[cache_key] = {"data": results, "loaded_at": datetime.now()}
    return results


# =============================================================================
# REFRESH FUNCTIONS
# =============================================================================

def refresh_bq_to_staging():
    """Query BigQuery and save to staging cache."""
    try:
        log_debug("Starting BQ refresh...")
        data = load_from_bigquery()
        
        bucket = get_gcs_bucket()
        if bucket:
            save_parquet_to_gcs(bucket, GCS_STAGING_CACHE, data)
            set_metadata_timestamp(bucket, GCS_BQ_REFRESH_METADATA)
            return True, "BQ refresh complete. Data saved to staging."
        return False, "GCS bucket not configured"
    except Exception as e:
        log_debug(f"BQ refresh error: {e}")
        return False, f"BQ refresh failed: {str(e)}"


def refresh_gcs_from_staging():
    """Copy staging cache to active cache."""
    global _app_cache, _derived_cache, _query_cache
    
    try:
        bucket = get_gcs_bucket()
        if not bucket:
            return False, "GCS bucket not configured"
        
        staging_blob = bucket.blob(GCS_STAGING_CACHE)
        if not staging_blob.exists():
            return False, "No staging data. Run Refresh BQ first."
        
        data = load_parquet_from_gcs(bucket, GCS_STAGING_CACHE)
        if data is None:
            return False, "Failed to load staging data"
        
        save_parquet_to_gcs(bucket, GCS_ACTIVE_CACHE, data)
        set_metadata_timestamp(bucket, GCS_GCS_REFRESH_METADATA)
        
        # Clear ALL caches
        _app_cache = {
            "data": None, 
            "loaded_at": None, 
            "date_bounds": None,
            "plan_groups_active": None, 
            "plan_groups_inactive": None
        }
        _derived_cache = {
            "date_bounds": {"data": None, "loaded_at": None},
            "plan_groups_active": {"data": None, "loaded_at": None},
            "plan_groups_inactive": {"data": None, "loaded_at": None},
        }
        _query_cache = {}
        
        return True, "GCS refresh complete."
    except Exception as e:
        return False, f"GCS refresh failed: {str(e)}"


def clear_all_caches():
    """Clear all caches - used after data refresh"""
    global _app_cache, _derived_cache, _query_cache, _metadata_cache, _gcs_bucket_cache
    
    _app_cache = {
        "data": None, 
        "loaded_at": None, 
        "date_bounds": None,
        "plan_groups_active": None, 
        "plan_groups_inactive": None
    }
    _derived_cache = {
        "date_bounds": {"data": None, "loaded_at": None},
        "plan_groups_active": {"data": None, "loaded_at": None},
        "plan_groups_inactive": {"data": None, "loaded_at": None},
    }
    _query_cache = {}
    _metadata_cache = {
        "bq_refresh": None,
        "gcs_refresh": None,
        "loaded_at": None
    }


def get_last_bq_refresh():
    """Get last BQ refresh time - CACHED"""
    global _metadata_cache
    
    if _is_metadata_cache_valid() and _metadata_cache.get("bq_refresh") is not None:
        return _metadata_cache["bq_refresh"]
    
    bucket = get_gcs_bucket()
    bq_time = get_metadata_timestamp(bucket, GCS_BQ_REFRESH_METADATA)
    gcs_time = get_metadata_timestamp(bucket, GCS_GCS_REFRESH_METADATA)
    
    _metadata_cache["bq_refresh"] = bq_time
    _metadata_cache["gcs_refresh"] = gcs_time
    _metadata_cache["loaded_at"] = datetime.now()
    
    return bq_time


def get_last_gcs_refresh():
    """Get last GCS refresh time - CACHED"""
    global _metadata_cache
    
    if _is_metadata_cache_valid() and _metadata_cache.get("gcs_refresh") is not None:
        return _metadata_cache["gcs_refresh"]
    
    bucket = get_gcs_bucket()
    bq_time = get_metadata_timestamp(bucket, GCS_BQ_REFRESH_METADATA)
    gcs_time = get_metadata_timestamp(bucket, GCS_GCS_REFRESH_METADATA)
    
    _metadata_cache["bq_refresh"] = bq_time
    _metadata_cache["gcs_refresh"] = gcs_time
    _metadata_cache["loaded_at"] = datetime.now()
    
    return gcs_time


def format_refresh_timestamp(timestamp):
    return timestamp.strftime("%d %b, %H:%M") if timestamp else "--"


def is_staging_ready():
    """Check if staging data is ready"""
    bq = get_last_bq_refresh()
    gcs = get_last_gcs_refresh()
    return bq is not None and (gcs is None or bq > gcs)


def get_cache_info():
    info = {
        "loaded": False, 
        "source": "Not loaded",
        "last_bq_refresh": "--", 
        "last_gcs_refresh": "--",
        "staging_ready": False, 
        "rows": 0,
        "gcs_configured": bool(GCS_BUCKET_NAME),
        "gcs_bucket": GCS_BUCKET_NAME or "Not set"
    }
    try:
        info["last_bq_refresh"] = format_refresh_timestamp(get_last_bq_refresh())
        info["last_gcs_refresh"] = format_refresh_timestamp(get_last_gcs_refresh())
        info["staging_ready"] = is_staging_ready()
        
        if _app_cache.get("data") is not None:
            info["loaded"] = True
            info["rows"] = _app_cache["data"].num_rows
            info["source"] = "App Cache"
        return info
    except Exception as e:
        info["error"] = str(e)
        return info
