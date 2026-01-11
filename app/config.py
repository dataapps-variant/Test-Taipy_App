"""
Configuration and Constants for Variant Analytics Dashboard
Taipy Version - Converted from Dash
"""

import os

# =============================================================================
# APPLICATION INFO
# =============================================================================
APP_NAME = "VARIANT GROUP"
APP_TITLE = "VARIANT GROUP"
VERSION = "2.0.0-taipy"

# =============================================================================
# BIGQUERY CONFIGURATION
# =============================================================================
BIGQUERY_PROJECT = "variant-finance-data-project"
BIGQUERY_DATASET = "ICARUS_Multi"
BIGQUERY_TABLE = "Final_Table"
BIGQUERY_FULL_TABLE = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

# Cache TTL (24 hours in seconds)
CACHE_TTL = 86400

# Auto refresh time (UTC) - 10:15 AM UTC daily
AUTO_REFRESH_HOUR = 10
AUTO_REFRESH_MINUTE = 15

# =============================================================================
# CACHE FILE NAMES (GCS)
# =============================================================================
GCS_ACTIVE_CACHE = "cache/master_data.parquet"
GCS_STAGING_CACHE = "cache/staging_data.parquet"
GCS_BQ_REFRESH_METADATA = "cache/bq_last_refresh.txt"
GCS_GCS_REFRESH_METADATA = "cache/gcs_last_refresh.txt"
GCS_USERS_FILE = "cache/users.json"
GCS_SESSIONS_PREFIX = "cache/sessions/"

# =============================================================================
# SESSION CONFIGURATION
# =============================================================================
SESSION_TTL_DEFAULT = 86400  # 1 day in seconds
SESSION_TTL_REMEMBER = 2592000  # 30 days in seconds
SECRET_KEY = os.environ.get("SECRET_KEY", "variant-dashboard-secret-key-change-in-production")

# =============================================================================
# DASHBOARD REGISTRY
# =============================================================================
DASHBOARDS = [
    {"id": "icarus_historical", "name": "ICARUS - Plan (Historical)", "icon": "📊", "enabled": True},
    {"id": "icarus_multi", "name": "ICARUS - Multi", "icon": "📈", "enabled": False},
    {"id": "vol_val_plan", "name": "Vol/Val Plan Level", "icon": "📉", "enabled": False},
    {"id": "pd_metrics", "name": "PD Metrics_Merged", "icon": "📋", "enabled": False},
    {"id": "dt_metrics", "name": "DT Metrics_Merged", "icon": "📑", "enabled": False},
    {"id": "icarus_cohort", "name": "ICARUS - Cohort", "icon": "👥", "enabled": False},
    {"id": "jf_metrics", "name": "JF_Metrics_Merged", "icon": "📊", "enabled": False},
    {"id": "cwc", "name": "CWC", "icon": "🔄", "enabled": False},
    {"id": "vol_val_entity", "name": "Vol/Val Entity Level", "icon": "🏢", "enabled": False},
    {"id": "ct_metrics", "name": "CT Metrics_Merged", "icon": "📈", "enabled": False},
]

# =============================================================================
# FILTER OPTIONS
# =============================================================================
BC_OPTIONS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
COHORT_OPTIONS = ["7K", "7K_30D"]

# Default filter values
DEFAULT_BC = 4
DEFAULT_COHORT = "7K"
DEFAULT_PLAN = "JF2788ST"

# =============================================================================
# METRICS CONFIGURATION
# =============================================================================
METRICS_CONFIG = {
    "Subscriptions": {"display": "Subscriptions", "format": "number", "suffix": ""},
    "Rebills": {"display": "Rebills", "format": "number", "suffix": ""},
    "Churn_Rate": {"display": "Churn Rate", "format": "percent", "suffix": " (%)"},
    "Refund_Rate": {"display": "Refund Rate", "format": "percent", "suffix": " (%)"},
    "Gross_ARPU_Retention_Rate": {"display": "Gross ARPU Retention", "format": "percent", "suffix": " (%)"},
    "Net_ARPU_Retention_Rate": {"display": "Net ARPU Retention", "format": "percent", "suffix": " (%)"},
    "Cohort_CAC": {"display": "Cohort CAC", "format": "dollar", "suffix": " ($)"},
    "Recent_CAC": {"display": "Recent CAC", "format": "dollar", "suffix": " ($)"},
    "Gross_ARPU_Discounted": {"display": "Gross ARPU", "format": "dollar", "suffix": " ($)"},
    "Net_ARPU_Discounted": {"display": "Net ARPU", "format": "dollar", "suffix": " ($)"},
    "Net_LTV_Discounted": {"display": "Net LTV", "format": "dollar", "suffix": " ($)"},
    "BC4_CAC_Ceiling": {"display": "BC4 CAC Ceiling", "format": "dollar", "suffix": " ($)"},
}

# Metrics list for filters
METRICS_LIST = list(METRICS_CONFIG.keys())

# =============================================================================
# CHART CONFIGURATION (10 metrics x 2 versions = 20 charts)
# =============================================================================
CHART_METRICS = [
    {"display": "Recent LTV", "metric": "Net_LTV_Discounted", "agg": "SUM", "format": "dollar"},
    {"display": "Gross ARPU", "metric": "Gross_ARPU_Discounted", "agg": "SUM", "format": "dollar"},
    {"display": "Net ARPU", "metric": "Net_ARPU_Discounted", "agg": "SUM", "format": "dollar"},
    {"display": "Subscriptions", "metric": "Subscriptions", "agg": "SUM", "format": "number"},
    {"display": "Rebills", "metric": "Rebills", "agg": "SUM", "format": "number"},
    {"display": "Churn", "metric": "Churn_Rate", "agg": "SUM", "format": "percent"},
    {"display": "Gross Retention", "metric": "Gross_ARPU_Retention_Rate", "agg": "SUM", "format": "percent"},
    {"display": "Refund", "metric": "Refund_Rate", "agg": "SUM", "format": "percent"},
    {"display": "Net ARPU Retention", "metric": "Net_ARPU_Retention_Rate", "agg": "SUM", "format": "percent"},
    {"display": "Recent CAC", "metric": "Recent_CAC", "agg": "SUM", "format": "dollar"},
]

# =============================================================================
# APP COLORS (14 apps - Universal for all charts)
# =============================================================================
APP_COLORS = {
    "AT": "#F97316",        # Orange
    "CL": "#3B82F6",        # Blue
    "CN": "#22C55E",        # Green
    "CT-Non-JP": "#14B8A6", # Teal
    "CT-JP": "#EC4899",     # Pink
    "CV": "#A855F7",        # Purple
    "DT": "#F59E0B",        # Amber
    "EN": "#84CC16",        # Lime
    "FS": "#EF4444",        # Red
    "IQ": "#6366F1",        # Indigo
    "JF": "#10B981",        # Emerald
    "PD": "#F43F5E",        # Rose
    "RL": "#0EA5E9",        # Sky
    "RT": "#8B5CF6",        # Violet
}

# =============================================================================
# THEME COLORS - Dark and Light modes
# =============================================================================
THEME_COLORS = {
    "dark": {
        "background": "#0F172A",
        "surface": "#1E293B",
        "border": "#334155",
        "text_primary": "#F1F5F9",
        "text_secondary": "#94A3B8",
        "accent": "#14B8A6",
        "accent_hover": "#0D9488",
        "success": "#34D399",
        "warning": "#FBBF24",
        "danger": "#F87171",
        "card_bg": "#1E293B",
        "input_bg": "#0F172A",
        "hover": "#334155",
        "logo_color": "#F1F5F9",
        "table_header_bg": "#334155",
        "table_row_odd": "#1E293B",
        "table_row_even": "#0F172A",
    },
    "light": {
        "background": "#F8FAFC",
        "surface": "#FFFFFF",
        "border": "#CBD5E1",
        "text_primary": "#0F172A",
        "text_secondary": "#475569",
        "accent": "#0F766E",
        "accent_hover": "#0D5D56",
        "success": "#059669",
        "warning": "#D97706",
        "danger": "#DC2626",
        "card_bg": "#FFFFFF",
        "input_bg": "#F1F5F9",
        "hover": "#E2E8F0",
        "logo_color": "#0F172A",
        "table_header_bg": "#E2E8F0",
        "table_row_odd": "#FFFFFF",
        "table_row_even": "#F8FAFC",
    }
}

# =============================================================================
# DEFAULT USERS FOR TAIPY AUTHENTICATOR
# =============================================================================
DEFAULT_USERS = {
    "admin": {
        "password": "admin123",
        "role": "admin",
        "name": "Administrator",
        "dashboards": "all"
    },
    "viewer": {
        "password": "viewer123",
        "role": "readonly",
        "name": "Viewer User",
        "dashboards": ["icarus_historical"]
    }
}

# Taipy credentials format: list of (username, password) tuples
TAIPY_CREDENTIALS = {
    username: info["password"] 
    for username, info in DEFAULT_USERS.items()
}

# =============================================================================
# ROLE OPTIONS
# =============================================================================
ROLE_OPTIONS = ["admin", "readonly"]
ROLE_DISPLAY = {
    "admin": "Admin",
    "readonly": "Read Only"
}
