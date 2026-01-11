"""
Variant Analytics Dashboard - Taipy Version
Main Application Entry Point

To run:
    taipy run app/main.py --port 8080

Environment Variables:
    GCS_CACHE_BUCKET - GCS bucket name for caching
    GOOGLE_APPLICATION_CREDENTIALS - Path to service account JSON
    SECRET_KEY - Secret key for session encryption
"""

import os
import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any
import pandas as pd

from taipy.gui import Gui, State, navigate, notify
from taipy.gui.gui_actions import on_init

from app.config import (
    APP_NAME, APP_TITLE, DASHBOARDS,
    BC_OPTIONS, COHORT_OPTIONS, DEFAULT_BC, DEFAULT_COHORT, DEFAULT_PLAN,
    METRICS_CONFIG, CHART_METRICS, TAIPY_CREDENTIALS, DEFAULT_USERS
)
from app.bigquery_client import (
    load_date_bounds, load_plan_groups, load_pivot_data, load_all_chart_data,
    refresh_bq_to_staging, refresh_gcs_from_staging, get_cache_info
)
from app.charts import build_line_chart, create_empty_chart
from app.colors import build_plan_color_map

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# STATE VARIABLES - Default values for all pages
# =============================================================================

# Authentication state
is_authenticated = False
current_user = ""
current_user_role = ""

# Theme state
theme = "dark"

# Current page
current_page = "login"

# Landing page state
dashboard_data = pd.DataFrame()

# ICARUS Dashboard state - Active tab
active_tab = "active"  # "active" or "inactive"

# Filters - Active
active_from_date = date.today()
active_to_date = date.today()
active_bc = DEFAULT_BC
active_cohort = DEFAULT_COHORT
active_selected_plans = [DEFAULT_PLAN]
active_selected_metrics = list(METRICS_CONFIG.keys())

# Filters - Inactive
inactive_from_date = date.today()
inactive_to_date = date.today()
inactive_bc = DEFAULT_BC
inactive_cohort = DEFAULT_COHORT
inactive_selected_plans = []
inactive_selected_metrics = list(METRICS_CONFIG.keys())

# Plan options (populated from data)
active_plan_options = []
inactive_plan_options = []

# Pivot table data
active_regular_df = pd.DataFrame()
active_crystal_df = pd.DataFrame()
inactive_regular_df = pd.DataFrame()
inactive_crystal_df = pd.DataFrame()

# Chart figures (20 charts - 10 metrics x 2 versions)
# We'll store them as a dict but reference individually
charts = {}

# Initialize empty charts
for i, cm in enumerate(CHART_METRICS):
    charts[f"active_regular_{i}"] = create_empty_chart(cm["display"])
    charts[f"active_crystal_{i}"] = create_empty_chart(f"{cm['display']} (Crystal Ball)")
    charts[f"inactive_regular_{i}"] = create_empty_chart(cm["display"])
    charts[f"inactive_crystal_{i}"] = create_empty_chart(f"{cm['display']} (Crystal Ball)")

# Shorthand chart variables for Taipy binding
chart_active_regular_0 = create_empty_chart()
chart_active_crystal_0 = create_empty_chart()
chart_active_regular_1 = create_empty_chart()
chart_active_crystal_1 = create_empty_chart()
chart_active_regular_2 = create_empty_chart()
chart_active_crystal_2 = create_empty_chart()
chart_active_regular_3 = create_empty_chart()
chart_active_crystal_3 = create_empty_chart()
chart_active_regular_4 = create_empty_chart()
chart_active_crystal_4 = create_empty_chart()
chart_active_regular_5 = create_empty_chart()
chart_active_crystal_5 = create_empty_chart()
chart_active_regular_6 = create_empty_chart()
chart_active_crystal_6 = create_empty_chart()
chart_active_regular_7 = create_empty_chart()
chart_active_crystal_7 = create_empty_chart()
chart_active_regular_8 = create_empty_chart()
chart_active_crystal_8 = create_empty_chart()
chart_active_regular_9 = create_empty_chart()
chart_active_crystal_9 = create_empty_chart()

# Same for inactive
chart_inactive_regular_0 = create_empty_chart()
chart_inactive_crystal_0 = create_empty_chart()
chart_inactive_regular_1 = create_empty_chart()
chart_inactive_crystal_1 = create_empty_chart()
chart_inactive_regular_2 = create_empty_chart()
chart_inactive_crystal_2 = create_empty_chart()
chart_inactive_regular_3 = create_empty_chart()
chart_inactive_crystal_3 = create_empty_chart()
chart_inactive_regular_4 = create_empty_chart()
chart_inactive_crystal_4 = create_empty_chart()
chart_inactive_regular_5 = create_empty_chart()
chart_inactive_crystal_5 = create_empty_chart()
chart_inactive_regular_6 = create_empty_chart()
chart_inactive_crystal_6 = create_empty_chart()
chart_inactive_regular_7 = create_empty_chart()
chart_inactive_crystal_7 = create_empty_chart()
chart_inactive_regular_8 = create_empty_chart()
chart_inactive_crystal_8 = create_empty_chart()
chart_inactive_regular_9 = create_empty_chart()
chart_inactive_crystal_9 = create_empty_chart()

# Cache info
cache_info_text = "--"
last_bq_refresh = "--"
last_gcs_refresh = "--"

# Login form
login_username = ""
login_password = ""
login_error = ""

# Refresh status
refresh_status = ""

# Loading states
is_loading = False

# Admin panel
show_admin_dialog = False
users_df = pd.DataFrame()
new_user_name = ""
new_user_id = ""
new_user_password = ""
new_user_role = "readonly"
create_user_status = ""

# Metric options for selector
metric_options = [(m, METRICS_CONFIG[m]["display"]) for m in METRICS_CONFIG.keys()]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_plans_by_app(plan_groups):
    """Group plans by App_Name"""
    result = {}
    for app, plan in zip(plan_groups["App_Name"], plan_groups["Plan_Name"]):
        if app not in result:
            result[app] = []
        if plan not in result[app]:
            result[app].append(plan)
    return result


def format_metric_value(value, metric_name, is_crystal_ball=False):
    """Format value based on metric type"""
    if value is None or pd.isna(value):
        return None
    
    config = METRICS_CONFIG.get(metric_name, {})
    format_type = config.get("format", "number")
    
    try:
        if metric_name == "Rebills" and is_crystal_ball:
            return round(float(value))
        
        if format_type == "percent":
            return round(float(value) * 100, 2)
        return round(float(value), 2)
    except:
        return None


def get_display_metric_name(metric_name):
    """Get display name with suffix"""
    config = METRICS_CONFIG.get(metric_name, {})
    display = config.get("display", metric_name)
    suffix = config.get("suffix", "")
    return f"{display}{suffix}"


def process_pivot_data(pivot_data, selected_metrics, is_crystal_ball=False):
    """Process pivot data into DataFrame"""
    if not pivot_data or "Reporting_Date" not in pivot_data or len(pivot_data["Reporting_Date"]) == 0:
        return pd.DataFrame()
    
    unique_dates = sorted(set(pivot_data["Reporting_Date"]), reverse=True)
    
    date_columns = []
    date_map = {}
    for d in unique_dates:
        if hasattr(d, 'strftime'):
            formatted = d.strftime("%m/%d/%Y")
        else:
            formatted = str(d)
        date_columns.append(formatted)
        date_map[d] = formatted
    
    plan_combos = []
    seen = set()
    for i in range(len(pivot_data["App_Name"])):
        combo = (pivot_data["App_Name"][i], pivot_data["Plan_Name"][i])
        if combo not in seen:
            plan_combos.append(combo)
            seen.add(combo)
    
    plan_combos.sort(key=lambda x: (x[0], x[1]))
    
    lookup = {}
    for i in range(len(pivot_data["Reporting_Date"])):
        app = pivot_data["App_Name"][i]
        plan = pivot_data["Plan_Name"][i]
        date = pivot_data["Reporting_Date"][i]
        
        key = (app, plan, date)
        if key not in lookup:
            lookup[key] = {}
        
        for metric in selected_metrics:
            if metric in pivot_data:
                lookup[key][metric] = pivot_data[metric][i]
    
    rows = []
    for app_name, plan_name in plan_combos:
        for metric in selected_metrics:
            row = {
                "App": app_name,
                "Plan": plan_name,
                "Metric": get_display_metric_name(metric)
            }
            
            for d in unique_dates:
                formatted_date = date_map[d]
                key = (app_name, plan_name, d)
                raw_value = lookup.get(key, {}).get(metric, None)
                formatted_value = format_metric_value(raw_value, metric, is_crystal_ball)
                row[formatted_date] = formatted_value
            
            rows.append(row)
    
    df = pd.DataFrame(rows)
    column_order = ["App", "Plan", "Metric"] + date_columns
    df = df[[c for c in column_order if c in df.columns]]
    
    return df


# =============================================================================
# PAGE DEFINITIONS
# =============================================================================

# Login Page
login_page = """
<|container|class_name=login-container|

<|layout|columns=1 1 1|

<|part|class_name=spacer|
|>

<|part|class_name=login-card|

<|text-center|
# 🔷 VARIANT GROUP
### Sign in to access your dashboards
|>

<|{login_username}|input|label=Username|class_name=login-input|>

<|{login_password}|input|label=Password|password=True|class_name=login-input|>

<|Login|button|on_action=on_login|class_name=login-button|>

<|{login_error}|text|class_name=error-text|>

<|part|class_name=demo-credentials|
**Demo Credentials:**

Admin: admin / admin123

Viewer: viewer / viewer123
|>

|>

<|part|class_name=spacer|
|>

|>

|>
"""

# Landing Page
landing_page = """
<|container|class_name=main-container|

<|layout|columns=3 1|

<|part|
# 🔷 VARIANT GROUP
### Welcome back, {current_user}
|>

<|part|class_name=header-actions|
<|☀️ Toggle Theme|button|on_action=toggle_theme|class_name=theme-button|>
<|🚪 Logout|button|on_action=on_logout|class_name=logout-button|>
<|🔧 Admin|button|on_action=show_admin|class_name=admin-button|active={current_user_role == 'admin'}|>
|>

|>

---

## 📊 Available Dashboards

<|{dashboard_data}|table|class_name=dashboard-table|>

<|text-center|
### Click to open a dashboard:

<|📊 ICARUS - Plan (Historical)|button|on_action=goto_icarus|class_name=nav-button|>
|>

---

<|text-center|
*Disabled dashboards: ICARUS - Multi, Vol/Val Plan Level, PD Metrics_Merged, DT Metrics_Merged, ICARUS - Cohort, JF_Metrics_Merged, CWC, Vol/Val Entity Level, CT Metrics_Merged*
|>

|>

<|{show_admin_dialog}|dialog|title=Admin Panel|on_action=close_admin|labels=Close|

## 👥 Users

<|{users_df}|table|>

---

## ➕ Add New User

<|layout|columns=1 1|
<|{new_user_name}|input|label=Display Name|>
<|{new_user_id}|input|label=Login ID|>
|>

<|layout|columns=1 1|
<|{new_user_password}|input|label=Password|password=True|>
<|{new_user_role}|selector|lov=admin;readonly|label=Role|dropdown=True|>
|>

<|Create User|button|on_action=create_user|>

<|{create_user_status}|text|>

|>
"""

# ICARUS Dashboard Page
icarus_page = """
<|container|class_name=main-container|

<|layout|columns=1 4 1|

<|part|
<|← Back|button|on_action=goto_landing|class_name=back-button|>
|>

<|part|class_name=text-center|
## ICARUS - Plan (Historical)
|>

<|part|class_name=header-actions|
<|☀️|button|on_action=toggle_theme|class_name=icon-button|>
<|🚪 Logout|button|on_action=on_logout|class_name=logout-button|>
|>

|>

---

<|layout|columns=3 1|

<|part|
|>

<|part|class_name=refresh-card|
**🔄 Data Refresh**

<|layout|columns=1 1|
<|Refresh BQ|button|on_action=on_refresh_bq|class_name=refresh-button|>
<|part|Last: {last_bq_refresh}|>
|>

<|layout|columns=1 1|
<|Refresh GCS|button|on_action=on_refresh_gcs|class_name=refresh-button|>
<|part|Last: {last_gcs_refresh}|>
|>

<|{refresh_status}|text|class_name=refresh-status|>
|>

|>

---

<|{active_tab}|toggle|lov=active:📈 Active;inactive:📉 Inactive|class_name=tab-toggle|>

<|part|render={active_tab == 'active'}|

<|expandable|title=📊 Filters - Active|expanded=True|

<|layout|columns=1 1 1 1|

<|part|
**Date Range**

<|layout|columns=1 1|
<|{active_from_date}|date|label=From|>
<|{active_to_date}|date|label=To|>
|>
|>

<|part|
**Billing Cycle**

<|{active_bc}|selector|lov={BC_OPTIONS}|dropdown=True|>
|>

<|part|
**Cohort**

<|{active_cohort}|selector|lov={COHORT_OPTIONS}|dropdown=True|>
|>

<|part|
<|🔄 Reset Filters|button|on_action=reset_active_filters|class_name=reset-button|>
|>

|>

---

**Plan Groups**

<|{active_selected_plans}|selector|lov={active_plan_options}|multiple=True|dropdown=True|class_name=plan-selector|>

---

**Metrics**

<|{active_selected_metrics}|selector|lov={metric_options}|multiple=True|dropdown=True|class_name=metric-selector|>

|>

<|text-center|
<|Load Data|button|on_action=load_active_data|class_name=load-button|>
|>

---

### 📊 Plan Overview (Regular)
<|{active_regular_df}|table|class_name=pivot-table|page_size=20|allow_all_rows=True|>

### 🔮 Plan Overview (Crystal Ball)
<|{active_crystal_df}|table|class_name=pivot-table|page_size=20|allow_all_rows=True|>

---

### Charts

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[0]['display']}**
<|chart|figure={chart_active_regular_0}|>
|>
<|part|
**{CHART_METRICS[0]['display']} (Crystal Ball)**
<|chart|figure={chart_active_crystal_0}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[1]['display']}**
<|chart|figure={chart_active_regular_1}|>
|>
<|part|
**{CHART_METRICS[1]['display']} (Crystal Ball)**
<|chart|figure={chart_active_crystal_1}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[2]['display']}**
<|chart|figure={chart_active_regular_2}|>
|>
<|part|
**{CHART_METRICS[2]['display']} (Crystal Ball)**
<|chart|figure={chart_active_crystal_2}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[3]['display']}**
<|chart|figure={chart_active_regular_3}|>
|>
<|part|
**{CHART_METRICS[3]['display']} (Crystal Ball)**
<|chart|figure={chart_active_crystal_3}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[4]['display']}**
<|chart|figure={chart_active_regular_4}|>
|>
<|part|
**{CHART_METRICS[4]['display']} (Crystal Ball)**
<|chart|figure={chart_active_crystal_4}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[5]['display']}**
<|chart|figure={chart_active_regular_5}|>
|>
<|part|
**{CHART_METRICS[5]['display']} (Crystal Ball)**
<|chart|figure={chart_active_crystal_5}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[6]['display']}**
<|chart|figure={chart_active_regular_6}|>
|>
<|part|
**{CHART_METRICS[6]['display']} (Crystal Ball)**
<|chart|figure={chart_active_crystal_6}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[7]['display']}**
<|chart|figure={chart_active_regular_7}|>
|>
<|part|
**{CHART_METRICS[7]['display']} (Crystal Ball)**
<|chart|figure={chart_active_crystal_7}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[8]['display']}**
<|chart|figure={chart_active_regular_8}|>
|>
<|part|
**{CHART_METRICS[8]['display']} (Crystal Ball)**
<|chart|figure={chart_active_crystal_8}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[9]['display']}**
<|chart|figure={chart_active_regular_9}|>
|>
<|part|
**{CHART_METRICS[9]['display']} (Crystal Ball)**
<|chart|figure={chart_active_crystal_9}|>
|>
|>

|>

<|part|render={active_tab == 'inactive'}|

<|expandable|title=📊 Filters - Inactive|expanded=True|

<|layout|columns=1 1 1 1|

<|part|
**Date Range**

<|layout|columns=1 1|
<|{inactive_from_date}|date|label=From|>
<|{inactive_to_date}|date|label=To|>
|>
|>

<|part|
**Billing Cycle**

<|{inactive_bc}|selector|lov={BC_OPTIONS}|dropdown=True|>
|>

<|part|
**Cohort**

<|{inactive_cohort}|selector|lov={COHORT_OPTIONS}|dropdown=True|>
|>

<|part|
<|🔄 Reset Filters|button|on_action=reset_inactive_filters|class_name=reset-button|>
|>

|>

---

**Plan Groups**

<|{inactive_selected_plans}|selector|lov={inactive_plan_options}|multiple=True|dropdown=True|class_name=plan-selector|>

---

**Metrics**

<|{inactive_selected_metrics}|selector|lov={metric_options}|multiple=True|dropdown=True|class_name=metric-selector|>

|>

<|text-center|
<|Load Data|button|on_action=load_inactive_data|class_name=load-button|>
|>

---

### 📊 Plan Overview (Regular)
<|{inactive_regular_df}|table|class_name=pivot-table|page_size=20|allow_all_rows=True|>

### 🔮 Plan Overview (Crystal Ball)
<|{inactive_crystal_df}|table|class_name=pivot-table|page_size=20|allow_all_rows=True|>

---

### Charts

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[0]['display']}**
<|chart|figure={chart_inactive_regular_0}|>
|>
<|part|
**{CHART_METRICS[0]['display']} (Crystal Ball)**
<|chart|figure={chart_inactive_crystal_0}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[1]['display']}**
<|chart|figure={chart_inactive_regular_1}|>
|>
<|part|
**{CHART_METRICS[1]['display']} (Crystal Ball)**
<|chart|figure={chart_inactive_crystal_1}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[2]['display']}**
<|chart|figure={chart_inactive_regular_2}|>
|>
<|part|
**{CHART_METRICS[2]['display']} (Crystal Ball)**
<|chart|figure={chart_inactive_crystal_2}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[3]['display']}**
<|chart|figure={chart_inactive_regular_3}|>
|>
<|part|
**{CHART_METRICS[3]['display']} (Crystal Ball)**
<|chart|figure={chart_inactive_crystal_3}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[4]['display']}**
<|chart|figure={chart_inactive_regular_4}|>
|>
<|part|
**{CHART_METRICS[4]['display']} (Crystal Ball)**
<|chart|figure={chart_inactive_crystal_4}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[5]['display']}**
<|chart|figure={chart_inactive_regular_5}|>
|>
<|part|
**{CHART_METRICS[5]['display']} (Crystal Ball)**
<|chart|figure={chart_inactive_crystal_5}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[6]['display']}**
<|chart|figure={chart_inactive_regular_6}|>
|>
<|part|
**{CHART_METRICS[6]['display']} (Crystal Ball)**
<|chart|figure={chart_inactive_crystal_6}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[7]['display']}**
<|chart|figure={chart_inactive_regular_7}|>
|>
<|part|
**{CHART_METRICS[7]['display']} (Crystal Ball)**
<|chart|figure={chart_inactive_crystal_7}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[8]['display']}**
<|chart|figure={chart_inactive_regular_8}|>
|>
<|part|
**{CHART_METRICS[8]['display']} (Crystal Ball)**
<|chart|figure={chart_inactive_crystal_8}|>
|>
|>

<|layout|columns=1 1|
<|part|
**{CHART_METRICS[9]['display']}**
<|chart|figure={chart_inactive_regular_9}|>
|>
<|part|
**{CHART_METRICS[9]['display']} (Crystal Ball)**
<|chart|figure={chart_inactive_crystal_9}|>
|>
|>

|>

|>
"""

# Root page with navigation
root_page = """
<|navbar|lov={[("login", "Login"), ("landing", "Home"), ("icarus", "ICARUS")]}|>

<|part|render={current_page == 'login' and not is_authenticated}|
""" + login_page + """
|>

<|part|render={current_page == 'landing' or (is_authenticated and current_page == 'login')}|
""" + landing_page + """
|>

<|part|render={current_page == 'icarus'}|
""" + icarus_page + """
|>
"""

# =============================================================================
# CALLBACKS / EVENT HANDLERS
# =============================================================================

def on_init(state: State):
    """Initialize the application state"""
    logger.info("Initializing application...")
    
    # Load cache info
    try:
        info = get_cache_info()
        state.last_bq_refresh = info.get("last_bq_refresh", "--")
        state.last_gcs_refresh = info.get("last_gcs_refresh", "--")
    except Exception as e:
        logger.error(f"Error loading cache info: {e}")
    
    # Build dashboard data for landing page
    dashboard_rows = []
    for dashboard in DASHBOARDS:
        is_enabled = dashboard.get("enabled", False)
        status = "✅ Active" if is_enabled else "⏸️ Disabled"
        dashboard_rows.append({
            "Dashboard": dashboard["name"],
            "Status": status,
            "Last BQ Refresh": state.last_bq_refresh if is_enabled else "--",
            "Last GCS Refresh": state.last_gcs_refresh if is_enabled else "--"
        })
    state.dashboard_data = pd.DataFrame(dashboard_rows)
    
    # Build users table for admin
    users_rows = []
    for user_id, info in DEFAULT_USERS.items():
        users_rows.append({
            "User ID": user_id,
            "Name": info["name"],
            "Role": "Admin" if info["role"] == "admin" else "Read Only",
            "Password": "••••••••"
        })
    state.users_df = pd.DataFrame(users_rows)


def on_login(state: State):
    """Handle login"""
    username = state.login_username
    password = state.login_password
    
    if not username or not password:
        state.login_error = "Please enter both username and password"
        return
    
    # Simple authentication against TAIPY_CREDENTIALS
    if username in TAIPY_CREDENTIALS and TAIPY_CREDENTIALS[username] == password:
        state.is_authenticated = True
        state.current_user = DEFAULT_USERS[username]["name"]
        state.current_user_role = DEFAULT_USERS[username]["role"]
        state.login_error = ""
        state.current_page = "landing"
        
        # Initialize ICARUS data
        initialize_icarus_data(state)
        
        notify(state, "success", f"Welcome, {state.current_user}!")
    else:
        state.login_error = "Invalid username or password"
        notify(state, "error", "Login failed")


def on_logout(state: State):
    """Handle logout"""
    state.is_authenticated = False
    state.current_user = ""
    state.current_user_role = ""
    state.current_page = "login"
    state.login_username = ""
    state.login_password = ""
    notify(state, "info", "Logged out successfully")


def toggle_theme(state: State):
    """Toggle between dark and light theme"""
    state.theme = "light" if state.theme == "dark" else "dark"
    notify(state, "info", f"Theme changed to {state.theme}")


def goto_landing(state: State):
    """Navigate to landing page"""
    state.current_page = "landing"


def goto_icarus(state: State):
    """Navigate to ICARUS dashboard"""
    state.current_page = "icarus"
    
    # Initialize data if not already done
    if not state.active_plan_options:
        initialize_icarus_data(state)


def show_admin(state: State):
    """Show admin dialog"""
    if state.current_user_role == "admin":
        state.show_admin_dialog = True


def close_admin(state: State, id, payload):
    """Close admin dialog"""
    state.show_admin_dialog = False


def create_user(state: State):
    """Create a new user (simplified - just shows notification)"""
    if not state.new_user_name or not state.new_user_id or not state.new_user_password:
        state.create_user_status = "Please fill all fields"
        return
    
    # In production, this would save to GCS
    state.create_user_status = f"User '{state.new_user_id}' created (demo mode)"
    notify(state, "success", f"User created: {state.new_user_name}")
    
    # Clear form
    state.new_user_name = ""
    state.new_user_id = ""
    state.new_user_password = ""


def initialize_icarus_data(state: State):
    """Initialize ICARUS dashboard data"""
    try:
        # Load date bounds
        date_bounds = load_date_bounds()
        min_date = date_bounds["min_date"]
        max_date = date_bounds["max_date"]
        
        state.active_from_date = min_date
        state.active_to_date = max_date
        state.inactive_from_date = min_date
        state.inactive_to_date = max_date
        
        # Load plan groups
        active_plans = load_plan_groups("Active")
        inactive_plans = load_plan_groups("Inactive")
        
        state.active_plan_options = sorted(set(active_plans["Plan_Name"]))
        state.inactive_plan_options = sorted(set(inactive_plans["Plan_Name"]))
        
        # Set default selection
        if DEFAULT_PLAN in state.active_plan_options:
            state.active_selected_plans = [DEFAULT_PLAN]
        elif state.active_plan_options:
            state.active_selected_plans = [state.active_plan_options[0]]
            
        if state.inactive_plan_options:
            state.inactive_selected_plans = [state.inactive_plan_options[0]]
        
        logger.info(f"Initialized ICARUS: {len(state.active_plan_options)} active plans, {len(state.inactive_plan_options)} inactive plans")
        
    except Exception as e:
        logger.error(f"Error initializing ICARUS data: {e}")
        notify(state, "error", f"Error loading data: {str(e)}")


def reset_active_filters(state: State):
    """Reset active tab filters to defaults"""
    try:
        date_bounds = load_date_bounds()
        state.active_from_date = date_bounds["min_date"]
        state.active_to_date = date_bounds["max_date"]
        state.active_bc = DEFAULT_BC
        state.active_cohort = DEFAULT_COHORT
        state.active_selected_metrics = list(METRICS_CONFIG.keys())
        
        if DEFAULT_PLAN in state.active_plan_options:
            state.active_selected_plans = [DEFAULT_PLAN]
        
        notify(state, "info", "Filters reset")
    except Exception as e:
        notify(state, "error", f"Error resetting filters: {str(e)}")


def reset_inactive_filters(state: State):
    """Reset inactive tab filters to defaults"""
    try:
        date_bounds = load_date_bounds()
        state.inactive_from_date = date_bounds["min_date"]
        state.inactive_to_date = date_bounds["max_date"]
        state.inactive_bc = DEFAULT_BC
        state.inactive_cohort = DEFAULT_COHORT
        state.inactive_selected_metrics = list(METRICS_CONFIG.keys())
        
        if state.inactive_plan_options:
            state.inactive_selected_plans = [state.inactive_plan_options[0]]
        
        notify(state, "info", "Filters reset")
    except Exception as e:
        notify(state, "error", f"Error resetting filters: {str(e)}")


def load_active_data(state: State):
    """Load data for active tab"""
    try:
        notify(state, "info", "Loading data...")
        
        selected_plans = state.active_selected_plans
        selected_metrics = state.active_selected_metrics
        
        if not selected_plans:
            notify(state, "warning", "Please select at least one plan")
            return
        
        if not selected_metrics:
            notify(state, "warning", "Please select at least one metric")
            return
        
        from_date = state.active_from_date
        to_date = state.active_to_date
        bc = int(state.active_bc)
        cohort = state.active_cohort
        
        # Load pivot data
        pivot_regular = load_pivot_data(from_date, to_date, bc, cohort, selected_plans, selected_metrics, "Regular", "Active")
        pivot_crystal = load_pivot_data(from_date, to_date, bc, cohort, selected_plans, selected_metrics, "Crystal Ball", "Active")
        
        state.active_regular_df = process_pivot_data(pivot_regular, selected_metrics, False)
        state.active_crystal_df = process_pivot_data(pivot_crystal, selected_metrics, True)
        
        # Load chart data
        chart_metric_names = [cm["metric"] for cm in CHART_METRICS]
        all_regular_data = load_all_chart_data(from_date, to_date, bc, cohort, selected_plans, chart_metric_names, "Regular", "Active")
        all_crystal_data = load_all_chart_data(from_date, to_date, bc, cohort, selected_plans, chart_metric_names, "Crystal Ball", "Active")
        
        # Build charts
        for i, chart_config in enumerate(CHART_METRICS):
            metric = chart_config["metric"]
            format_type = chart_config["format"]
            display_name = chart_config["display"]
            
            if format_type == "dollar":
                display_title = f"{display_name} ($)"
            elif format_type == "percent":
                display_title = f"{display_name} (%)"
            else:
                display_title = display_name
            
            chart_data_regular = all_regular_data.get(metric, {"Plan_Name": [], "Reporting_Date": [], "metric_value": []})
            chart_data_crystal = all_crystal_data.get(metric, {"Plan_Name": [], "Reporting_Date": [], "metric_value": []})
            
            fig_regular, _ = build_line_chart(chart_data_regular, display_title, format_type, (from_date, to_date), state.theme)
            fig_crystal, _ = build_line_chart(chart_data_crystal, f"{display_title} (Crystal Ball)", format_type, (from_date, to_date), state.theme)
            
            # Update state for each chart
            setattr(state, f"chart_active_regular_{i}", fig_regular)
            setattr(state, f"chart_active_crystal_{i}", fig_crystal)
        
        notify(state, "success", "Data loaded successfully!")
        
    except Exception as e:
        logger.error(f"Error loading active data: {e}")
        notify(state, "error", f"Error loading data: {str(e)}")


def load_inactive_data(state: State):
    """Load data for inactive tab"""
    try:
        notify(state, "info", "Loading data...")
        
        selected_plans = state.inactive_selected_plans
        selected_metrics = state.inactive_selected_metrics
        
        if not selected_plans:
            notify(state, "warning", "Please select at least one plan")
            return
        
        if not selected_metrics:
            notify(state, "warning", "Please select at least one metric")
            return
        
        from_date = state.inactive_from_date
        to_date = state.inactive_to_date
        bc = int(state.inactive_bc)
        cohort = state.inactive_cohort
        
        # Load pivot data
        pivot_regular = load_pivot_data(from_date, to_date, bc, cohort, selected_plans, selected_metrics, "Regular", "Inactive")
        pivot_crystal = load_pivot_data(from_date, to_date, bc, cohort, selected_plans, selected_metrics, "Crystal Ball", "Inactive")
        
        state.inactive_regular_df = process_pivot_data(pivot_regular, selected_metrics, False)
        state.inactive_crystal_df = process_pivot_data(pivot_crystal, selected_metrics, True)
        
        # Load chart data
        chart_metric_names = [cm["metric"] for cm in CHART_METRICS]
        all_regular_data = load_all_chart_data(from_date, to_date, bc, cohort, selected_plans, chart_metric_names, "Regular", "Inactive")
        all_crystal_data = load_all_chart_data(from_date, to_date, bc, cohort, selected_plans, chart_metric_names, "Crystal Ball", "Inactive")
        
        # Build charts
        for i, chart_config in enumerate(CHART_METRICS):
            metric = chart_config["metric"]
            format_type = chart_config["format"]
            display_name = chart_config["display"]
            
            if format_type == "dollar":
                display_title = f"{display_name} ($)"
            elif format_type == "percent":
                display_title = f"{display_name} (%)"
            else:
                display_title = display_name
            
            chart_data_regular = all_regular_data.get(metric, {"Plan_Name": [], "Reporting_Date": [], "metric_value": []})
            chart_data_crystal = all_crystal_data.get(metric, {"Plan_Name": [], "Reporting_Date": [], "metric_value": []})
            
            fig_regular, _ = build_line_chart(chart_data_regular, display_title, format_type, (from_date, to_date), state.theme)
            fig_crystal, _ = build_line_chart(chart_data_crystal, f"{display_title} (Crystal Ball)", format_type, (from_date, to_date), state.theme)
            
            # Update state for each chart
            setattr(state, f"chart_inactive_regular_{i}", fig_regular)
            setattr(state, f"chart_inactive_crystal_{i}", fig_crystal)
        
        notify(state, "success", "Data loaded successfully!")
        
    except Exception as e:
        logger.error(f"Error loading inactive data: {e}")
        notify(state, "error", f"Error loading data: {str(e)}")


def on_refresh_bq(state: State):
    """Refresh data from BigQuery"""
    try:
        notify(state, "info", "Refreshing from BigQuery...")
        success, msg = refresh_bq_to_staging()
        
        if success:
            info = get_cache_info()
            state.last_bq_refresh = info.get("last_bq_refresh", "--")
            state.refresh_status = "✅ " + msg
            notify(state, "success", msg)
        else:
            state.refresh_status = "❌ " + msg
            notify(state, "error", msg)
            
    except Exception as e:
        state.refresh_status = f"❌ Error: {str(e)}"
        notify(state, "error", f"Refresh failed: {str(e)}")


def on_refresh_gcs(state: State):
    """Refresh GCS cache"""
    try:
        notify(state, "info", "Refreshing GCS cache...")
        success, msg = refresh_gcs_from_staging()
        
        if success:
            info = get_cache_info()
            state.last_gcs_refresh = info.get("last_gcs_refresh", "--")
            state.refresh_status = "✅ " + msg
            notify(state, "success", msg)
            
            # Reinitialize data
            initialize_icarus_data(state)
        else:
            state.refresh_status = "❌ " + msg
            notify(state, "error", msg)
            
    except Exception as e:
        state.refresh_status = f"❌ Error: {str(e)}"
        notify(state, "error", f"Refresh failed: {str(e)}")


# =============================================================================
# PAGES CONFIGURATION
# =============================================================================

pages = {
    "/": root_page,
}

# =============================================================================
# CREATE AND RUN GUI
# =============================================================================

# Create GUI instance
gui = Gui(pages=pages, css_file="styles/main.css")

if __name__ == "__main__":
    # Preload data at startup
    logger.info("🚀 Preloading data at startup...")
    try:
        date_bounds = load_date_bounds()
        logger.info(f"  ✓ Date bounds loaded: {date_bounds['min_date']} to {date_bounds['max_date']}")
        
        active_plans = load_plan_groups("Active")
        logger.info(f"  ✓ Active plans loaded: {len(active_plans.get('Plan_Name', []))} plans")
        
        inactive_plans = load_plan_groups("Inactive")
        logger.info(f"  ✓ Inactive plans loaded: {len(inactive_plans.get('Plan_Name', []))} plans")
        
        logger.info("✅ Preloading complete")
    except Exception as e:
        logger.error(f"❌ Preloading failed: {e}")
    
    # Run the GUI
    gui.run(
        title=APP_TITLE,
        host="0.0.0.0",
        port=8080,
        dark_mode=True,
        use_reloader=False,
        debug=False
    )
