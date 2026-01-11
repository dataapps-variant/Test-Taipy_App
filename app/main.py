"""
Variant Analytics Dashboard - Taipy Version
Complete Rewrite: Grouped Filters + Working Pivot Tables
"""

import os
import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any
import pandas as pd
import plotly.graph_objects as go

from taipy.gui import Gui, State, notify

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
# IN-MEMORY USER STORAGE (for admin panel)
# =============================================================================
runtime_users = dict(DEFAULT_USERS)

# =============================================================================
# STATE VARIABLES
# =============================================================================

# Authentication
is_authenticated = False
current_user = ""
current_user_role = ""
current_page = "login"

# Dashboard data
dashboard_data = pd.DataFrame()

# Tab state
active_tab = "active"

# Filters - Active
active_from_date = date.today()
active_to_date = date.today()
active_bc = str(DEFAULT_BC)
active_cohort = DEFAULT_COHORT
active_selected_plans = []
active_selected_metrics = list(METRICS_CONFIG.keys())

# Filters - Inactive
inactive_from_date = date.today()
inactive_to_date = date.today()
inactive_bc = str(DEFAULT_BC)
inactive_cohort = DEFAULT_COHORT
inactive_selected_plans = []
inactive_selected_metrics = list(METRICS_CONFIG.keys())

# Plan options - formatted as "APP | Plan" for grouping
active_plan_options = []
inactive_plan_options = []

# Plan lookup (to extract app/plan from formatted string)
active_plan_lookup = {}  # {"APP | Plan": ("APP", "Plan")}
inactive_plan_lookup = {}

# Tables - use dict format for better Taipy compatibility
active_regular_data = {"columns": ["App", "Plan", "Metric"], "data": []}
active_crystal_data = {"columns": ["App", "Plan", "Metric"], "data": []}
inactive_regular_data = {"columns": ["App", "Plan", "Metric"], "data": []}
inactive_crystal_data = {"columns": ["App", "Plan", "Metric"], "data": []}

# DataFrames for display
active_regular_df = pd.DataFrame()
active_crystal_df = pd.DataFrame()
inactive_regular_df = pd.DataFrame()
inactive_crystal_df = pd.DataFrame()

# Table column configs (dynamic)
active_table_columns = {}
inactive_table_columns = {}

# Charts
def make_empty_fig(title="Load data to see chart"):
    fig = go.Figure()
    fig.update_layout(
        height=300,
        paper_bgcolor="#1E293B",
        plot_bgcolor="#1E293B",
        font=dict(color="#F1F5F9"),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        annotations=[dict(text=title, x=0.5, y=0.5, showarrow=False, font=dict(size=14, color="#94A3B8"))]
    )
    return fig

fig_active_0 = make_empty_fig()
fig_active_1 = make_empty_fig()
fig_active_2 = make_empty_fig()
fig_active_3 = make_empty_fig()
fig_active_4 = make_empty_fig()
fig_active_cb_0 = make_empty_fig()
fig_active_cb_1 = make_empty_fig()
fig_active_cb_2 = make_empty_fig()
fig_active_cb_3 = make_empty_fig()
fig_active_cb_4 = make_empty_fig()

fig_inactive_0 = make_empty_fig()
fig_inactive_1 = make_empty_fig()
fig_inactive_2 = make_empty_fig()
fig_inactive_3 = make_empty_fig()
fig_inactive_4 = make_empty_fig()
fig_inactive_cb_0 = make_empty_fig()
fig_inactive_cb_1 = make_empty_fig()
fig_inactive_cb_2 = make_empty_fig()
fig_inactive_cb_3 = make_empty_fig()
fig_inactive_cb_4 = make_empty_fig()

# UI options
bc_options_list = [str(b) for b in BC_OPTIONS]
cohort_options_list = list(COHORT_OPTIONS)
metric_options_list = list(METRICS_CONFIG.keys())

# Admin
show_admin_dialog = False
users_df = pd.DataFrame()
new_user_name = ""
new_user_id = ""
new_user_password = ""
new_user_role = "viewer"
admin_status = ""
role_options = ["admin", "viewer"]

# Login
login_username = ""
login_password = ""
login_error = ""

# Refresh
last_bq_refresh = "--"
last_gcs_refresh = "--"
refresh_status = ""

# Theme
dark_mode = True


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def build_users_df():
    """Build users DataFrame from runtime users"""
    rows = []
    for uid, info in runtime_users.items():
        rows.append({
            "User ID": uid,
            "Name": info["name"],
            "Role": "Admin" if info["role"] == "admin" else "Read Only"
        })
    return pd.DataFrame(rows)


def format_metric_value(value, metric_name, is_crystal_ball=False):
    if value is None or pd.isna(value):
        return ""
    config = METRICS_CONFIG.get(metric_name, {})
    format_type = config.get("format", "number")
    try:
        if metric_name == "Rebills" and is_crystal_ball:
            return str(round(float(value)))
        if format_type == "percent":
            return f"{round(float(value) * 100, 2)}%"
        return str(round(float(value), 2))
    except:
        return ""


def get_display_metric_name(metric_name):
    config = METRICS_CONFIG.get(metric_name, {})
    return config.get("display", metric_name)


def process_pivot_data(pivot_data, selected_metrics, is_crystal_ball=False):
    """Process pivot data into DataFrame with proper columns
    
    Returns DataFrame with columns: App, Plan, Metric, plus date columns (D1, D2, etc.)
    Also returns a mapping of D1->actual_date for display
    """
    if not pivot_data or "Reporting_Date" not in pivot_data or len(pivot_data["Reporting_Date"]) == 0:
        return pd.DataFrame(columns=["App", "Plan", "Metric", "Info"]), {}
    
    # Get unique dates (most recent 10)
    unique_dates = sorted(set(pivot_data["Reporting_Date"]), reverse=True)[:10]
    
    # Create simple column names and date mapping
    date_cols = []
    date_display = {}  # column_name -> display_date
    for idx, d in enumerate(unique_dates):
        col = f"D{idx+1}"
        date_cols.append(col)
        if hasattr(d, 'strftime'):
            date_display[col] = d.strftime("%m/%d")
        else:
            date_display[col] = str(d)[:5]
    
    # Get unique plan combinations
    plan_combos = []
    seen = set()
    for i in range(len(pivot_data["App_Name"])):
        combo = (pivot_data["App_Name"][i], pivot_data["Plan_Name"][i])
        if combo not in seen:
            plan_combos.append(combo)
            seen.add(combo)
    plan_combos.sort()
    
    # Build lookup
    lookup = {}
    for i in range(len(pivot_data["Reporting_Date"])):
        key = (pivot_data["App_Name"][i], pivot_data["Plan_Name"][i], pivot_data["Reporting_Date"][i])
        if key not in lookup:
            lookup[key] = {}
        for metric in selected_metrics:
            if metric in pivot_data:
                lookup[key][metric] = pivot_data[metric][i]
    
    # Build rows
    rows = []
    for app_name, plan_name in plan_combos:
        for metric in selected_metrics:
            row = {
                "App": str(app_name) if app_name else "",
                "Plan": str(plan_name) if plan_name else "",
                "Metric": get_display_metric_name(metric)
            }
            for idx, d in enumerate(unique_dates):
                col = f"D{idx+1}"
                key = (app_name, plan_name, d)
                raw_value = lookup.get(key, {}).get(metric)
                row[col] = format_metric_value(raw_value, metric, is_crystal_ball)
            rows.append(row)
    
    if not rows:
        return pd.DataFrame(columns=["App", "Plan", "Metric"] + date_cols), date_display
    
    df = pd.DataFrame(rows)
    
    # Rename columns to include dates for display
    rename_map = {col: date_display[col] for col in date_cols if col in df.columns}
    df = df.rename(columns=rename_map)
    
    return df, date_display


def build_plan_options(plan_data):
    """Build formatted plan options with App prefix for grouping
    
    Returns: (options_list, lookup_dict)
    - options_list: ["AT | AT2788YT", "CL | CL2788ST", ...]
    - lookup_dict: {"AT | AT2788YT": ("AT", "AT2788YT"), ...}
    """
    if not plan_data or "App_Name" not in plan_data:
        return [], {}
    
    # Build grouped structure
    app_plans = {}
    for app, plan in zip(plan_data.get("App_Name", []), plan_data.get("Plan_Name", [])):
        if app not in app_plans:
            app_plans[app] = []
        if plan not in app_plans[app]:
            app_plans[app].append(plan)
    
    # Sort apps and plans
    options = []
    lookup = {}
    for app in sorted(app_plans.keys()):
        for plan in sorted(app_plans[app]):
            formatted = f"{app} | {plan}"
            options.append(formatted)
            lookup[formatted] = (app, plan)
    
    return options, lookup


def get_selected_plan_names(selected_formatted, lookup):
    """Extract actual plan names from formatted selections"""
    plans = []
    for formatted in selected_formatted:
        if formatted in lookup:
            _, plan = lookup[formatted]
            plans.append(plan)
    return plans


# =============================================================================
# PAGE DEFINITIONS
# =============================================================================

login_page_md = """
# 🔷 VARIANT GROUP

### Sign in to access your dashboards

<|{login_username}|input|label=Username|>

<|{login_password}|input|label=Password|password|>

<|Login|button|on_action=on_login|>

<|{login_error}|text|class_name=error-text|>

---

**Demo Credentials:** admin / admin123 or viewer / viewer123
"""

landing_page_md = """
# 🔷 VARIANT GROUP

### Welcome back, <|{current_user}|>

<|layout|columns=1 1 1|
<|Toggle Theme|button|on_action=toggle_theme|>
<|Logout|button|on_action=on_logout|>
<|Admin|button|on_action=show_admin|>
|>

---

## 📊 Available Dashboards

<|{dashboard_data}|table|>

---

<|Open ICARUS Dashboard|button|on_action=goto_icarus|>

<|{show_admin_dialog}|dialog|title=Admin Panel|on_action=close_admin|labels=Close|

### 👥 Current Users

<|{users_df}|table|>

---

### ➕ Add New User

<|{new_user_name}|input|label=Display Name|>

<|{new_user_id}|input|label=Login ID|>

<|{new_user_password}|input|label=Password|password|>

<|{new_user_role}|selector|lov={role_options}|dropdown|label=Role|>

<|Create User|button|on_action=create_user|>

<|{admin_status}|text|>

|>
"""

# ICARUS page with GROUPED plan checkboxes
icarus_page_md = """
<|layout|columns=1 3 1|
<|← Back|button|on_action=goto_landing|>
<|
## ICARUS - Plan (Historical)
|>
<|Logout|button|on_action=on_logout|>
|>

---

<|layout|columns=3 1|
<||>
<|
**🔄 Data Refresh**

<|Refresh BQ|button|on_action=on_refresh_bq|> Last: <|{last_bq_refresh}|>

<|Refresh GCS|button|on_action=on_refresh_gcs|> Last: <|{last_gcs_refresh}|>

<|{refresh_status}|>
|>
|>

---

<|{active_tab}|toggle|lov=active;inactive|>

<|part|render={active_tab == 'active'}|

### 📋 Filters - Active Plans

<|layout|columns=1 1 1 1|
<|
**From Date**

<|{active_from_date}|date|>
|>
<|
**To Date**

<|{active_to_date}|date|>
|>
<|
**Billing Cycle**

<|{active_bc}|selector|lov={bc_options_list}|dropdown|>
|>
<|
**Cohort**

<|{active_cohort}|selector|lov={cohort_options_list}|dropdown|>
|>
|>

---

**PLAN GROUPS** (Select plans - grouped by App)

<|{active_selected_plans}|selector|lov={active_plan_options}|multiple|height=300px|>

---

**METRICS**

<|{active_selected_metrics}|selector|lov={metric_options_list}|multiple|>

---

<|Load Data|button|on_action=load_active_data|class_name=primary|>

---

### 📊 Regular Data

<|{active_regular_df}|table|page_size=20|rebuild|>

### 🔮 Crystal Ball Data

<|{active_crystal_df}|table|page_size=20|rebuild|>

---

### 📈 Charts

<|layout|columns=1 1|
<|
**Recent LTV**

<|chart|figure={fig_active_0}|>
|>
<|
**Recent LTV (Crystal Ball)**

<|chart|figure={fig_active_cb_0}|>
|>
|>

<|layout|columns=1 1|
<|
**Gross ARPU**

<|chart|figure={fig_active_1}|>
|>
<|
**Gross ARPU (Crystal Ball)**

<|chart|figure={fig_active_cb_1}|>
|>
|>

<|layout|columns=1 1|
<|
**Net ARPU**

<|chart|figure={fig_active_2}|>
|>
<|
**Net ARPU (Crystal Ball)**

<|chart|figure={fig_active_cb_2}|>
|>
|>

<|layout|columns=1 1|
<|
**Subscriptions**

<|chart|figure={fig_active_3}|>
|>
<|
**Subscriptions (Crystal Ball)**

<|chart|figure={fig_active_cb_3}|>
|>
|>

<|layout|columns=1 1|
<|
**Rebills**

<|chart|figure={fig_active_4}|>
|>
<|
**Rebills (Crystal Ball)**

<|chart|figure={fig_active_cb_4}|>
|>
|>

|>

<|part|render={active_tab == 'inactive'}|

### 📋 Filters - Inactive Plans

<|layout|columns=1 1 1 1|
<|
**From Date**

<|{inactive_from_date}|date|>
|>
<|
**To Date**

<|{inactive_to_date}|date|>
|>
<|
**Billing Cycle**

<|{inactive_bc}|selector|lov={bc_options_list}|dropdown|>
|>
<|
**Cohort**

<|{inactive_cohort}|selector|lov={cohort_options_list}|dropdown|>
|>
|>

---

**PLAN GROUPS** (Select plans - grouped by App)

<|{inactive_selected_plans}|selector|lov={inactive_plan_options}|multiple|height=300px|>

---

**METRICS**

<|{inactive_selected_metrics}|selector|lov={metric_options_list}|multiple|>

---

<|Load Data|button|on_action=load_inactive_data|class_name=primary|>

---

### 📊 Regular Data

<|{inactive_regular_df}|table|page_size=20|rebuild|>

### 🔮 Crystal Ball Data

<|{inactive_crystal_df}|table|page_size=20|rebuild|>

---

### 📈 Charts

<|layout|columns=1 1|
<|
**Recent LTV**

<|chart|figure={fig_inactive_0}|>
|>
<|
**Recent LTV (Crystal Ball)**

<|chart|figure={fig_inactive_cb_0}|>
|>
|>

<|layout|columns=1 1|
<|
**Gross ARPU**

<|chart|figure={fig_inactive_1}|>
|>
<|
**Gross ARPU (Crystal Ball)**

<|chart|figure={fig_inactive_cb_1}|>
|>
|>

<|layout|columns=1 1|
<|
**Net ARPU**

<|chart|figure={fig_inactive_2}|>
|>
<|
**Net ARPU (Crystal Ball)**

<|chart|figure={fig_inactive_cb_2}|>
|>
|>

<|layout|columns=1 1|
<|
**Subscriptions**

<|chart|figure={fig_inactive_3}|>
|>
<|
**Subscriptions (Crystal Ball)**

<|chart|figure={fig_inactive_cb_3}|>
|>
|>

<|layout|columns=1 1|
<|
**Rebills**

<|chart|figure={fig_inactive_4}|>
|>
<|
**Rebills (Crystal Ball)**

<|chart|figure={fig_inactive_cb_4}|>
|>
|>

|>
"""


# =============================================================================
# PAGE ROUTING & AUTH
# =============================================================================

def on_login(state: State):
    username = state.login_username.strip()
    password = state.login_password
    
    if not username or not password:
        state.login_error = "Please enter username and password"
        return
    
    # Check credentials
    if username in TAIPY_CREDENTIALS and TAIPY_CREDENTIALS[username] == password:
        state.is_authenticated = True
        state.current_user = username
        state.current_user_role = runtime_users.get(username, {}).get("role", "viewer")
        state.login_error = ""
        state.current_page = "landing"
        
        # Load dashboard data
        state.dashboard_data = pd.DataFrame(DASHBOARDS)
        
        notify(state, "success", f"Welcome {username}!")
    else:
        state.login_error = "Invalid username or password"
        notify(state, "error", "Invalid credentials")


def on_logout(state: State):
    state.is_authenticated = False
    state.current_user = ""
    state.current_user_role = ""
    state.current_page = "login"
    state.login_username = ""
    state.login_password = ""
    notify(state, "info", "Logged out successfully")


def goto_landing(state: State):
    state.current_page = "landing"


def goto_icarus(state: State):
    state.current_page = "icarus"
    init_icarus_data(state)


def toggle_theme(state: State):
    state.dark_mode = not state.dark_mode
    notify(state, "info", f"Theme: {'Dark' if state.dark_mode else 'Light'}")


def show_admin(state: State):
    if state.current_user_role == "admin":
        state.users_df = build_users_df()
        state.show_admin_dialog = True
    else:
        notify(state, "warning", "Admin access required")


def close_admin(state: State, id, payload):
    state.show_admin_dialog = False


def create_user(state: State):
    """Create new user - stores in runtime memory"""
    global runtime_users
    
    name = state.new_user_name.strip()
    uid = state.new_user_id.strip()
    pwd = state.new_user_password
    role = state.new_user_role
    
    if not name or not uid or not pwd:
        state.admin_status = "❌ Please fill all fields"
        return
    
    if uid in runtime_users:
        state.admin_status = f"❌ User '{uid}' already exists"
        return
    
    # Add to runtime users
    runtime_users[uid] = {
        "name": name,
        "role": role,
        "password": pwd
    }
    
    # Also add to credentials for login
    TAIPY_CREDENTIALS[uid] = pwd
    
    # Update table
    state.users_df = build_users_df()
    
    # Clear form
    state.new_user_name = ""
    state.new_user_id = ""
    state.new_user_password = ""
    state.admin_status = f"✅ User '{uid}' created successfully!"
    
    notify(state, "success", f"User '{name}' created!")


# =============================================================================
# DATA LOADING
# =============================================================================

def init_icarus_data(state: State):
    """Initialize ICARUS dashboard data"""
    try:
        # Load date bounds
        date_bounds = load_date_bounds()
        state.active_from_date = date_bounds["min_date"]
        state.active_to_date = date_bounds["max_date"]
        state.inactive_from_date = date_bounds["min_date"]
        state.inactive_to_date = date_bounds["max_date"]
        
        # Load active plans with grouped format
        active_data = load_plan_groups("Active")
        state.active_plan_options, state.active_plan_lookup = build_plan_options(active_data)
        
        # Set default selection (first plan)
        if state.active_plan_options:
            # Find default plan if exists
            default_found = False
            for opt in state.active_plan_options:
                if DEFAULT_PLAN in opt:
                    state.active_selected_plans = [opt]
                    default_found = True
                    break
            if not default_found:
                state.active_selected_plans = [state.active_plan_options[0]]
        
        # Load inactive plans
        inactive_data = load_plan_groups("Inactive")
        state.inactive_plan_options, state.inactive_plan_lookup = build_plan_options(inactive_data)
        
        if state.inactive_plan_options:
            state.inactive_selected_plans = [state.inactive_plan_options[0]]
        
        # Load cache info for refresh times
        try:
            cache_info = get_cache_info()
            if cache_info.get("bq_staging_loaded_at"):
                state.last_bq_refresh = cache_info["bq_staging_loaded_at"].strftime("%d %b, %H:%M")
            if cache_info.get("gcs_loaded_at"):
                state.last_gcs_refresh = cache_info["gcs_loaded_at"].strftime("%d %b, %H:%M")
        except:
            pass
        
        logger.info(f"Loaded {len(state.active_plan_options)} active plans, {len(state.inactive_plan_options)} inactive plans")
        
    except Exception as e:
        logger.error(f"Error in init_icarus_data: {e}")
        notify(state, "error", f"Failed to load data: {e}")


def load_active_data(state: State):
    """Load data for active tab"""
    try:
        notify(state, "info", "Loading data...")
        
        # Get actual plan names from formatted selections
        plans = get_selected_plan_names(state.active_selected_plans, state.active_plan_lookup)
        metrics = state.active_selected_metrics
        
        if not plans:
            notify(state, "warning", "Please select at least one plan")
            return
        if not metrics:
            notify(state, "warning", "Please select at least one metric")
            return
        
        from_date = state.active_from_date
        to_date = state.active_to_date
        bc = int(state.active_bc) if state.active_bc else DEFAULT_BC
        cohort = state.active_cohort or DEFAULT_COHORT
        
        logger.info(f"Loading active data: {len(plans)} plans, {len(metrics)} metrics")
        logger.info(f"Selected plans: {plans}")
        
        # Load pivot data
        pivot_regular = load_pivot_data(from_date, to_date, bc, cohort, plans, metrics, "Regular", "Active")
        pivot_crystal = load_pivot_data(from_date, to_date, bc, cohort, plans, metrics, "Crystal Ball", "Active")
        
        # Process into DataFrames
        df_regular, date_map_r = process_pivot_data(pivot_regular, metrics, False)
        df_crystal, date_map_c = process_pivot_data(pivot_crystal, metrics, True)
        
        # Update state - create NEW DataFrame objects to force refresh
        state.active_regular_df = df_regular.copy()
        state.active_crystal_df = df_crystal.copy()
        
        logger.info(f"Regular table: {len(df_regular)} rows, columns: {list(df_regular.columns)}")
        logger.info(f"Crystal table: {len(df_crystal)} rows, columns: {list(df_crystal.columns)}")
        
        # Load charts
        chart_metrics = [cm["metric"] for cm in CHART_METRICS[:5]]
        all_regular = load_all_chart_data(from_date, to_date, bc, cohort, plans, chart_metrics, "Regular", "Active")
        all_crystal = load_all_chart_data(from_date, to_date, bc, cohort, plans, chart_metrics, "Crystal Ball", "Active")
        
        for i, cm in enumerate(CHART_METRICS[:5]):
            metric = cm["metric"]
            fmt = cm["format"]
            
            data_r = all_regular.get(metric, {"Plan_Name": [], "Reporting_Date": [], "metric_value": []})
            data_c = all_crystal.get(metric, {"Plan_Name": [], "Reporting_Date": [], "metric_value": []})
            
            fig_r, _ = build_line_chart(data_r, cm["display"], fmt, (from_date, to_date), "dark")
            fig_c, _ = build_line_chart(data_c, f"{cm['display']} (CB)", fmt, (from_date, to_date), "dark")
            
            setattr(state, f"fig_active_{i}", fig_r)
            setattr(state, f"fig_active_cb_{i}", fig_c)
        
        notify(state, "success", f"Loaded {len(df_regular)} rows")
        
    except Exception as e:
        logger.error(f"Error loading active data: {e}")
        import traceback
        traceback.print_exc()
        notify(state, "error", f"Error: {str(e)}")


def load_inactive_data(state: State):
    """Load data for inactive tab"""
    try:
        notify(state, "info", "Loading data...")
        
        plans = get_selected_plan_names(state.inactive_selected_plans, state.inactive_plan_lookup)
        metrics = state.inactive_selected_metrics
        
        if not plans:
            notify(state, "warning", "Please select at least one plan")
            return
        if not metrics:
            notify(state, "warning", "Please select at least one metric")
            return
        
        from_date = state.inactive_from_date
        to_date = state.inactive_to_date
        bc = int(state.inactive_bc) if state.inactive_bc else DEFAULT_BC
        cohort = state.inactive_cohort or DEFAULT_COHORT
        
        logger.info(f"Loading inactive data: {len(plans)} plans, {len(metrics)} metrics")
        
        # Load pivot data
        pivot_regular = load_pivot_data(from_date, to_date, bc, cohort, plans, metrics, "Regular", "Inactive")
        pivot_crystal = load_pivot_data(from_date, to_date, bc, cohort, plans, metrics, "Crystal Ball", "Inactive")
        
        # Process into DataFrames
        df_regular, _ = process_pivot_data(pivot_regular, metrics, False)
        df_crystal, _ = process_pivot_data(pivot_crystal, metrics, True)
        
        state.inactive_regular_df = df_regular.copy()
        state.inactive_crystal_df = df_crystal.copy()
        
        # Load charts
        chart_metrics = [cm["metric"] for cm in CHART_METRICS[:5]]
        all_regular = load_all_chart_data(from_date, to_date, bc, cohort, plans, chart_metrics, "Regular", "Inactive")
        all_crystal = load_all_chart_data(from_date, to_date, bc, cohort, plans, chart_metrics, "Crystal Ball", "Inactive")
        
        for i, cm in enumerate(CHART_METRICS[:5]):
            metric = cm["metric"]
            fmt = cm["format"]
            
            data_r = all_regular.get(metric, {"Plan_Name": [], "Reporting_Date": [], "metric_value": []})
            data_c = all_crystal.get(metric, {"Plan_Name": [], "Reporting_Date": [], "metric_value": []})
            
            fig_r, _ = build_line_chart(data_r, cm["display"], fmt, (from_date, to_date), "dark")
            fig_c, _ = build_line_chart(data_c, f"{cm['display']} (CB)", fmt, (from_date, to_date), "dark")
            
            setattr(state, f"fig_inactive_{i}", fig_r)
            setattr(state, f"fig_inactive_cb_{i}", fig_c)
        
        notify(state, "success", f"Loaded {len(df_regular)} rows")
        
    except Exception as e:
        logger.error(f"Error loading inactive data: {e}")
        notify(state, "error", f"Error: {str(e)}")


def on_refresh_bq(state: State):
    try:
        state.refresh_status = "Refreshing BQ data..."
        notify(state, "info", "Refreshing from BigQuery...")
        refresh_bq_to_staging()
        state.last_bq_refresh = datetime.now().strftime("%d %b, %H:%M")
        state.refresh_status = "BQ refresh complete!"
        notify(state, "success", "BigQuery data refreshed!")
    except Exception as e:
        state.refresh_status = f"Error: {e}"
        notify(state, "error", f"Refresh failed: {e}")


def on_refresh_gcs(state: State):
    try:
        state.refresh_status = "Refreshing GCS data..."
        notify(state, "info", "Refreshing from GCS...")
        refresh_gcs_from_staging()
        state.last_gcs_refresh = datetime.now().strftime("%d %b, %H:%M")
        state.refresh_status = "GCS refresh complete!"
        notify(state, "success", "GCS data refreshed!")
    except Exception as e:
        state.refresh_status = f"Error: {e}"
        notify(state, "error", f"Refresh failed: {e}")


# =============================================================================
# MAIN PAGE LAYOUT
# =============================================================================

main_page = """
<|part|render={current_page == 'login'}|
""" + login_page_md + """
|>

<|part|render={current_page == 'landing'}|
""" + landing_page_md + """
|>

<|part|render={current_page == 'icarus'}|
""" + icarus_page_md + """
|>
"""


# =============================================================================
# APP INITIALIZATION
# =============================================================================

def on_init(state: State):
    """Initialize app state"""
    state.dashboard_data = pd.DataFrame()
    state.users_df = build_users_df()
    
    # Initialize empty tables
    state.active_regular_df = pd.DataFrame(columns=["App", "Plan", "Metric"])
    state.active_crystal_df = pd.DataFrame(columns=["App", "Plan", "Metric"])
    state.inactive_regular_df = pd.DataFrame(columns=["App", "Plan", "Metric"])
    state.inactive_crystal_df = pd.DataFrame(columns=["App", "Plan", "Metric"])


if __name__ == "__main__":
    # Create and run GUI
    gui = Gui(page=main_page)
    
    # Run with authentication disabled (we handle it manually)
    gui.run(
        title=APP_TITLE,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080)),
        dark_mode=True,
        use_reloader=False,
        on_init=on_init
    )
