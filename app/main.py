"""
Variant Analytics Dashboard - Taipy Version
Fixed: Filters, Pivot Tables, Admin Panel
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
runtime_users = dict(DEFAULT_USERS)  # Copy of default users

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
active_selected_apps = []  # NEW: App selection
active_selected_plans = []
active_selected_metrics = list(METRICS_CONFIG.keys())

# Filters - Inactive
inactive_from_date = date.today()
inactive_to_date = date.today()
inactive_bc = str(DEFAULT_BC)
inactive_cohort = DEFAULT_COHORT
inactive_selected_apps = []  # NEW: App selection
inactive_selected_plans = []
inactive_selected_metrics = list(METRICS_CONFIG.keys())

# Options
active_app_options = []
active_plan_options = []  # Will be filtered by selected apps
inactive_app_options = []
inactive_plan_options = []

# Full plan data (for filtering)
active_plans_full = {}  # {app: [plans]}
inactive_plans_full = {}

# Tables
active_regular_df = pd.DataFrame()
active_crystal_df = pd.DataFrame()
inactive_regular_df = pd.DataFrame()
inactive_crystal_df = pd.DataFrame()

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
fig_active_cb_0 = make_empty_fig()
fig_active_1 = make_empty_fig()
fig_active_cb_1 = make_empty_fig()
fig_active_2 = make_empty_fig()
fig_active_cb_2 = make_empty_fig()
fig_active_3 = make_empty_fig()
fig_active_cb_3 = make_empty_fig()
fig_active_4 = make_empty_fig()
fig_active_cb_4 = make_empty_fig()

fig_inactive_0 = make_empty_fig()
fig_inactive_cb_0 = make_empty_fig()
fig_inactive_1 = make_empty_fig()
fig_inactive_cb_1 = make_empty_fig()
fig_inactive_2 = make_empty_fig()
fig_inactive_cb_2 = make_empty_fig()
fig_inactive_3 = make_empty_fig()
fig_inactive_cb_3 = make_empty_fig()
fig_inactive_4 = make_empty_fig()
fig_inactive_cb_4 = make_empty_fig()

# Cache info
last_bq_refresh = "--"
last_gcs_refresh = "--"
refresh_status = ""

# Login
login_username = ""
login_password = ""
login_error = ""

# Admin
show_admin_dialog = False
users_df = pd.DataFrame()
new_user_name = ""
new_user_id = ""
new_user_password = ""
new_user_role = "readonly"
admin_status = ""

# Selector options
bc_options_list = [str(x) for x in BC_OPTIONS]
cohort_options_list = list(COHORT_OPTIONS)
metric_options_list = list(METRICS_CONFIG.keys())
role_options = ["admin", "readonly"]

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
    config = METRICS_CONFIG.get(metric_name, {})
    return config.get("display", metric_name)


def process_pivot_data(pivot_data, selected_metrics, is_crystal_ball=False):
    """Process pivot data - FIXED column names"""
    if not pivot_data or "Reporting_Date" not in pivot_data or len(pivot_data["Reporting_Date"]) == 0:
        return pd.DataFrame(columns=["App", "Plan", "Metric"])
    
    # Get unique dates (most recent 10)
    unique_dates = sorted(set(pivot_data["Reporting_Date"]), reverse=True)[:10]
    
    # Format dates with underscores instead of slashes for column names
    date_columns = []
    date_map = {}
    for d in unique_dates:
        if hasattr(d, 'strftime'):
            # Use format like "Jan_10" instead of "01/10/2025"
            col_name = d.strftime("%b_%d")
            date_map[d] = col_name
        else:
            col_name = str(d).replace("/", "_").replace("-", "_")
            date_map[d] = col_name
        date_columns.append(col_name)
    
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
                "App": app_name,
                "Plan": plan_name,
                "Metric": get_display_metric_name(metric)
            }
            for d in unique_dates:
                col_name = date_map[d]
                key = (app_name, plan_name, d)
                raw_value = lookup.get(key, {}).get(metric)
                row[col_name] = format_metric_value(raw_value, metric, is_crystal_ball)
            rows.append(row)
    
    if not rows:
        return pd.DataFrame(columns=["App", "Plan", "Metric"] + date_columns)
    
    df = pd.DataFrame(rows)
    # Ensure column order
    cols = ["App", "Plan", "Metric"] + date_columns
    df = df[[c for c in cols if c in df.columns]]
    return df


def get_plans_for_apps(plans_full, selected_apps):
    """Get plans filtered by selected apps"""
    if not selected_apps:
        # Return all plans if no apps selected
        all_plans = []
        for app, plans in plans_full.items():
            all_plans.extend(plans)
        return sorted(set(all_plans))
    
    result = []
    for app in selected_apps:
        if app in plans_full:
            result.extend(plans_full[app])
    return sorted(set(result))


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

### Filters - Active Plans

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

**Select Apps (Entity)**

<|{active_selected_apps}|selector|lov={active_app_options}|multiple|dropdown|on_change=on_active_app_change|>

**Select Plans**

<|{active_selected_plans}|selector|lov={active_plan_options}|multiple|dropdown|>

**Select Metrics**

<|{active_selected_metrics}|selector|lov={metric_options_list}|multiple|dropdown|>

---

<|Load Data|button|on_action=load_active_data|>

---

### 📊 Regular Data

<|{active_regular_df}|table|page_size=15|>

### 🔮 Crystal Ball Data

<|{active_crystal_df}|table|page_size=15|>

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

### Filters - Inactive Plans

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

**Select Apps (Entity)**

<|{inactive_selected_apps}|selector|lov={inactive_app_options}|multiple|dropdown|on_change=on_inactive_app_change|>

**Select Plans**

<|{inactive_selected_plans}|selector|lov={inactive_plan_options}|multiple|dropdown|>

**Select Metrics**

<|{inactive_selected_metrics}|selector|lov={metric_options_list}|multiple|dropdown|>

---

<|Load Data|button|on_action=load_inactive_data|>

---

### 📊 Regular Data

<|{inactive_regular_df}|table|page_size=15|>

### 🔮 Crystal Ball Data

<|{inactive_crystal_df}|table|page_size=15|>

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
# CALLBACKS
# =============================================================================

def on_init(state: State):
    """Initialize application state"""
    logger.info("Initializing application...")
    
    # Load cache info
    try:
        info = get_cache_info()
        state.last_bq_refresh = info.get("last_bq_refresh", "--")
        state.last_gcs_refresh = info.get("last_gcs_refresh", "--")
    except Exception as e:
        logger.warning(f"Could not load cache info: {e}")
    
    # Build dashboard table
    rows = []
    for d in DASHBOARDS:
        rows.append({
            "Dashboard": d["name"],
            "Status": "✅ Active" if d.get("enabled") else "⏸️ Disabled",
            "Last BQ Refresh": state.last_bq_refresh if d.get("enabled") else "--",
            "Last GCS Refresh": state.last_gcs_refresh if d.get("enabled") else "--"
        })
    state.dashboard_data = pd.DataFrame(rows)
    
    # Build users table
    state.users_df = build_users_df()


def on_login(state: State):
    """Handle login"""
    username = state.login_username.strip()
    password = state.login_password
    
    if not username or not password:
        state.login_error = "Please enter username and password"
        return
    
    # Check against runtime users
    if username in runtime_users:
        user_info = runtime_users[username]
        expected_pwd = TAIPY_CREDENTIALS.get(username, user_info.get("password", ""))
        if password == expected_pwd:
            state.is_authenticated = True
            state.current_user = user_info["name"]
            state.current_user_role = user_info["role"]
            state.login_error = ""
            state.current_page = "landing"
            
            try:
                init_icarus_data(state)
            except Exception as e:
                logger.warning(f"Could not init ICARUS data: {e}")
            
            notify(state, "success", f"Welcome, {state.current_user}!")
            return
    
    state.login_error = "Invalid username or password"


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
    notify(state, "info", "Theme toggle coming soon")


def goto_landing(state: State):
    state.current_page = "landing"


def goto_icarus(state: State):
    state.current_page = "icarus"
    try:
        init_icarus_data(state)
    except Exception as e:
        notify(state, "error", f"Error loading data: {e}")


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


def init_icarus_data(state: State):
    """Initialize ICARUS dashboard data"""
    try:
        # Load date bounds
        date_bounds = load_date_bounds()
        state.active_from_date = date_bounds["min_date"]
        state.active_to_date = date_bounds["max_date"]
        state.inactive_from_date = date_bounds["min_date"]
        state.inactive_to_date = date_bounds["max_date"]
        
        # Load active plans
        active_data = load_plan_groups("Active")
        active_apps = sorted(set(active_data.get("App_Name", [])))
        state.active_app_options = active_apps
        
        # Build app -> plans mapping for active
        state.active_plans_full = {}
        for app, plan in zip(active_data.get("App_Name", []), active_data.get("Plan_Name", [])):
            if app not in state.active_plans_full:
                state.active_plans_full[app] = []
            if plan not in state.active_plans_full[app]:
                state.active_plans_full[app].append(plan)
        
        # All active plans
        all_active_plans = sorted(set(active_data.get("Plan_Name", [])))
        state.active_plan_options = all_active_plans
        
        # Set defaults
        if DEFAULT_PLAN in all_active_plans:
            state.active_selected_plans = [DEFAULT_PLAN]
        elif all_active_plans:
            state.active_selected_plans = [all_active_plans[0]]
        
        # Load inactive plans
        inactive_data = load_plan_groups("Inactive")
        inactive_apps = sorted(set(inactive_data.get("App_Name", [])))
        state.inactive_app_options = inactive_apps
        
        # Build app -> plans mapping for inactive
        state.inactive_plans_full = {}
        for app, plan in zip(inactive_data.get("App_Name", []), inactive_data.get("Plan_Name", [])):
            if app not in state.inactive_plans_full:
                state.inactive_plans_full[app] = []
            if plan not in state.inactive_plans_full[app]:
                state.inactive_plans_full[app].append(plan)
        
        # All inactive plans
        all_inactive_plans = sorted(set(inactive_data.get("Plan_Name", [])))
        state.inactive_plan_options = all_inactive_plans
        
        if all_inactive_plans:
            state.inactive_selected_plans = [all_inactive_plans[0]]
        
        logger.info(f"Loaded {len(active_apps)} active apps, {len(inactive_apps)} inactive apps")
        
    except Exception as e:
        logger.error(f"Error in init_icarus_data: {e}")
        raise


def on_active_app_change(state: State):
    """When active apps selection changes, filter plans"""
    selected_apps = state.active_selected_apps
    if selected_apps:
        # Filter plans to only show plans from selected apps
        filtered_plans = get_plans_for_apps(state.active_plans_full, selected_apps)
        state.active_plan_options = filtered_plans
        # Keep only selected plans that are still valid
        state.active_selected_plans = [p for p in state.active_selected_plans if p in filtered_plans]
    else:
        # Show all plans if no app filter
        all_plans = get_plans_for_apps(state.active_plans_full, [])
        state.active_plan_options = all_plans


def on_inactive_app_change(state: State):
    """When inactive apps selection changes, filter plans"""
    selected_apps = state.inactive_selected_apps
    if selected_apps:
        filtered_plans = get_plans_for_apps(state.inactive_plans_full, selected_apps)
        state.inactive_plan_options = filtered_plans
        state.inactive_selected_plans = [p for p in state.inactive_selected_plans if p in filtered_plans]
    else:
        all_plans = get_plans_for_apps(state.inactive_plans_full, [])
        state.inactive_plan_options = all_plans


def load_active_data(state: State):
    """Load data for active tab"""
    try:
        notify(state, "info", "Loading data...")
        
        plans = state.active_selected_plans
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
        
        # Load pivot data
        pivot_regular = load_pivot_data(from_date, to_date, bc, cohort, plans, metrics, "Regular", "Active")
        pivot_crystal = load_pivot_data(from_date, to_date, bc, cohort, plans, metrics, "Crystal Ball", "Active")
        
        state.active_regular_df = process_pivot_data(pivot_regular, metrics, False)
        state.active_crystal_df = process_pivot_data(pivot_crystal, metrics, True)
        
        logger.info(f"Regular table: {len(state.active_regular_df)} rows, Crystal: {len(state.active_crystal_df)} rows")
        
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
        
        notify(state, "success", f"Loaded {len(state.active_regular_df)} rows")
        
    except Exception as e:
        logger.error(f"Error loading active data: {e}")
        notify(state, "error", f"Error: {str(e)}")


def load_inactive_data(state: State):
    """Load data for inactive tab"""
    try:
        notify(state, "info", "Loading data...")
        
        plans = state.inactive_selected_plans
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
        
        # Load pivot data
        pivot_regular = load_pivot_data(from_date, to_date, bc, cohort, plans, metrics, "Regular", "Inactive")
        pivot_crystal = load_pivot_data(from_date, to_date, bc, cohort, plans, metrics, "Crystal Ball", "Inactive")
        
        state.inactive_regular_df = process_pivot_data(pivot_regular, metrics, False)
        state.inactive_crystal_df = process_pivot_data(pivot_crystal, metrics, True)
        
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
        
        notify(state, "success", f"Loaded {len(state.inactive_regular_df)} rows")
        
    except Exception as e:
        logger.error(f"Error loading inactive data: {e}")
        notify(state, "error", f"Error: {str(e)}")


def on_refresh_bq(state: State):
    """Refresh from BigQuery"""
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
        state.refresh_status = f"❌ {str(e)}"
        notify(state, "error", str(e))


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
            init_icarus_data(state)
        else:
            state.refresh_status = "❌ " + msg
            notify(state, "error", msg)
    except Exception as e:
        state.refresh_status = f"❌ {str(e)}"
        notify(state, "error", str(e))


# =============================================================================
# MAIN PAGE
# =============================================================================

main_page = """
<|part|render={current_page == 'login' and not is_authenticated}|
""" + login_page_md + """
|>

<|part|render={current_page == 'landing' or (is_authenticated and current_page == 'login')}|
""" + landing_page_md + """
|>

<|part|render={current_page == 'icarus' and is_authenticated}|
""" + icarus_page_md + """
|>
"""

# =============================================================================
# CREATE GUI
# =============================================================================

gui = Gui(page=main_page)

# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    logger.info("🚀 Starting Variant Dashboard (Taipy)...")
    
    try:
        date_bounds = load_date_bounds()
        logger.info(f"✓ Date bounds: {date_bounds['min_date']} to {date_bounds['max_date']}")
        
        active_plans = load_plan_groups("Active")
        logger.info(f"✓ Active: {len(set(active_plans.get('App_Name', [])))} apps, {len(set(active_plans.get('Plan_Name', [])))} plans")
        
        inactive_plans = load_plan_groups("Inactive")
        logger.info(f"✓ Inactive: {len(set(inactive_plans.get('App_Name', [])))} apps, {len(set(inactive_plans.get('Plan_Name', [])))} plans")
    except Exception as e:
        logger.warning(f"⚠️ Preload skipped: {e}")
    
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"🚀 Starting on port {port}...")
    
    gui.run(
        title=APP_TITLE,
        host="0.0.0.0",
        port=port,
        dark_mode=True,
        use_reloader=False,
        debug=False
    )
