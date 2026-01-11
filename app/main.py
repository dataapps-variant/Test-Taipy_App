"""
Variant Analytics Dashboard - Taipy Version
Main Application Entry Point
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
# STATE VARIABLES
# =============================================================================

# Authentication state
is_authenticated = False
current_user = ""
current_user_role = ""

# Current page
current_page = "login"

# Landing page state
dashboard_data = pd.DataFrame([
    {"Dashboard": "ICARUS - Plan (Historical)", "Status": "✅ Active", "Last BQ Refresh": "--", "Last GCS Refresh": "--"},
    {"Dashboard": "ICARUS - Multi", "Status": "⏸️ Disabled", "Last BQ Refresh": "--", "Last GCS Refresh": "--"},
    {"Dashboard": "Vol/Val Plan Level", "Status": "⏸️ Disabled", "Last BQ Refresh": "--", "Last GCS Refresh": "--"},
])

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

# Plan options
active_plan_options = []
inactive_plan_options = []

# Tables
active_regular_df = pd.DataFrame()
active_crystal_df = pd.DataFrame()
inactive_regular_df = pd.DataFrame()
inactive_crystal_df = pd.DataFrame()

# Charts - create empty figures
def make_empty_fig():
    fig = go.Figure()
    fig.update_layout(
        height=300,
        paper_bgcolor="#1E293B",
        plot_bgcolor="#1E293B",
        font=dict(color="#F1F5F9"),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        annotations=[dict(text="Load data to see chart", x=0.5, y=0.5, showarrow=False, font=dict(size=14, color="#94A3B8"))]
    )
    return fig

# Individual chart variables
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

# Login form
login_username = ""
login_password = ""
login_error = ""

# Refresh status
refresh_status = ""

# Admin
show_admin_dialog = False
users_df = pd.DataFrame([
    {"User ID": "admin", "Name": "Administrator", "Role": "Admin"},
    {"User ID": "viewer", "Name": "Viewer User", "Role": "Read Only"},
])
new_user_name = ""
new_user_id = ""
new_user_password = ""
new_user_role = "readonly"
create_user_status = ""

# Options for selectors
bc_options_list = [str(x) for x in BC_OPTIONS]
cohort_options_list = COHORT_OPTIONS
metric_options_list = list(METRICS_CONFIG.keys())
role_options_list = ["admin", "readonly"]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

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
    display = config.get("display", metric_name)
    suffix = config.get("suffix", "")
    return f"{display}{suffix}"


def process_pivot_data(pivot_data, selected_metrics, is_crystal_ball=False):
    if not pivot_data or "Reporting_Date" not in pivot_data or len(pivot_data["Reporting_Date"]) == 0:
        return pd.DataFrame()
    
    unique_dates = sorted(set(pivot_data["Reporting_Date"]), reverse=True)[:10]
    
    date_map = {}
    for d in unique_dates:
        if hasattr(d, 'strftime'):
            date_map[d] = d.strftime("%m/%d/%Y")
        else:
            date_map[d] = str(d)
    
    plan_combos = []
    seen = set()
    for i in range(len(pivot_data["App_Name"])):
        combo = (pivot_data["App_Name"][i], pivot_data["Plan_Name"][i])
        if combo not in seen:
            plan_combos.append(combo)
            seen.add(combo)
    plan_combos.sort()
    
    lookup = {}
    for i in range(len(pivot_data["Reporting_Date"])):
        key = (pivot_data["App_Name"][i], pivot_data["Plan_Name"][i], pivot_data["Reporting_Date"][i])
        if key not in lookup:
            lookup[key] = {}
        for metric in selected_metrics:
            if metric in pivot_data:
                lookup[key][metric] = pivot_data[metric][i]
    
    rows = []
    for app_name, plan_name in plan_combos:
        for metric in selected_metrics:
            row = {"App": app_name, "Plan": plan_name, "Metric": get_display_metric_name(metric)}
            for d in unique_dates:
                key = (app_name, plan_name, d)
                raw_value = lookup.get(key, {}).get(metric)
                row[date_map[d]] = format_metric_value(raw_value, metric, is_crystal_ball)
            rows.append(row)
    
    return pd.DataFrame(rows)


# =============================================================================
# PAGE DEFINITIONS
# =============================================================================

login_page_md = """
# 🔷 VARIANT GROUP

### Sign in to access your dashboards

<|{login_username}|input|label=Username|>

<|{login_password}|input|label=Password|password|>

<|Login|button|on_action=on_login|>

<|{login_error}|>

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

## Users
<|{users_df}|table|>

## Add New User

<|{new_user_name}|input|label=Display Name|>
<|{new_user_id}|input|label=Login ID|>
<|{new_user_password}|input|label=Password|password|>
<|{new_user_role}|selector|lov={role_options_list}|dropdown|label=Role|>

<|Create User|button|on_action=create_user|>
<|{create_user_status}|>

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

### Filters - Active

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

**Plans**
<|{active_selected_plans}|selector|lov={active_plan_options}|multiple|dropdown|>

**Metrics**
<|{active_selected_metrics}|selector|lov={metric_options_list}|multiple|dropdown|>

<|Load Data|button|on_action=load_active_data|>

---

### 📊 Regular Data
<|{active_regular_df}|table|page_size=10|>

### 🔮 Crystal Ball Data
<|{active_crystal_df}|table|page_size=10|>

---

### Charts

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

### Filters - Inactive

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

**Plans**
<|{inactive_selected_plans}|selector|lov={inactive_plan_options}|multiple|dropdown|>

**Metrics**
<|{inactive_selected_metrics}|selector|lov={metric_options_list}|multiple|dropdown|>

<|Load Data|button|on_action=load_inactive_data|>

---

### 📊 Regular Data
<|{inactive_regular_df}|table|page_size=10|>

### 🔮 Crystal Ball Data
<|{inactive_crystal_df}|table|page_size=10|>

---

### Charts

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
    
    try:
        info = get_cache_info()
        state.last_bq_refresh = info.get("last_bq_refresh", "--")
        state.last_gcs_refresh = info.get("last_gcs_refresh", "--")
    except Exception as e:
        logger.warning(f"Could not load cache info: {e}")
    
    rows = []
    for d in DASHBOARDS:
        rows.append({
            "Dashboard": d["name"],
            "Status": "✅ Active" if d.get("enabled") else "⏸️ Disabled",
            "Last BQ Refresh": state.last_bq_refresh if d.get("enabled") else "--",
            "Last GCS Refresh": state.last_gcs_refresh if d.get("enabled") else "--"
        })
    state.dashboard_data = pd.DataFrame(rows)


def on_login(state: State):
    """Handle login"""
    username = state.login_username
    password = state.login_password
    
    if not username or not password:
        state.login_error = "Please enter username and password"
        return
    
    if username in TAIPY_CREDENTIALS and TAIPY_CREDENTIALS[username] == password:
        state.is_authenticated = True
        state.current_user = DEFAULT_USERS[username]["name"]
        state.current_user_role = DEFAULT_USERS[username]["role"]
        state.login_error = ""
        state.current_page = "landing"
        
        try:
            init_icarus_data(state)
        except Exception as e:
            logger.warning(f"Could not init ICARUS data: {e}")
        
        notify(state, "success", f"Welcome, {state.current_user}!")
    else:
        state.login_error = "Invalid username or password"


def on_logout(state: State):
    """Handle logout"""
    state.is_authenticated = False
    state.current_user = ""
    state.current_user_role = ""
    state.current_page = "login"
    notify(state, "info", "Logged out")


def toggle_theme(state: State):
    """Toggle theme"""
    notify(state, "info", "Theme toggle not yet implemented")


def goto_landing(state: State):
    """Go to landing page"""
    state.current_page = "landing"


def goto_icarus(state: State):
    """Go to ICARUS dashboard"""
    state.current_page = "icarus"
    try:
        init_icarus_data(state)
    except Exception as e:
        notify(state, "error", f"Error loading data: {e}")


def show_admin(state: State):
    """Show admin dialog"""
    if state.current_user_role == "admin":
        state.show_admin_dialog = True
    else:
        notify(state, "warning", "Admin access required")


def close_admin(state: State, id, payload):
    """Close admin dialog"""
    state.show_admin_dialog = False


def create_user(state: State):
    """Create new user"""
    if not state.new_user_name or not state.new_user_id or not state.new_user_password:
        state.create_user_status = "Please fill all fields"
        return
    state.create_user_status = f"User '{state.new_user_id}' created (demo)"
    notify(state, "success", "User created")


def init_icarus_data(state: State):
    """Initialize ICARUS dashboard data"""
    try:
        date_bounds = load_date_bounds()
        state.active_from_date = date_bounds["min_date"]
        state.active_to_date = date_bounds["max_date"]
        state.inactive_from_date = date_bounds["min_date"]
        state.inactive_to_date = date_bounds["max_date"]
        
        active_plans = load_plan_groups("Active")
        inactive_plans = load_plan_groups("Inactive")
        
        state.active_plan_options = sorted(set(active_plans.get("Plan_Name", [])))
        state.inactive_plan_options = sorted(set(inactive_plans.get("Plan_Name", [])))
        
        if DEFAULT_PLAN in state.active_plan_options:
            state.active_selected_plans = [DEFAULT_PLAN]
        elif state.active_plan_options:
            state.active_selected_plans = [state.active_plan_options[0]]
            
        if state.inactive_plan_options:
            state.inactive_selected_plans = [state.inactive_plan_options[0]]
            
        logger.info(f"Loaded {len(state.active_plan_options)} active, {len(state.inactive_plan_options)} inactive plans")
    except Exception as e:
        logger.error(f"Error in init_icarus_data: {e}")
        raise


def load_active_data(state: State):
    """Load data for active tab"""
    try:
        notify(state, "info", "Loading data...")
        
        plans = state.active_selected_plans
        metrics = state.active_selected_metrics
        
        if not plans:
            notify(state, "warning", "Select at least one plan")
            return
        if not metrics:
            notify(state, "warning", "Select at least one metric")
            return
        
        from_date = state.active_from_date
        to_date = state.active_to_date
        bc = int(state.active_bc) if state.active_bc else DEFAULT_BC
        cohort = state.active_cohort or DEFAULT_COHORT
        
        pivot_regular = load_pivot_data(from_date, to_date, bc, cohort, plans, metrics, "Regular", "Active")
        pivot_crystal = load_pivot_data(from_date, to_date, bc, cohort, plans, metrics, "Crystal Ball", "Active")
        
        state.active_regular_df = process_pivot_data(pivot_regular, metrics, False)
        state.active_crystal_df = process_pivot_data(pivot_crystal, metrics, True)
        
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
        
        notify(state, "success", "Data loaded!")
        
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
            notify(state, "warning", "Select at least one plan")
            return
        if not metrics:
            notify(state, "warning", "Select at least one metric")
            return
        
        from_date = state.inactive_from_date
        to_date = state.inactive_to_date
        bc = int(state.inactive_bc) if state.inactive_bc else DEFAULT_BC
        cohort = state.inactive_cohort or DEFAULT_COHORT
        
        pivot_regular = load_pivot_data(from_date, to_date, bc, cohort, plans, metrics, "Regular", "Inactive")
        pivot_crystal = load_pivot_data(from_date, to_date, bc, cohort, plans, metrics, "Crystal Ball", "Inactive")
        
        state.inactive_regular_df = process_pivot_data(pivot_regular, metrics, False)
        state.inactive_crystal_df = process_pivot_data(pivot_crystal, metrics, True)
        
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
        
        notify(state, "success", "Data loaded!")
        
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
# MAIN PAGE WITH ROUTING
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
        logger.info(f"✓ Active plans: {len(active_plans.get('Plan_Name', []))}")
        
        inactive_plans = load_plan_groups("Inactive")
        logger.info(f"✓ Inactive plans: {len(inactive_plans.get('Plan_Name', []))}")
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
