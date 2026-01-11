"""
Chart Components for Variant Analytics Dashboard - Taipy Version

Taipy natively supports Plotly, so this is very similar to the Dash version.

Customizations:
- Zoom & Pan enabled
- Download CSV button
- Thin lines (1px) with semi-transparency
- X-axis: Month Year format (Jan 2024)
- Tooltip: Full date on hover
- Lines start from first data point
"""

import plotly.graph_objects as go
from app.colors import build_plan_color_map
from app.config import THEME_COLORS


def get_theme_colors(theme="dark"):
    """Get the color palette for specified theme"""
    return THEME_COLORS.get(theme, THEME_COLORS["dark"])


def hex_to_rgba(hex_color, opacity=1.0):
    """Convert hex color to rgba string"""
    hex_color = hex_color.lstrip('#')
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    return f"rgba({r}, {g}, {b}, {opacity})"


def build_legend_data(plans, color_map):
    """Build legend data for external legend component"""
    legend_items = []
    for plan in plans:
        color = color_map.get(plan, "#6B7280")
        legend_items.append({
            "plan": plan,
            "color": color
        })
    return legend_items


def build_line_chart(data, display_name, format_type="dollar", date_range=None, theme="dark"):
    """
    Build a line chart for a metric by Plan over time
    
    Args:
        data: Dict with Plan_Name, Reporting_Date, metric_value lists
        display_name: Chart title
        format_type: 'dollar', 'percent', or 'number'
        date_range: Tuple of (min_date, max_date) for x-axis range
        theme: 'dark' or 'light'
    
    Returns:
        Plotly figure and list of unique plans
    """
    
    colors = get_theme_colors(theme)
    
    # Check for empty data
    if not data or "Plan_Name" not in data or len(data["Plan_Name"]) == 0:
        fig = go.Figure()
        fig.update_layout(
            height=350,
            paper_bgcolor=colors["card_bg"],
            plot_bgcolor=colors["card_bg"],
            font=dict(family="Inter, sans-serif", size=12, color=colors["text_primary"]),
            annotations=[{
                "text": "No data available for selected filters",
                "xref": "paper",
                "yref": "paper",
                "x": 0.5,
                "y": 0.5,
                "showarrow": False,
                "font": {"size": 14, "color": colors["text_secondary"]}
            }]
        )
        return fig, []
    
    # Get unique plans and build color map
    unique_plans = sorted(set(data["Plan_Name"]))
    color_map = build_plan_color_map(unique_plans)
    
    # Organize data by plan
    plan_data = {}
    for i in range(len(data["Plan_Name"])):
        plan = data["Plan_Name"][i]
        date = data["Reporting_Date"][i]
        value = data["metric_value"][i]
        
        if plan not in plan_data:
            plan_data[plan] = {"dates": [], "values": []}
        plan_data[plan]["dates"].append(date)
        plan_data[plan]["values"].append(value if value is not None else 0)
    
    # Create figure
    fig = go.Figure()
    
    # Line opacity for semi-transparency
    LINE_OPACITY = 0.7
    LINE_WIDTH = 1  # Thin lines
    
    # Add trace for each plan
    for plan in unique_plans:
        if plan in plan_data:
            # Sort by date
            sorted_pairs = sorted(zip(plan_data[plan]["dates"], plan_data[plan]["values"]))
            dates = [p[0] for p in sorted_pairs]
            values = [p[1] for p in sorted_pairs]
            
            base_color = color_map.get(plan, "#6B7280")
            line_color = hex_to_rgba(base_color, LINE_OPACITY)
            
            # Custom hover template with full date
            if format_type == "dollar":
                hover_template = (
                    f'<b>{plan}</b><br>'
                    f'Date: %{{x|%B %d, %Y}}<br>'
                    f'Value: $%{{y:,.2f}}'
                    f'<extra></extra>'
                )
            elif format_type == "percent":
                hover_template = (
                    f'<b>{plan}</b><br>'
                    f'Date: %{{x|%B %d, %Y}}<br>'
                    f'Value: %{{y:.2%}}'
                    f'<extra></extra>'
                )
            else:
                hover_template = (
                    f'<b>{plan}</b><br>'
                    f'Date: %{{x|%B %d, %Y}}<br>'
                    f'Value: %{{y:,.0f}}'
                    f'<extra></extra>'
                )
            
            fig.add_trace(
                go.Scatter(
                    x=dates,
                    y=values,
                    mode='lines',  # No markers, just lines
                    name=plan,
                    line=dict(
                        color=line_color,
                        width=LINE_WIDTH,
                        shape='linear'  # Sharp corners (not spline)
                    ),
                    hovertemplate=hover_template,
                    showlegend=False,
                    connectgaps=False  # Don't connect gaps in data
                )
            )
    
    # Y-axis formatting
    if format_type == "dollar":
        yaxis_tickprefix = "$"
        yaxis_tickformat = ",.2f"
    elif format_type == "percent":
        yaxis_tickprefix = ""
        yaxis_tickformat = ".1%"
    else:
        yaxis_tickprefix = ""
        yaxis_tickformat = ",d"
    
    # X-axis range (full selected range)
    xaxis_range = None
    if date_range:
        xaxis_range = [date_range[0], date_range[1]]
    
    # Update layout
    fig.update_layout(
        height=350,
        margin=dict(l=60, r=20, t=20, b=50),
        hovermode="x unified",
        paper_bgcolor=colors["card_bg"],
        plot_bgcolor=colors["card_bg"],
        font=dict(
            family="Inter, sans-serif",
            size=12,
            color=colors["text_primary"]
        ),
        xaxis=dict(
            gridcolor=colors["border"],
            linecolor=colors["border"],
            tickfont=dict(color=colors["text_secondary"]),
            tickformat="%b %Y",  # Month Year format (Jan 2024)
            range=xaxis_range,
            fixedrange=False  # Allow zoom on x-axis
        ),
        yaxis=dict(
            gridcolor=colors["border"],
            linecolor=colors["border"],
            tickfont=dict(color=colors["text_secondary"]),
            tickprefix=yaxis_tickprefix,
            tickformat=yaxis_tickformat,
            fixedrange=False  # Allow zoom on y-axis
        ),
        legend=dict(
            font=dict(color=colors["text_primary"]),
            bgcolor="rgba(0,0,0,0)"
        ),
        # Enable drag modes for zoom/pan
        dragmode="zoom"  # Default to zoom mode
    )
    
    return fig, unique_plans


def get_chart_config():
    """Get Plotly chart configuration with zoom, pan, and download enabled"""
    return {
        'displayModeBar': True,
        'displaylogo': False,
        'modeBarButtonsToAdd': ['downloadCsv'],
        'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
        'toImageButtonOptions': {
            'format': 'png',
            'filename': 'chart',
            'height': 500,
            'width': 800,
            'scale': 2
        },
        'scrollZoom': False  # DISABLED - prevents scroll hijacking
    }


def create_empty_chart(title="No Data", theme="dark"):
    """Create an empty placeholder chart"""
    colors = get_theme_colors(theme)
    
    fig = go.Figure()
    fig.update_layout(
        height=350,
        paper_bgcolor=colors["card_bg"],
        plot_bgcolor=colors["card_bg"],
        font=dict(family="Inter, sans-serif", size=12, color=colors["text_primary"]),
        title=dict(text=title, x=0.5, font=dict(size=14)),
        annotations=[{
            "text": "Load data to see chart",
            "xref": "paper",
            "yref": "paper",
            "x": 0.5,
            "y": 0.5,
            "showarrow": False,
            "font": {"size": 14, "color": colors["text_secondary"]}
        }]
    )
    return fig


def get_legend_html(plans, color_map):
    """
    Generate legend HTML for Taipy
    Returns a list of dicts for rendering in a table/list
    """
    legend_items = []
    for plan in plans:
        color = color_map.get(plan, "#6B7280")
        legend_items.append({
            "plan": plan,
            "color": color
        })
    return legend_items
