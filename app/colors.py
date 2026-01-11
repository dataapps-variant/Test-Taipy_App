"""
Color System for Variant Analytics Dashboard
- App-based colors (14 apps)
- Shade generation for multiple plans per app
- Color map building for charts
"""

from app.config import APP_COLORS


def hex_to_rgb(hex_color):
    """Convert hex color to RGB tuple"""
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))


def rgb_to_hex(rgb):
    """Convert RGB tuple to hex color"""
    return '#{:02x}{:02x}{:02x}'.format(
        max(0, min(255, int(rgb[0]))),
        max(0, min(255, int(rgb[1]))),
        max(0, min(255, int(rgb[2])))
    )


def lighten_color(hex_color, factor=0.2):
    """Lighten a hex color by a factor (0-1)"""
    rgb = hex_to_rgb(hex_color)
    new_rgb = tuple(c + (255 - c) * factor for c in rgb)
    return rgb_to_hex(new_rgb)


def darken_color(hex_color, factor=0.2):
    """Darken a hex color by a factor (0-1)"""
    rgb = hex_to_rgb(hex_color)
    new_rgb = tuple(c * (1 - factor) for c in rgb)
    return rgb_to_hex(new_rgb)


def get_app_from_plan(plan_name):
    """
    Extract App name from Plan name
    Handles special cases like CT-Non-JP and CT-JP
    """
    if plan_name is None:
        return "Unknown"
    
    plan_upper = plan_name.upper()
    
    # Check for special CT cases first
    if "CT" in plan_upper:
        if "NON-JP" in plan_upper or "NONJP" in plan_upper:
            return "CT-Non-JP"
        elif "JP" in plan_upper and "NON" not in plan_upper:
            return "CT-JP"
        # Default CT handling
        if plan_name.startswith("CT"):
            return "CT-Non-JP"
    
    # Extract first 2 characters as App prefix
    if len(plan_name) >= 2:
        prefix = plan_name[:2].upper()
        if prefix in APP_COLORS:
            return prefix
    
    return "Unknown"


def get_plan_color(plan_name, plan_index_in_app=0):
    """
    Get color for a specific plan based on its App and position
    
    Args:
        plan_name: The plan name (e.g., "JF2788ST")
        plan_index_in_app: Index of this plan within its App group (for shade variation)
    
    Returns:
        Hex color string
    """
    app_name = get_app_from_plan(plan_name)
    base_color = APP_COLORS.get(app_name, "#6B7280")  # Default gray if unknown
    
    # Generate shade based on index
    shade_pattern = [
        0,      # Base color
        0.15,   # Lighter 15%
        -0.15,  # Darker 15%
        0.30,   # Lighter 30%
        -0.30,  # Darker 30%
        0.45,   # Lighter 45%
        -0.45,  # Darker 45%
        0.20,   # Lighter 20%
        -0.20,  # Darker 20%
        0.35,   # Lighter 35%
    ]
    
    shade_index = plan_index_in_app % len(shade_pattern)
    shade_value = shade_pattern[shade_index]
    
    if shade_value > 0:
        return lighten_color(base_color, shade_value)
    elif shade_value < 0:
        return darken_color(base_color, abs(shade_value))
    else:
        return base_color


def build_plan_color_map(plans):
    """
    Build a color map for a list of plans
    Groups plans by App and assigns shades
    
    Args:
        plans: List of plan names
    
    Returns:
        Dictionary mapping plan_name -> hex_color
    """
    # Group plans by App
    app_plans = {}
    for plan in plans:
        app = get_app_from_plan(plan)
        if app not in app_plans:
            app_plans[app] = []
        app_plans[app].append(plan)
    
    # Sort plans within each App for consistency
    for app in app_plans:
        app_plans[app].sort()
    
    # Build color map
    color_map = {}
    for app, plan_list in app_plans.items():
        for idx, plan in enumerate(plan_list):
            color_map[plan] = get_plan_color(plan, idx)
    
    return color_map


def get_chart_colors(plans):
    """
    Get ordered list of colors for a list of plans (for Plotly charts)
    
    Args:
        plans: List of plan names in order
    
    Returns:
        List of hex colors in same order
    """
    color_map = build_plan_color_map(plans)
    return [color_map.get(plan, "#6B7280") for plan in plans]


# Fallback colors for unknown apps
FALLBACK_COLORS = [
    "#6B7280",  # Gray
    "#9CA3AF",  # Light gray
    "#4B5563",  # Dark gray
    "#D1D5DB",  # Lighter gray
    "#374151",  # Darker gray
]


def get_fallback_color(index):
    """Get a fallback color by index"""
    return FALLBACK_COLORS[index % len(FALLBACK_COLORS)]
