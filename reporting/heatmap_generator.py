"""
Heatmap Generator for Automated Inventory Reporting

This module extracts and reuses the exact heatmap generation logic from the dashboard.
It produces identical pivot tables, aggregations, and styling as the dashboard.

IMPORTANT: This module copies pure functions from the dashboard to avoid Streamlit dependencies.
No Streamlit code executes in this module.
"""

import pandas as pd
import numpy as np
import os
from typing import Tuple, Optional

# Import safe pure function from comparison_tab
try:
    from comparison_tab import calculate_free_for_sale
except ImportError:
    # If import fails, we'll need to copy the function
    calculate_free_for_sale = None

from reporting.logger import get_logger

logger = get_logger(__name__)

# Import dataframe-image for PNG conversion
try:
    import dataframe_image as dfi
except ImportError:
    dfi = None
    logger.warning("dataframe-image not installed. PNG export will not work.")

# Import config for directory paths
try:
    from reporting.config import REPORTS_DIR, HEATMAP_IMAGE_PREFIX, HEATMAP_IMAGE_EXTENSION
except ImportError:
    # Fallback if config not available
    REPORTS_DIR = "reports"
    HEATMAP_IMAGE_PREFIX = "temp_heatmap_"
    HEATMAP_IMAGE_EXTENSION = ".png"

# ============================================================================
# Constants (copied from dashboard)
# ============================================================================

OD_ORDER = [
    '1/8"', '1/4"', '3/8"', '1/2"', '3/4"', '1"', '1-1/4"', '1-1/2"',
    '2"', '2-1/2"', '3"', '3-1/2"', '4"', '5"', '6"', '8"', '10"', '12"',
    '14"', '16"', '18"', '20"', '22"', '24"', '26"', '28"', '30"', '32"',
    '34"', '36"', '38"', '40"', '42"', '44"', '46"', '48"', '52"', '56"',
    '60"', '64"', '68"', '72"', '76"', '80"', 'Non Standard OD', 'Non STD', 'Unknown OD'
]

CS_AS_WT = [
    "SCH 10", "SCH 20", "SCH 30", "STD", "SCH 40", "SCH 60", "XS", "SCH 80",
    "SCH 100", "SCH 120", "SCH 140", "SCH 160", "SCH XXS", "Non STD"
]

SS_WT = [
    "Schedule 5S", "Schedule 10S", "Schedule 40S", "Schedule 80S", "Schedule 160S", "XXS", "Non STD"
]

IS_WT = [
    "IS 1239: Light (A-Class)", "IS 1239: Medium (B-Class)", "IS 1239: Heavy (C-Class)",
    '7" NB', '8" NB', '10" NB', '12" NB', '14" NB', '16" NB', '18" NB', '20" NB', "Non IS Standard"
]

TUBES_WT = [
    "Small Wall Tube", "Medium Wall Tube", "Heavy Wall Tube", "Non-Standard Tube"
]

# ============================================================================
# Specification Mapping (copied from dashboard logic)
# ============================================================================

def load_specification_mapping():
    """Load specification to grade type mapping from Excel file"""
    try:
        mapping_file = 'Spec_mapping.xlsx'
        if os.path.exists(mapping_file):
            mapping_df = pd.read_excel(mapping_file)
            spec_to_grade = dict(zip(mapping_df['Specification'], mapping_df['Grade Type']))
            return spec_to_grade
        else:
            logger.warning(f"Specification mapping file not found: {mapping_file}. Using fallback logic.")
            return {}
    except Exception as e:
        logger.warning(f"Error loading specification mapping: {e}. Using fallback logic.")
        return {}

# Load mapping once at module level
SPECIFICATION_MAPPING = load_specification_mapping()

# ============================================================================
# Grade Derivation Functions (copied from dashboard)
# ============================================================================

def derive_grade_from_spec(spec, combine_cs_as=False):
    """
    Consolidated function to derive Grade Type from Specification.
    Copied from dashboard - exact same logic.
    """
    if pd.isna(spec):
        return "Unknown"
    
    spec_str = str(spec).strip()
    
    # First try to get from mapping
    if spec_str in SPECIFICATION_MAPPING:
        grade_type = SPECIFICATION_MAPPING[spec_str]
        if combine_cs_as and grade_type in ["AS", "CS"]:
            return "CS & AS"
        elif combine_cs_as and grade_type == "TUBES":
            return "Tubes"
        else:
            return grade_type
    
    # Fallback to pattern-based derivation
    spec_upper = spec_str.upper()
    
    # Check for IS specifications (contains IS in middle, like: CSEWPIS1239PT1)
    if "IS" in spec_upper and not spec_upper.startswith("IS"):
        return "IS"
    
    # Check for Tube specifications (contains T in middle, like: CSSMT2391ST52)
    if "T" in spec_upper and not spec_upper.startswith("T"):
        # More specific check for tube patterns
        if any(pattern in spec_upper for pattern in ["TUBE", "TUB", "ST52", "ST42"]):
            return "Tubes"
    
    # Check starting patterns
    if spec_upper.startswith("AS"):
        return "CS & AS" if combine_cs_as else "AS"
    elif spec_upper.startswith("CS"):
        return "CS & AS" if combine_cs_as else "CS"
    elif spec_upper.startswith("SS"):
        return "SS"
    elif spec_upper.startswith("IS"):
        return "IS"
    elif spec_upper.startswith("T"):
        return "Tubes"
    
    # Default fallback
    return "Unknown"

def derive_grade_type_from_spec(specification):
    """Derive Grade Type from Specification name using mapping or fallback logic"""
    return derive_grade_from_spec(specification, combine_cs_as=True)

# ============================================================================
# OD Categorization Functions (copied from dashboard)
# ============================================================================

def categorize_OD_CS_AS(od):
    """Categorize OD for CS/AS grade types"""
    od_map = {
        10.3: '1/8"', 13.7: '1/4"', 17.1: '3/8"', 21.3: '1/2"', 26.7: '3/4"', 33.4: '1"',
        42.2: '1-1/4"', 48.3: '1-1/2"', 60.3: '2"', 73.0: '2-1/2"', 88.9: '3"', 101.6: '3-1/2"',
        114.3: '4"', 141.3: '5"', 168.3: '6"', 219.1: '8"', 273.0: '10"', 273.1: '10"',
        323.8: '12"', 355.6: '14"', 406.4: '16"', 457.0: '18"', 457.2: '18"', 508.0: '20"',
        559.0: '22"', 609.6: '24"', 610.0: '24"', 660.0: '26"', 660.4: '26"', 711.0: '28"',
        711.2: '28"', 762.0: '30"', 813.0: '32"', 812.8: '32"', 864.0: '34"', 863.6: '34"',
        914.0: '36"', 914.4: '36"', 965.0: '38"', 965.2: '38"', 1016.0: '40"', 1066.0: '42"',
        1066.8: '42"', 1067.0: '42"', 1118.0: '44"', 1117.6: '44"', 1168.0: '46"', 1168.4: '46"',
        1219.0: '48"', 1219.2: '48"', 1321.0: '52"', 1422.0: '56"', 1524.0: '60"', 1626.0: '64"',
        1727.0: '68"', 1829.0: '72"', 1930.0: '76"', 2032.0: '80"'
    }
    try:
        od = float(od)
        return od_map.get(od, "Non Standard OD")
    except:
        return "Non Standard OD"

def categorize_OD_SS(od):
    """Categorize OD for SS grade types - same as CS_AS"""
    return categorize_OD_CS_AS(od)

def categorize_OD_IS(od):
    """Categorize OD for IS grade types"""
    od_map = {
        10.32: '1/8"', 13.49: '1/4"', 17.10: '3/8"', 21.30: '1/2"', 21.43: '1/2"',
        26.90: '3/4"', 27.20: '3/4"', 33.70: '1"', 33.80: '1"', 42.90: '1-1/4"',
        48.40: '1-1/2"', 48.30: '1-1/2"', 60.30: '2"', 76.10: '2-1/2"', 76.20: '2-1/2"',
        88.90: '3"', 114.30: '4"', 139.70: '5"', 165.10: '6"'
    }
    try:
        od = float(od)
        return od_map.get(od, "Non Standard OD")
    except:
        return "Non Standard OD"

def categorize_OD_Tube(od):
    """Categorize OD for Tube grade types"""
    od_map = {
        6.35: '1/4"', 9.53: '3/8"', 12.70: '1/2"', 15.88: '5/8"', 19.05: '3/4"',
        22.23: '7/8"', 25.40: '1"', 31.75: '1-1/4"', 38.10: '1-1/2"', 50.80: '2"',
        63.50: '2-1/2"', 76.20: '3"', 101.60: '4"'
    }
    try:
        od = float(od)
        return od_map.get(od, "Unknown OD")
    except:
        return "Unknown OD"

def categorize_OD(od, grade):
    """Main OD categorization function"""
    if pd.isna(grade):
        return "Unknown Grade"
    grade_clean = str(grade).strip().lower()
    if "is" in grade_clean:
        return categorize_OD_IS(od)
    elif "tube" in grade_clean:
        return categorize_OD_Tube(od)
    elif "ss" in grade_clean or "stainless" in grade_clean:
        return categorize_OD_SS(od)
    else:
        return categorize_OD_CS_AS(od)

# ============================================================================
# WT Schedule Categorization Functions (copied from dashboard)
# ============================================================================

def categorize_carbon(od, wt):
    """Categorize WT schedule for Carbon/CS/AS grade types"""
    try:
        od = float(od)
        wt = float(wt)
    except:
        return "Non STD"
    
    # STD (Standard Weight) - Same as SCH 40 for NPS 1/8" to NPS 10"
    for defined_od, defined_wt in [
        (10.3, 1.73), (13.7, 2.24), (17.1, 2.31), (21.3, 2.77), (26.7, 2.87), (33.4, 3.38),
        (42.2, 3.56), (48.3, 3.68), (60.3, 3.91), (73.0, 5.16), (88.9, 5.49), (101.6, 5.74),
        (114.3, 6.02), (141.3, 6.55), (168.3, 7.11), (219.1, 8.18), (273.0, 9.27), (273.1, 9.27),
        (323.8, 9.53), (355.6, 9.53), (406.4, 9.53), (457.0, 9.53), (457.2, 9.53), (508.0, 9.53),
        (559.0, 9.53), (558.8, 9.53), (610.0, 9.53), (609.6, 9.53), (660.4, 9.53), (711.2, 9.53),
        (711, 9.53), (762, 9.53), (812.8, 9.53), (863.6, 9.53), (914.4, 9.53), (914, 9.53),
        (965.2, 9.53), (1016, 9.53), (1066.8, 9.53), (1117.6, 9.53), (1168.4, 9.53), (1219.2, 9.53),
        (1219, 12.70), (1524, 12.70)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "STD"
    
    # XS (Extra Strong) - Same as SCH 80 for NPS 1/8" to NPS 8"
    for defined_od, defined_wt in [
        (10.3, 2.41), (13.7, 3.02), (17.1, 3.20), (21.3, 3.73), (26.7, 3.91), (33.4, 4.55),
        (42.2, 4.85), (48.3, 5.08), (60.3, 5.54), (73.0, 7.01), (88.9, 7.62), (101.6, 8.08),
        (114.3, 8.56), (141.3, 9.53), (168.3, 10.97), (219.1, 12.70), (273.0, 12.70), (273.1, 12.70),
        (323.8, 12.70), (355.6, 12.70), (406.4, 12.70), (457.0, 12.70), (508.0, 12.70), (559.0, 12.70),
        (610.0, 12.70), (609.6, 12.70), (660.4, 12.70), (711.2, 12.70), (762, 12.70), (812.8, 12.70),
        (863.6, 12.70), (914.4, 12.70), (914, 12.70), (965.2, 12.70), (1016, 12.70), (1066.8, 12.70),
        (1117.6, 12.70), (1168.4, 12.70), (1219.2, 12.70), (1219, 12.70), (1524, 12.70)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "XS"
    
    # XXS (Double Extra Strong)
    for defined_od, defined_wt in [
        (10.3, 4.83), (13.7, 6.05), (17.1, 6.40), (21.3, 7.47), (26.7, 7.82), (33.4, 9.09),
        (42.2, 9.70), (48.3, 10.15), (60.3, 11.07), (73.0, 14.02), (88.9, 15.24), (114.3, 17.12),
        (141.3, 19.05), (168.3, 21.95), (219.1, 22.23), (273.0, 25.40), (273.1, 25.40), (323.8, 25.40)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "SCH XXS"
    
    # SCH 10
    for defined_od, defined_wt in [
        (10.3, 1.24), (13.7, 1.65), (17.1, 1.65), (21.3, 2.11), (26.7, 2.11), (33.4, 2.77),
        (42.2, 2.77), (48.3, 2.77), (60.3, 2.77), (73.0, 3.05), (88.9, 3.05), (101.6, 3.05),
        (114.3, 3.05), (141.3, 3.40), (168.3, 3.40), (219.1, 3.76), (273.0, 4.19), (273.1, 4.19),
        (323.8, 4.57), (355.6, 6.35), (406.4, 6.35), (457.0, 6.35), (508.0, 6.35), (559.0, 6.35),
        (610.0, 6.35), (609.6, 6.35)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "SCH 10"
    
    # SCH 20
    for defined_od, defined_wt in [
        (219.1, 6.35), (273.0, 6.35), (273.1, 6.35), (323.8, 6.35), (323.8, 7.1),
        (355.6, 7.92), (406.4, 7.92), (457.0, 7.92), (508.0, 9.53), (559.0, 9.53),
        (610.0, 9.53), (609.6, 9.53)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "SCH 20"
    
    # SCH 30
    for defined_od, defined_wt in [
        (21.3, 2.41), (26.7, 2.41), (33.4, 2.90), (42.2, 2.97), (48.3, 3.18), (60.3, 3.18),
        (73.0, 4.78), (88.9, 4.78), (101.6, 4.78), (114.3, 4.78), (219.1, 7.04), (273.0, 7.80),
        (273.1, 7.80), (323.8, 8.38), (355.6, 9.53), (406.4, 9.53), (457.0, 11.13), (508.0, 12.70),
        (559.0, 12.70), (610.0, 14.27), (609.6, 14.27)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "SCH 30"
    
    # SCH 40 - Same as STD for NPS 1/8" to NPS 10"
    for defined_od, defined_wt in [
        (10.3, 1.73), (13.7, 2.24), (17.1, 2.31), (21.3, 2.77), (26.7, 2.87), (33.4, 3.38),
        (42.2, 3.56), (48.3, 3.68), (60.3, 3.91), (73.0, 5.16), (88.9, 5.49), (101.6, 5.74),
        (114.3, 6.02), (141.3, 6.55), (168.3, 7.11), (219.1, 8.18), (273.0, 9.27), (273.1, 9.27),
        (323.8, 10.31), (355.6, 11.13), (355.6, 14.3), (406.4, 12.70), (457.0, 14.27), (508.0, 15.09),
        (610.0, 17.48), (609.6, 17.48)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "SCH 40"
    
    # SCH 60
    for defined_od, defined_wt in [
        (219.1, 10.31), (273.0, 12.70), (273.1, 12.70), (323.8, 14.27), (355.6, 15.09),
        (406.4, 16.66), (457.0, 19.05), (457.0, 22.23), (508.0, 20.62), (559.0, 22.23),
        (610.0, 24.61), (609.6, 24.61)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "SCH 60"
    
    # SCH 80 - Same as XS for NPS 1/8" to NPS 8"
    for defined_od, defined_wt in [
        (10.3, 2.41), (13.7, 3.02), (17.1, 3.20), (21.3, 3.73), (26.7, 3.91), (33.4, 4.55),
        (42.2, 4.85), (48.3, 5.08), (60.3, 5.54), (73.0, 7.01), (88.9, 7.62), (101.6, 8.08),
        (114.3, 8.56), (141.3, 9.53), (168.3, 10.97), (219.1, 12.70), (273.0, 15.09), (273.1, 15.09),
        (323.8, 17.48), (355.6, 19.05), (406.4, 21.44), (457.0, 23.83), (508.0, 26.19),
        (559.0, 28.58), (610.0, 30.96), (609.6, 30.96)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "SCH 80"
    
    # SCH 100
    for defined_od, defined_wt in [
        (219.1, 15.09), (273.0, 18.26), (273.1, 18.26), (323.8, 21.44), (355.6, 23.83),
        (406.4, 26.19), (457.0, 29.36), (508.0, 32.54), (559.0, 34.93), (610.0, 38.89), (609.6, 38.89)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "SCH 100"
    
    # SCH 120
    for defined_od, defined_wt in [
        (114.3, 11.13), (141.3, 12.70), (168.3, 14.27), (219.1, 18.26), (273.0, 21.44),
        (273.1, 21.44), (323.8, 25.40), (355.6, 27.79), (406.4, 30.96), (457.0, 34.93),
        (508.0, 38.10), (559.0, 41.28), (610.0, 46.02), (609.6, 46.02)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "SCH 120"
    
    # SCH 140
    for defined_od, defined_wt in [
        (219.1, 20.62), (273.0, 25.40), (273.1, 25.40), (323.8, 28.58), (355.6, 31.75),
        (406.4, 36.53), (457.0, 39.67), (508.0, 44.45), (559.0, 47.63), (610.0, 52.37), (609.6, 52.37)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "SCH 140"
    
    # SCH 160
    for defined_od, defined_wt in [
        (21.3, 4.78), (26.7, 5.56), (33.4, 6.35), (42.2, 6.35), (48.3, 7.14), (60.3, 8.74),
        (73.0, 9.53), (88.9, 11.13), (114.3, 13.49), (141.3, 15.88), (168.3, 18.26), (219.1, 23.01),
        (273.0, 28.58), (273.1, 28.58), (273.1, 32), (323.8, 33.32), (355.6, 35.71), (406.4, 40.49),
        (457.0, 45.24), (508.0, 50.01), (559.0, 53.98), (610.0, 59.54), (609.6, 59.54)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "SCH 160"
    
    return "Non STD"

def categorize_stainless(od, wt):
    """Categorize WT schedule for Stainless Steel grade types"""
    try:
        od = float(od)
        wt = float(wt)
    except:
        return "Non STD"
    
    # Schedule 5S
    for defined_od, defined_wt in [
        (10.3, 1.24), (13.7, 1.65), (17.1, 1.65), (21.3, 1.65), (26.7, 1.65), (33.4, 2.11),
        (42.2, 2.11), (48.3, 2.11), (60.3, 2.77), (73.0, 2.77), (88.9, 2.77), (114.3, 2.77),
        (141.3, 3.40), (168.3, 3.40), (219.1, 3.76), (273.0, 4.19), (323.8, 4.57), (355.6, 4.78),
        (406.4, 4.78), (457.0, 4.78), (508.0, 5.54), (610.0, 6.35), (609.6, 6.35)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "Schedule 5S"
    
    # Schedule 10S
    for defined_od, defined_wt in [
        (10.3, 1.24), (13.7, 1.65), (17.1, 1.65), (21.3, 2.11), (26.7, 2.11), (33.4, 2.77),
        (42.2, 2.77), (48.3, 2.77), (60.3, 2.77), (73.0, 3.05), (88.9, 3.05), (114.3, 3.05),
        (141.3, 3.40), (168.3, 3.40), (219.1, 3.76), (273.0, 4.19), (323.8, 4.57), (355.6, 4.78),
        (406.4, 4.78), (457.0, 4.78), (508.0, 5.54), (610.0, 6.35), (609.6, 6.35)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "Schedule 10S"
    
    # Schedule 40S
    for defined_od, defined_wt in [
        (10.3, 1.73), (13.7, 2.24), (17.1, 2.31), (21.3, 2.77), (26.7, 2.87), (33.4, 3.38),
        (42.2, 3.56), (48.3, 3.68), (60.3, 3.91), (73.0, 5.16), (88.9, 5.49), (101.6, 5.74),
        (114.3, 6.02), (141.3, 6.55), (168.3, 7.11), (219.1, 8.18), (273.0, 9.27), (323.8, 9.53),
        (355.6, 9.53), (406.4, 9.53), (457.0, 9.53), (508.0, 9.53), (610.0, 9.53), (609.6, 9.53)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "Schedule 40S"
    
    # Schedule 80S
    for defined_od, defined_wt in [
        (10.3, 2.41), (13.7, 3.02), (17.1, 3.20), (21.3, 3.73), (26.7, 3.91), (33.4, 4.55),
        (42.2, 4.85), (48.3, 5.08), (60.3, 5.54), (73.0, 7.01), (88.9, 7.62), (101.6, 8.08),
        (114.3, 8.56), (141.3, 9.53), (168.3, 10.97), (219.1, 12.70), (273.0, 15.09), (273.1, 15.09),
        (323.8, 17.48), (355.6, 19.05), (406.4, 21.44), (406.4, 25.4), (457.0, 23.83), (508.0, 26.19),
        (559.0, 28.58), (610.0, 30.96), (609.6, 30.96)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "Schedule 80S"
    
    # Schedule 160S
    for defined_od, defined_wt in [
        (21.3, 4.78), (26.7, 5.56), (33.4, 6.35), (42.2, 6.35), (48.3, 7.14), (60.3, 8.74),
        (73.0, 9.53), (88.9, 11.13), (114.3, 13.49), (141.3, 15.88), (168.3, 18.26), (219.1, 23.01),
        (273.0, 28.58), (323.8, 33.32), (355.6, 35.71), (406.4, 40.49), (457.0, 45.24), (508.0, 50.01),
        (559.0, 53.98), (610.0, 59.54), (609.6, 59.54)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "Schedule 160S"
    
    # XXS (Double Extra Strong)
    for defined_od, defined_wt in [
        (10.3, 4.83), (13.7, 6.05), (17.1, 6.40), (21.3, 7.47), (26.7, 7.82), (33.4, 9.09),
        (42.2, 9.70), (48.3, 10.15), (60.3, 11.07), (73.0, 14.02), (88.9, 15.24), (114.3, 17.12),
        (141.3, 19.05), (168.3, 21.95), (219.1, 22.23), (273.0, 25.40), (323.8, 25.40)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "SCH XXS"
    
    return "Non STD"

def categorize_is(od, wt):
    """Categorize WT schedule for IS grade types"""
    try:
        od = float(od)
        wt = float(wt)
    except:
        return "Non IS Standard"
    
    # Light (A-Class)
    for defined_od, defined_wt in [
        (10.32, 1.80), (13.49, 1.80), (17.10, 1.80), (21.3, 2.00), (21.43, 2.00), (27.20, 2.35),
        (33.70, 2.65), (33.80, 2.65), (42.90, 2.65), (48.40, 2.90), (48.30, 2.90), (60.30, 2.90),
        (76.20, 3.25), (88.90, 3.25), (114.30, 3.65)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "IS 1239: Light (A-Class)"
    
    # Medium (B-Class)
    for defined_od, defined_wt in [
        (10.32, 2.00), (13.49, 2.35), (17.10, 2.35), (21.3, 2.65), (21.43, 2.65), (27.20, 2.65),
        (33.80, 3.25), (33.70, 3.25), (42.90, 3.25), (48.40, 3.25), (48.30, 3.25), (60.30, 3.65),
        (76.20, 3.65), (76.10, 3.60), (88.90, 4.05), (114.30, 4.50), (139.70, 4.85), (165.10, 4.85)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "IS 1239: Medium (B-Class)"
    
    # Heavy (C-Class)
    for defined_od, defined_wt in [
        (10.32, 2.65), (13.49, 2.90), (17.10, 2.90), (21.43, 3.25), (27.20, 3.25), (33.80, 4.05),
        (33.70, 4), (21.3, 3.2), (42.90, 4.05), (48.40, 4.05), (48.30, 4.05), (60.30, 4.47),
        (76.20, 4.47), (76.10, 4.50), (88.90, 4.85), (114.30, 5.40), (139.70, 5.40), (165.10, 5.40)
    ]:
        if abs(od - defined_od) <= 1.0 and abs(wt - defined_wt) <= 0.2:
            return "IS 1239: Heavy (C-Class)"
    
    return "Non IS Standard"

def categorize_WT_Tube(od, wt):
    """Categorize WT schedule for Tube grade types"""
    try:
        od = float(od)
        wt = float(wt)
    except:
        return "Non-Standard Tube"
    
    # Light wall tubes
    if (od, wt) in [
        (6.35, 0.71), (6.35, 0.89), (9.53, 0.89), (9.53, 1.24), (12.70, 0.89), (12.70, 1.24),
        (15.88, 0.89), (15.88, 1.24), (15.88, 1.65), (19.05, 0.89), (19.05, 1.24), (19.05, 1.65),
        (22.23, 1.24), (22.23, 1.65), (25.40, 1.24), (25.40, 1.65), (31.75, 1.24), (31.75, 1.65),
        (31.75, 2.11), (38.10, 1.65), (38.10, 2.11), (50.80, 1.65), (50.80, 2.11), (50.80, 2.77),
        (63.50, 1.65), (63.50, 2.11), (63.50, 2.77), (76.20, 1.65), (76.20, 2.11), (76.20, 2.77),
        (101.60, 2.11), (101.60, 2.77)
    ]:
        return "Small Wall Tube"
    
    # Medium wall tubes
    if (od, wt) in [
        (6.35, 1.24), (9.53, 1.65), (12.70, 1.65), (15.88, 2.11), (19.05, 2.11), (22.23, 2.11),
        (25.40, 2.11), (31.75, 2.77), (38.10, 2.77), (50.80, 3.05), (63.50, 3.05), (76.20, 3.05),
        (101.60, 3.05)
    ]:
        return "Medium Wall Tube"
    
    # Heavy wall tubes
    if (od, wt) in [
        (6.35, 1.65), (9.53, 2.11), (12.70, 2.11), (15.88, 2.77), (19.05, 2.77), (22.23, 2.77),
        (25.40, 2.77), (31.75, 3.05), (38.10, 3.05), (50.80, 3.40), (63.50, 3.40), (76.20, 3.40),
        (101.60, 3.40)
    ]:
        return "Heavy Wall Tube"
    
    # Extra heavy wall tubes
    if (od, wt) in [
        (15.88, 3.05), (19.05, 3.05), (22.23, 3.05), (25.40, 3.05), (31.75, 3.40), (38.10, 3.40),
        (50.80, 3.73), (63.50, 3.73), (76.20, 3.73), (101.60, 4.78)
    ]:
        return "Non-Standard Tube"
    
    return "Non-Standard Tube"

def categorize_WT_schedule(od, wt, grade):
    """Main WT schedule categorization function"""
    if pd.isna(grade):
        return "Unknown"
    grade_clean = str(grade).strip().lower()
    if "tube" in grade_clean:
        return categorize_WT_Tube(od, wt)
    elif "is" in grade_clean:
        return categorize_is(od, wt)
    elif "cs" in grade_clean or "carbon" in grade_clean or "as" in grade_clean or "alloy" in grade_clean:
        return categorize_carbon(od, wt)
    elif "ss" in grade_clean or "stainless" in grade_clean:
        return categorize_stainless(od, wt)
    else:
        return "Unknown"

# ============================================================================
# Data Categorization Function (copied from dashboard)
# ============================================================================

def add_categorizations(df):
    """
    Add OD_Category and WT_Schedule columns to DataFrame.
    Copied from dashboard - exact same logic.
    """
    try:
        if df.empty:
            return df
        
        # Add OD_Category and WT_Schedule columns
        # Use Grade_Logic if available, otherwise fall back to Grade
        grade_col = 'Grade_Logic' if 'Grade_Logic' in df.columns else 'Grade'
        
        if 'OD' in df.columns and grade_col in df.columns:
            # Vectorized OD categorization
            od_values = df['OD'].values
            grade_values = df[grade_col].values
            
            od_categories = []
            for i in range(len(df)):
                od_categories.append(categorize_OD(od_values[i], grade_values[i]))
            df['OD_Category'] = od_categories
        else:
            df['OD_Category'] = "Unknown"
        
        if 'OD' in df.columns and 'WT' in df.columns and grade_col in df.columns:
            # Vectorized WT categorization
            od_values = df['OD'].values
            wt_values = df['WT'].values
            grade_values = df[grade_col].values
            
            wt_schedules = []
            for i in range(len(df)):
                wt_schedules.append(categorize_WT_schedule(od_values[i], wt_values[i], grade_values[i]))
            df['WT_Schedule'] = wt_schedules
        else:
            df['WT_Schedule'] = "Unknown"
        
        return df
    except (KeyError, ValueError, AttributeError, IndexError) as e:
        # If categorization fails, return DataFrame with Unknown categories
        if 'OD_Category' not in df.columns:
            df['OD_Category'] = "Unknown"
        if 'WT_Schedule' not in df.columns:
            df['WT_Schedule'] = "Unknown"
        return df
    except Exception as e:
        # Unexpected error - return DataFrame with Unknown categories
        if 'OD_Category' not in df.columns:
            df['OD_Category'] = "Unknown"
        if 'WT_Schedule' not in df.columns:
            df['WT_Schedule'] = "Unknown"
        return df

# ============================================================================
# Highlight Function (copied from dashboard)
# ============================================================================

def highlight(val, minval, maxval, numeric_no_totals, size_chart_type="Free For Sale"):
    """
    Apply conditional formatting to heatmap cells.
    Copied from dashboard - exact same color logic.
    """
    if pd.isna(val) or val == 0:
        return "background-color: #FFFFFF; color: #CCCCCC;"
    
    # Handle negative values with red coloring
    if val < 0:
        if size_chart_type == "Compare Files":
            red_colors = [
                "#FFF0F0", "#FFE0E0", "#FFC1C1", "#FFA3A3", "#FF8585",
                "#FF6666", "#D14848", "#8B2E2E", "#601F1F", "#5A2E2E"
            ]
            negative_vals = numeric_no_totals[numeric_no_totals < 0]
            if not negative_vals.empty:
                neg_min = negative_vals.min().min()
                neg_max = negative_vals.max().max()
                if neg_min < neg_max:
                    idx = int((val - neg_max) / (neg_min - neg_max) * (len(red_colors) - 1))
                else:
                    idx = len(red_colors) - 1
            else:
                idx = 0
            idx = max(0, min(idx, len(red_colors) - 1))
            text_color = "#FFFFFF" if idx >= 7 else "#222"
            return f"background-color: {red_colors[idx]}; color: {text_color}; font-weight: bold;"
        else:
            return "background-color: #DC143C; color: #FFFFFF; font-weight: bold;"
    
    # Greens scale (10 steps) - Lighter darkest green for better readability
    colors = [
        "#F0FFF0", "#E0FFE0", "#C1FFC1", "#A3FFA3", "#85FF85", "#66FF66",
        "#48D148", "#2E8B2E", "#1F601F", "#2E5A2E"
    ]
    
    # Normalize - use only positive values for scaling to ensure consistent light green
    positive_vals = numeric_no_totals[numeric_no_totals > 0]
    if not positive_vals.empty:
        pos_minval = positive_vals.min().min()
        pos_maxval = positive_vals.max().max()
        if pos_maxval > pos_minval:
            # Scale based on positive values only
            idx = int((val - pos_minval) / (pos_maxval - pos_minval) * (len(colors) - 1))
            idx = max(0, min(idx, len(colors) - 1))  # Ensure idx is within bounds
        else:
            idx = 0  # Use lightest green for single positive value
    else:
        idx = 0  # Use lightest green if no positive values
    
    # Use white text for darker backgrounds (last 3 colors)
    if idx >= 7:
        text_color = "#FFFFFF"
    else:
        text_color = "#222"
    
    return f"background-color: {colors[idx]}; color: {text_color}; font-weight: bold;"

# ============================================================================
# Main Heatmap Generation Function
# ============================================================================

def generate_heatmap_dataframe(
    stock_df: pd.DataFrame,
    reservations_df: pd.DataFrame,
    incoming_df: pd.DataFrame,
    specification: str,
    metric: str = "Free For Sale"
) -> Tuple[Optional[pd.io.formats.style.Styler], Optional[dict], Optional[str]]:
    """
    Generate heatmap DataFrame for a specific specification.
    
    This function replicates the EXACT logic from the dashboard heatmap generation.
    It produces identical pivot tables, aggregations, and styling.
    
    Args:
        stock_df: DataFrame from Stock sheet
        reservations_df: DataFrame from Reservations sheet
        incoming_df: DataFrame from Incoming sheet
        specification: Specification name to filter by (e.g., "CSSMP106B")
        metric: Metric to display in heatmap (default: "Free For Sale")
    
    Returns:
        Tuple of (styled_dataframe, metrics_dict, error_message)
        - styled_dataframe: pandas Styler object ready for image export (or None if error)
        - metrics_dict: Dictionary with summary metrics (Stock, Reservation, Incoming, Free For Sale)
        - error_message: Error message string (or None if successful)
    
    Raises:
        ValueError: If required columns are missing
        KeyError: If specification not found in data
    """
    try:
        # Validate inputs
        if not specification or pd.isna(specification):
            raise ValueError("Specification cannot be empty")
        
        # Check if calculate_free_for_sale is available
        if calculate_free_for_sale is None:
            raise ImportError("calculate_free_for_sale function not available from comparison_tab")
        
        # Step 1: Calculate Free For Sale (reuse existing function)
        logger.info(f"Calculating Free For Sale for specification: {specification}")
        free_for_sale_df = calculate_free_for_sale(stock_df, reservations_df, incoming_df)
        
        if free_for_sale_df.empty:
            raise ValueError(f"No data available for Free For Sale calculation")
        
        # Step 2: Filter by specification
        if 'Specification' not in free_for_sale_df.columns:
            raise ValueError("'Specification' column not found in Free For Sale data")
        
        # Filter by specification (exact match, case-sensitive)
        df_filtered = free_for_sale_df[free_for_sale_df['Specification'].str.strip() == specification.strip()].copy()
        
        if df_filtered.empty:
            raise ValueError(f"No data found for specification: {specification}")
        
        logger.info(f"Filtered {len(df_filtered)} rows for specification: {specification}")
        
        # Step 3: Add categorizations (OD_Category, WT_Schedule)
        # First, we need to derive Grade from specification
        grade_type = derive_grade_type_from_spec(specification)
        
        # Add Grade column if not present (needed for categorization)
        if 'Grade' not in df_filtered.columns and 'Grade_Logic' not in df_filtered.columns:
            df_filtered['Grade'] = grade_type
            df_filtered['Grade_Logic'] = grade_type
        
        # Add categorizations
        df_filtered = add_categorizations(df_filtered)
        
        # Validate required columns after categorization
        required_cols = ['OD_Category', 'WT_Schedule', 'MT']
        missing_cols = [col for col in required_cols if col not in df_filtered.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns after categorization: {missing_cols}")
        
        # Step 4: Determine WT schedule order based on grade type
        # This matches the dashboard logic for selecting appropriate WT schedules
        if grade_type == "CS & AS":
            wt_schedule = [s for s in CS_AS_WT if s in df_filtered['WT_Schedule'].unique()]
        elif grade_type == "SS":
            wt_schedule = [s for s in SS_WT if s in df_filtered['WT_Schedule'].unique()]
        elif grade_type == "IS":
            wt_schedule = [s for s in IS_WT if s in df_filtered['WT_Schedule'].unique()]
        elif grade_type == "Tubes":
            wt_schedule = [s for s in TUBES_WT if s in df_filtered['WT_Schedule'].unique()]
        else:
            # Unknown grade - use all available schedules
            wt_schedule = sorted(df_filtered['WT_Schedule'].unique().tolist())
        
        # Ensure we have at least some schedules
        if not wt_schedule:
            logger.warning(f"No WT schedules found for specification {specification}. Using all available.")
            wt_schedule = sorted(df_filtered['WT_Schedule'].unique().tolist())
        
        # Step 5: Create pivot table (same logic as dashboard)
        logger.info(f"Creating pivot table for specification: {specification}")
        
        # Build base DataFrame with all combinations
        import itertools
        base_index = pd.MultiIndex.from_product([OD_ORDER, wt_schedule], names=["OD_Category", "WT_Schedule"])
        df_base = pd.DataFrame(index=base_index).reset_index()
        
        # Group and sum - ensure metric column is numeric before summing
        try:
            # Convert MT column to numeric, handling any non-numeric values
            if 'MT' in df_filtered.columns:
                df_filtered['MT'] = pd.to_numeric(df_filtered['MT'], errors='coerce').fillna(0)
            grouped = df_filtered.groupby(["OD_Category", "WT_Schedule"])['MT'].sum().reset_index()
        except (ValueError, TypeError) as e:
            logger.error(f"Error grouping data: {e}")
            grouped = df_base.copy()
            grouped['MT'] = 0
        
        # Merge with base to ensure all combinations exist
        merged = pd.merge(df_base, grouped, on=["OD_Category", "WT_Schedule"], how="left").fillna(0)
        
        # Pivot
        pivot = merged.pivot(index="OD_Category", columns="WT_Schedule", values="MT")
        
        # Keep only the fixed order
        pivot = pivot.reindex(index=OD_ORDER, columns=wt_schedule, fill_value=0)
        
        # Remove all-zero rows except for totals (we'll add totals next)
        pivot = pivot.loc[~((pivot == 0).all(axis=1)) | (pivot.index == "Total")]
        
        # Add row totals
        pivot["Total"] = pivot.sum(axis=1)
        
        # Add column totals
        col_total = pivot.sum(axis=0)
        col_total.name = "Total"
        pivot = pd.concat([pivot, col_total.to_frame().T])
        
        # Format all numeric values to 2 decimals (same as dashboard)
        pivot = pivot.applymap(lambda x: round(x, 2) if isinstance(x, (int, float)) else x)
        
        # Step 6: Apply conditional formatting (same as dashboard)
        # Only color the numeric cells (not OD_Category)
        numeric = pivot.select_dtypes(include=[float, int])
        # Exclude the "Total" row and column for color calculation
        numeric_no_totals = numeric.drop('Total', axis=1, errors='ignore').drop('Total', axis=0, errors='ignore')
        minval = numeric_no_totals.min().min() if not numeric_no_totals.empty else 0
        maxval = numeric_no_totals.max().max() if not numeric_no_totals.empty else 1
        
        # Define OD categories to highlight with star marker (same as dashboard)
        highlight_od_categories = ['2"', '4"', '6"', '8"', '10"', '12"', '14"', '16"', '18"', '20"']
        
        # Create a modified pivot with highlighted OD categories
        pivot_highlighted = pivot.copy()
        
        # Add star marker prefix to highlighted OD categories (using "*" instead of emoji for font compatibility)
        new_index = []
        for idx in pivot_highlighted.index:
            if idx in highlight_od_categories:
                new_index.append(f"* {idx}")
            else:
                new_index.append(idx)
        pivot_highlighted.index = new_index
        
        # Apply styling
        styled = (
            pivot_highlighted.style
            .format("{:.2f}")
            .applymap(
                lambda v: highlight(v, minval, maxval, numeric_no_totals, metric),
                subset=pd.IndexSlice[pivot_highlighted.index, pivot_highlighted.columns]
            )
        )
        
        # Step 7: Calculate summary metrics
        # Calculate totals from original dataframes (filtered by specification)
        # Filter original dataframes by specification
        stock_filtered = stock_df[stock_df['Specification'].str.strip() == specification.strip()] if 'Specification' in stock_df.columns else pd.DataFrame()
        reservations_filtered = reservations_df[reservations_df['Specification'].str.strip() == specification.strip()] if 'Specification' in reservations_df.columns else pd.DataFrame()
        incoming_filtered = incoming_df[incoming_df['Specification'].str.strip() == specification.strip()] if 'Specification' in incoming_df.columns else pd.DataFrame()
        
        # Calculate totals
        stock_total = float(stock_filtered['MT'].sum()) if not stock_filtered.empty and 'MT' in stock_filtered.columns else 0.0
        reservation_total = float(reservations_filtered['MT'].sum()) if not reservations_filtered.empty and 'MT' in reservations_filtered.columns else 0.0
        incoming_total = float(incoming_filtered['MT'].sum()) if not incoming_filtered.empty and 'MT' in incoming_filtered.columns else 0.0
        free_for_sale_total = float(df_filtered['MT'].sum())
        
        metrics_dict = {
            'stock': round(stock_total, 2),
            'reservation': round(reservation_total, 2),
            'incoming': round(incoming_total, 2),
            'free_for_sale': round(free_for_sale_total, 2)
        }
        
        logger.info(f"Successfully generated heatmap for specification: {specification}")
        logger.info(f"Metrics: {metrics_dict}")
        
        return styled, metrics_dict, None
        
    except ValueError as e:
        error_msg = f"ValueError generating heatmap for {specification}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return None, None, error_msg
    
    except KeyError as e:
        error_msg = f"KeyError generating heatmap for {specification}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return None, None, error_msg
    
    except Exception as e:
        error_msg = f"Unexpected error generating heatmap for {specification}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return None, None, error_msg


# ============================================================================
# Image Generation Function
# ============================================================================

def generate_heatmap_image(
    styled_dataframe: pd.io.formats.style.Styler,
    specification: str,
    output_dir: Optional[str] = None
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Convert styled DataFrame to PNG image.
    
    This function takes the styled DataFrame from generate_heatmap_dataframe()
    and converts it to a PNG image file. No business logic or styling changes
    are applied - this is purely an image export operation.
    
    Args:
        styled_dataframe: pandas Styler object from generate_heatmap_dataframe()
        specification: Specification name (used for filename)
        output_dir: Output directory for image (default: from config)
    
    Returns:
        Tuple of (success: bool, image_path: str or None, error_message: str or None)
        - success: True if image was generated successfully
        - image_path: Full path to generated PNG file (or None if failed)
        - error_message: Error message string (or None if successful)
    
    Raises:
        ValueError: If styled_dataframe is None or invalid
        ImportError: If dataframe-image library is not installed
        IOError: If output directory cannot be created or written to
    """
    try:
        # Validate inputs
        if styled_dataframe is None:
            raise ValueError("styled_dataframe cannot be None")
        
        if not specification or pd.isna(specification):
            raise ValueError("specification cannot be empty")
        
        # Check if dataframe-image is available
        if dfi is None:
            raise ImportError(
                "dataframe-image library is not installed. "
                "Please install it with: pip install dataframe-image"
            )
        
        # Determine output directory
        if output_dir is None:
            output_dir = REPORTS_DIR
        
        # Ensure output directory exists
        try:
            os.makedirs(output_dir, exist_ok=True)
            logger.debug(f"Output directory ensured: {output_dir}")
        except OSError as e:
            error_msg = f"Failed to create output directory '{output_dir}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, None, error_msg
        
        # Generate image filename
        # Sanitize specification name for filename (remove invalid characters)
        safe_spec = str(specification).strip().replace('/', '_').replace('\\', '_')
        filename = f"{HEATMAP_IMAGE_PREFIX}{safe_spec}{HEATMAP_IMAGE_EXTENSION}"
        image_path = os.path.join(output_dir, filename)
        
        logger.info(f"Converting styled DataFrame to PNG: {image_path}")
        
        # Convert styled DataFrame to PNG image
        # Using matplotlib backend for better compatibility
        try:
            dfi.export(
                styled_dataframe,
                image_path,
                table_conversion='matplotlib',
                dpi=150  # High resolution for clear images
            )
            logger.info(f"Successfully generated PNG image: {image_path}")
            
            # Verify file was created
            if not os.path.exists(image_path):
                error_msg = f"Image file was not created at expected path: {image_path}"
                logger.error(error_msg)
                return False, None, error_msg
            
            # Check file size (should be > 0)
            file_size = os.path.getsize(image_path)
            if file_size == 0:
                error_msg = f"Generated image file is empty: {image_path}"
                logger.error(error_msg)
                # Clean up empty file
                try:
                    os.remove(image_path)
                except Exception:
                    pass
                return False, None, error_msg
            
            logger.info(f"Image file created successfully: {image_path} ({file_size} bytes)")
            return True, image_path, None
            
        except Exception as e:
            error_msg = f"Failed to export styled DataFrame to PNG: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            # Clean up partial file if it exists
            if os.path.exists(image_path):
                try:
                    os.remove(image_path)
                    logger.debug(f"Cleaned up partial image file: {image_path}")
                except Exception:
                    pass
            
            return False, None, error_msg
        
    except ValueError as e:
        error_msg = f"ValueError in image generation: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, None, error_msg
    
    except ImportError as e:
        error_msg = f"ImportError in image generation: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, None, error_msg
    
    except OSError as e:
        error_msg = f"OSError in image generation: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, None, error_msg
    
    except Exception as e:
        error_msg = f"Unexpected error in image generation: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, None, error_msg

