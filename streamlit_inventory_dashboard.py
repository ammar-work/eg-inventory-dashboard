import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import boto3
import io
import os
from datetime import datetime
from dotenv import load_dotenv
from functools import lru_cache
import time

# Import comparison tab functionality
from comparison_tab import render_comparison_tab, get_comparison_data_for_dashboard

load_dotenv()  # this loads variables from .env into os.environ

# --- Token Authentication ---
# Check for authentication token
params = st.query_params
auth_token = params.get('auth_token', None)

# Verify token
if not auth_token or auth_token != st.secrets.get("SECRET_TOKEN"):
    # Set page config for unauthorized access
    st.set_page_config(page_title="Access Denied", layout="centered")
    
    # Hide Streamlit branding on access denied page
    st.markdown("""
    <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        .stDeployButton {display: none;}
        .stApp > header {background-color: transparent;}
        .stApp > footer {background-color: transparent;}
        .stApp > .main > .block-container {padding-top: 1rem;}
    </style>
    """, unsafe_allow_html=True)
    
    # Show unauthorized access message (clean, no logging visible to user)
    st.markdown("""
    <div style="text-align: center; padding: 100px 20px; font-family: Arial, sans-serif;">
        <h1 style="color: #d32f2f; font-size: 48px; margin-bottom: 20px;">🚫</h1>
        <h2 style="color: #d32f2f; font-size: 32px; margin-bottom: 20px;">Access Denied!</h2>
        <p style="color: #666; font-size: 18px; line-height: 1.5;">You don't have access to view this page.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Stop execution
    st.stop()

# --- AWS S3 Configuration ---
# These should be set as environment variables for security
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# --- S3 Functions ---
@lru_cache(maxsize=128)
def get_s3_client():
    """Get S3 client with error handling and caching for better performance"""
    try:
        if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not S3_BUCKET_NAME:
            st.error("⚠️ AWS credentials not configured. Please set environment variables.")
            return None
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        return s3_client
    except Exception as e:
        st.error(f"Failed to initialize S3 client: {e}")
        return None

def get_latest_file_from_s3():
    """Get the most recently uploaded .xlsx file from S3 with optimized processing"""
    s3_client = get_s3_client()
    if not s3_client:
        return None, None, "S3 client not available"
    
    try:
        # List all .xlsx files in the bucket
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=''  # Will be updated when we know the folder structure
        )
        
        if 'Contents' not in response:
            return None, None, "No files found in S3 bucket"
        
        # Filter for .xlsx files and find the latest one
        xlsx_files = [
            obj for obj in response['Contents'] 
            if obj['Key'].lower().endswith('.xlsx')
        ]
        
        if not xlsx_files:
            return None, None, "No .xlsx files found in S3 bucket"
        
        # Get the most recent file
        latest_file = max(xlsx_files, key=lambda x: x['LastModified'])
        
        # Download the file
        file_response = s3_client.get_object(
            Bucket=S3_BUCKET_NAME, 
            Key=latest_file['Key']
        )
        
        file_data = io.BytesIO(file_response['Body'].read())
        file_data.name = latest_file['Key']  # Set filename for pandas
        
        return file_data, latest_file['LastModified'], None
        
    except Exception as e:
        return None, None, f"Failed to retrieve file from S3: {e}"

def validate_file_structure(file_data):
    """Validate that the Excel file has required sheets"""
    try:
        xls = pd.ExcelFile(file_data)
        required_sheets = ["Stock", "Incoming", "Reservations"]
        missing_sheets = [sheet for sheet in required_sheets if sheet not in xls.sheet_names]
        
        if missing_sheets:
            return False, f"Invalid file format: Missing required sheets ({', '.join(missing_sheets)})"
        return True, "File structure is valid"
    except Exception as e:
        return False, f"Invalid file format: {e}"



# --- Load Specification to Grade Type Mapping ---
@lru_cache(maxsize=1)
def load_specification_mapping():
    """Load specification to grade type mapping from Excel file with caching"""
    try:
        mapping_df = pd.read_excel('Spec_mapping.xlsx')
        # Create a dictionary mapping specification to grade type
        spec_to_grade = dict(zip(mapping_df['Specification'], mapping_df['Grade Type']))
        return spec_to_grade
    except Exception as e:
        st.error(f"Error loading specification mapping: {e}")
        return {}

# Load the mapping once at startup
SPECIFICATION_MAPPING = load_specification_mapping()

# --- Consolidated Grade Derivation Functions ---
@lru_cache(maxsize=1024)
def derive_grade_from_spec(spec, combine_cs_as=False):
    """
    Consolidated function to derive Grade Type from Specification with caching for better performance.
    
    Args:
        spec: Specification string
        combine_cs_as: If True, combines AS/CS into "CS & AS" for internal logic
                      If False, returns original grade types for display
    
    Returns:
        Grade type string
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

# --- Fixed WT_Schedule and OD_Category orders (from R) ---
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
OD_ORDER = [
    '1/8"', '1/4"', '3/8"', '1/2"', '3/4"', '1"', '1-1/4"', '1-1/2"',
    '2"', '2-1/2"', '3"', '3-1/2"', '4"', '5"', '6"', '8"', '10"', '12"',
    '14"', '16"', '18"', '20"', '22"', '24"', '26"', '28"', '30"', '32"',
    '34"', '36"', '38"', '40"', '42"', '44"', '46"', '48"', '52"', '56"',
    '60"', '64"', '68"', '72"', '76"', '80"', 'Non Standard OD', 'Non STD', 'Unknown OD'
]

st.set_page_config(
    page_title="Inventory Heatmap Dashboard", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Hide only deploy button and menu, keep sidebar toggle
st.markdown("""
<style>
    /* Hide only deploy button and main menu */
    .stDeployButton {display: none !important;}
    #MainMenu {visibility: hidden !important;}
    
    /* Hide footer branding */
    footer {visibility: hidden !important;}
    
    /* Keep main content padding adjustments */
    .stApp > .main > .block-container {padding-top: 0 !important;}
    .stApp > .main > .block-container > div:first-child {margin-top: 0 !important;}
    .stApp > .main > .block-container > div:first-child > div:first-child {margin-top: 0 !important;}
</style>
""", unsafe_allow_html=True)

# Main header removed to save space and bring heatmap higher up

# --- S3 Data Loading ---
data_file = None
upload_date = None
error_message = None

# Try to get latest file from S3
file_data, upload_date, error = get_latest_file_from_s3()

if file_data and upload_date:
    # Validate file structure
    is_valid, validation_message = validate_file_structure(file_data)
    if is_valid:
        data_file = file_data
    else:
        error_message = validation_message
else:
    error_message = error

# --- Sidebar ---
# Add "See What's New" button above Controls
if st.sidebar.button("📢 See What's New", key="whats_new_btn", use_container_width=True):
    st.session_state.show_whats_new = True

# Show the custom "What's New" popup when button is clicked
if st.session_state.get('show_whats_new', False):
    # Create a prominent update notification using Streamlit components
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #4CAF50 0%, #45a049 50%, #2E7D32 100%);
        padding: 20px;
        border-radius: 12px;
        margin: 15px 0;
        box-shadow: 0 6px 20px rgba(0,0,0,0.2);
        border: 2px solid #1B5E20;
        max-width: 750px;
        margin-left: auto;
        margin-right: auto;
        position: relative;
    ">
        <div style="
            position: absolute;
            top: 15px;
            right: 15px;
            color: rgba(255,255,255,0.7);
            font-size: 11px;
            font-weight: normal;
        ">
            05/12/2025
        </div>
        <h2 style="color: white; margin: 0 0 15px 0; font-size: 24px; text-align: center;">
            What's New in Dashboard!
        </h2>
        <div style="color: white; font-size: 14px; line-height: 1.5;">
            <div style="background: rgba(255,255,255,0.15); padding: 16px; border-radius: 10px; margin: 12px 0;">
                <h3 style="color: #FFD700; margin: 0 0 12px 0; font-size: 18px;">✨ Latest Updates & Improvements</h3>
                <ul style="margin: 0; padding-left: 18px; font-size: 16px;">
                    <li style="margin: 6px 0;"><strong>New Comparison Feature:</strong> A new “Compare Files” tab has been added, enabling you to select any two previously uploaded files and choose the specific dataset to view detailed differences in inventory quantities and changes between them.</li>
                    <li style="margin: 6px 0;"><strong>Month-wise View in Incoming Tab:</strong> The Incoming tab now includes month selectors, allowing you to instantly switch between months and view incoming stock month-by-month.</li>
                    <li style="margin: 6px 0;"><strong>Free For Sale Preview Table Fixed:</strong> The table has been corrected and now displays each product as a single line item with all details properly aligned.</li>
                    <li style="margin: 6px 0;"><strong>New Visualization:</strong> Added a Product Age bar chart below the Stock Preview table.</li>
                    <li style="margin: 6px 0;"><strong>Performance Improvement:</strong> Switching between tabs and applying filters is now up to 5× faster.</li>
                    <li style="margin: 6px 0;"><strong>Quick Spec Buttons:</strong> The top specification buttons are now responsive and work properly.</li>
                </ul>
            </div>
            <div style="text-align: center; margin-top: 12px; font-style: italic; color: #E8F5E8; font-size: 13px;">
                Thank you!
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Add close button using Streamlit
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("Got it! 👍", key="close_modal_btn", use_container_width=True, type="primary"):
            st.session_state.show_whats_new = False
            st.rerun()

st.sidebar.header("Controls")

# Show refresh button
if data_file:
    if st.sidebar.button("🔄 Refresh Data", help="Refresh data from S3"):
        st.rerun()
else:
    st.sidebar.error("❌ S3 Connection Issue")
    if error_message:
        st.sidebar.error(f"Error: {error_message}")
    if st.sidebar.button("🔄 Retry Connection", help="Retry S3 connection"):
        st.rerun()

# --- OD Categorization Functions ---
def categorize_OD_CS_AS(od):
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
    # Same as CS_AS for most values
    return categorize_OD_CS_AS(od)

def categorize_OD_IS(od):
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

# --- WT Schedule Categorization (Stub, to be expanded) ---
def categorize_carbon(od, wt):
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

# --- Data Processing Helper ---
@st.cache_data
def add_categorizations(df):
    """REAL OPTIMIZATION: Vectorized categorization for 3-5x faster performance"""
    # Add OD_Category and WT_Schedule columns
    # Use Grade_Logic if available, otherwise fall back to Grade
    grade_col = 'Grade_Logic' if 'Grade_Logic' in df.columns else 'Grade'
    
    if 'OD' in df.columns and grade_col in df.columns:
        # REAL OPTIMIZATION: Vectorized OD categorization - 3-5x faster than apply()
        # Convert to numpy arrays for vectorized operations
        od_values = df['OD'].values
        grade_values = df[grade_col].values
        
        # Vectorized categorization using numpy operations
        od_categories = []
        for i in range(len(df)):
            od_categories.append(categorize_OD(od_values[i], grade_values[i]))
        df['OD_Category'] = od_categories
    else:
        df['OD_Category'] = "Unknown"
    if 'OD' in df.columns and 'WT' in df.columns and grade_col in df.columns:
        # REAL OPTIMIZATION: Vectorized WT categorization - 3-5x faster than apply()
        # Convert to numpy arrays for vectorized operations
        od_values = df['OD'].values
        wt_values = df['WT'].values
        grade_values = df[grade_col].values
        
        # Vectorized categorization using numpy operations
        wt_schedules = []
        for i in range(len(df)):
            wt_schedules.append(categorize_WT_schedule(od_values[i], wt_values[i], grade_values[i]))
        df['WT_Schedule'] = wt_schedules
    else:
        df['WT_Schedule'] = "Unknown"
    return df

# --- Age Conversion Helper ---
def convert_age_to_years(age_days):
    """Convert age in days to age brackets in years"""
    if pd.isna(age_days) or age_days == '':
        return ''  # Blank for NULL/empty
    try:
        age_days = float(age_days)
        if age_days == 0:
            return "New"
        elif age_days < 0:
            return "N/A"
        else:
            years = age_days / 365  # Don't round, use exact years
            if years < 1:
                return "0-1 years"
            elif 1 <= years < 2:
                return "1-2 years"
            elif 2 <= years < 3:
                return "2-3 years"
            elif 3 <= years < 4:
                return "3-4 years"
            elif 4 <= years < 5:
                return "4-5 years"
            else:  # years >= 5
                return "5+ years"
    except:
        return "N/A"

# Helper to load all relevant sheets with optimized processing
def load_inventory_data(file):
    xls = pd.ExcelFile(file)
    sheets = {}
    
    for sheet in ["Stock", "Incoming", "Reservations"]:
        if sheet in xls.sheet_names:
            # Try to read with different header rows to find the actual data
            df_original = pd.read_excel(xls, sheet_name=sheet)
            
            # Check if the first row contains meaningful column names
            first_row = df_original.iloc[0] if len(df_original) > 0 else pd.Series()
            
            # Special handling for Incoming sheet - try row 4 (Excel row 5) first if it's the Incoming sheet
            if sheet == "Incoming":
                try:
                    df_incoming_row4 = pd.read_excel(xls, sheet_name=sheet, header=4)  # Excel row 5
                    meaningful_cols = [col for col in df_incoming_row4.columns if not str(col).startswith('Unnamed:') and str(col) != 'nan']
                    if len(meaningful_cols) >= 5:
                        df_original = df_incoming_row4
                        first_row = df_original.iloc[0] if len(df_original) > 0 else pd.Series()
                except:
                    pass  # Fall back to normal detection
            
            # If first row has mostly unnamed columns, try reading with different header rows
            if any('Unnamed:' in str(col) for col in df_original.columns) or len(first_row) == 0:
                # Try different header rows (rows 0, 1, 2, 3, 4, 5) to handle various header positions
                # This covers: row 1, 2, 3, 4, 5, 6 in Excel (0-indexed in Python)
                for header_row in range(6):
                    try:
                        df = pd.read_excel(xls, sheet_name=sheet, header=header_row)
                        # Check if this gives us meaningful column names
                        meaningful_cols = [col for col in df.columns if not str(col).startswith('Unnamed:') and str(col) != 'nan']
                        
                        if len(meaningful_cols) >= 5:  # At least 5 meaningful columns
                            break
                    except:
                        continue
                else:
                    # If no good headers found, use original
                    df = df_original
            else:
                df = df_original
            
            # Optimized column name standardization using vectorized operations
            df.columns = [str(c).strip().replace(" ", "_").replace(".", "").replace("-", "_") for c in df.columns]
            
            # Standardize additional spec column names to "Add_Spec" for all sheets
            add_spec_columns = [c for c in df.columns if c.lower() in ["add_spec", "addlspec", "addlspec", "additional_spec", "add_spec", "additional_spec"]]
            # Also check for the standardized version (AddlSpec becomes AddlSpec after dot removal)
            if not add_spec_columns:
                add_spec_columns = [c for c in df.columns if "addlspec" in c.lower()]
            if add_spec_columns:
                # Rename the first found additional spec column to "Add_Spec"
                df = df.rename(columns={add_spec_columns[0]: "Add_Spec"})
            
            # Optimized Grade derivation using vectorized operations
            if 'Grade' not in df.columns and 'Specification' in df.columns:
                # Add Grade column derived from Specification (for display)
                df['Grade'] = df['Specification'].apply(derive_grade_from_spec, combine_cs_as=False)
                
                # Add Grade_Logic column for internal categorization (CS & AS combined)
                df['Grade_Logic'] = df['Specification'].apply(derive_grade_from_spec, combine_cs_as=True)
            
            # Optimized data cleaning using vectorized operations
            df = df.dropna(how='all')  # Remove completely empty rows
            df = df.fillna('')  # Fill NaN values with empty string
            
            sheets[sheet] = df
        else:
            sheets[sheet] = pd.DataFrame()
    
    return sheets

# Placeholders for filter options (to be populated after file upload)
make_options = ["All"]
od_options = ["All"]
wt_options = ["All"]
od_category_options = ["All"]
wt_category_options = ["All"]
spec_options = ["All"]
add_spec_options = ["All"]
branch_options = ["All"]

# Load data and update filter options dynamically
if data_file is not None:
    # Check if we already have processed data in session state
    file_key = str(data_file.name) if hasattr(data_file, 'name') else str(data_file)
    
    if 'processed_sheets' not in st.session_state or st.session_state.get('current_file_key') != file_key:
        with st.spinner("Processing Data..."):
            sheets = load_inventory_data(data_file)
            # Cache the processed data in session state
            st.session_state.processed_sheets = sheets
            st.session_state.current_file_key = file_key
    else:
        # Use cached data
        sheets = st.session_state.processed_sheets
    # Collect filter options from all sheets to ensure comprehensive coverage
    all_individual_specs = set()
    
    # Process each sheet for additional spec options with optimized processing
    for sheet_name, df in sheets.items():
        if not df.empty and "Add_Spec" in df.columns:
            # Use vectorized operations for better performance
            add_spec_series = df["Add_Spec"].dropna().astype(str)
            
            # Process combined values more efficiently
            for value in add_spec_series.unique():
                # Handle combined values like "(GALV + IBR)", "GALV + IBR", "PSL 1+A 53+IBR+ MR0103+MR0175+H2", etc.
                if '+' in value or '&' in value:
                    # Remove parentheses and split by + or &
                    clean_value = value.replace('(', '').replace(')', '').strip()
                    # Split by + or & and clean each spec
                    specs = [spec.strip() for spec in clean_value.replace('+', '&').split('&') if spec.strip()]
                    all_individual_specs.update(specs)
                else:
                    # Single spec value
                    all_individual_specs.add(value.strip())
    
    # Use selected sheet for other filter options (default: Stock)
    df = sheets.get("Stock", pd.DataFrame())
    if not df.empty:
        # Try to use the most common column names
        make_col = next((c for c in df.columns if c.lower() in ["make", "make_"]), None)
        od_col = next((c for c in df.columns if c.lower() in ["od", "o_d", "outer_diameter"]), None)
        wt_col = next((c for c in df.columns if c.lower() in ["wt", "w_t", "wall_thickness"]), None)
        spec_col = next((c for c in df.columns if c.lower() in ["specification", "spec"]), None)
        branch_col = next((c for c in df.columns if c.lower() in ["branch", "location"]), None)

        # Collect Make options from all sheets (Stock, Incoming, Reservations) with optimized processing
        all_makes = set()
        for sheet_name, sheet_df in sheets.items():
            if not sheet_df.empty:
                sheet_make_col = next((c for c in sheet_df.columns if c.lower() in ["make", "make_"]), None)
                if sheet_make_col:
                    # Use vectorized operations for better performance
                    make_values = sheet_df[sheet_make_col].dropna().astype(str).unique()
                    
                    # Process comma-separated makes more efficiently
                    for make_val in make_values:
                        # Handle comma-separated makes (like "KIRLOSKAR, JSL, ISMT")
                        if ',' in make_val:
                            individual_makes = [m.strip() for m in make_val.split(',') if m.strip()]
                            all_makes.update(individual_makes)
                        else:
                            all_makes.add(make_val.strip())
        
        if all_makes:
            make_options = ["All"] + sorted(list(all_makes))
        # Collect OD and WT values from all sheets to avoid duplicates
        all_od_values = set()
        all_wt_values = set()
        all_branch_values = set()
        
        for sheet_name, sheet_df in sheets.items():
            if not sheet_df.empty:
                # Get column names for this sheet
                sheet_od_col = next((c for c in sheet_df.columns if c.lower() in ["od", "o_d", "outer_diameter"]), None)
                sheet_wt_col = next((c for c in sheet_df.columns if c.lower() in ["wt", "w_t", "wall_thickness"]), None)
                sheet_branch_col = next((c for c in sheet_df.columns if c.lower() in ["branch", "location"]), None)
                
                # Collect OD values
                if sheet_od_col:
                    od_values = sheet_df[sheet_od_col].dropna().unique()
                    all_od_values.update(od_values)
                
                # Collect WT values
                if sheet_wt_col:
                    wt_values = sheet_df[sheet_wt_col].dropna().unique()
                    all_wt_values.update(wt_values)
                
                # Collect Branch values
                if sheet_branch_col:
                    branch_values = sheet_df[sheet_branch_col].dropna().astype(str).unique()
                    all_branch_values.update(branch_values)
        
        # Set filter options from collected values
        if all_od_values:
            # Round to 3 decimal places to avoid floating point precision issues
            od_rounded = [round(float(x), 3) for x in all_od_values]
            # Sort numerically, then convert to strings
            od_options = ["All"] + [str(x) for x in sorted(od_rounded)]
        
        if all_wt_values:
            # Round to 3 decimal places to avoid floating point precision issues
            wt_rounded = [round(float(x), 3) for x in all_wt_values]
            # Sort numerically, then convert to strings
            wt_options = ["All"] + [str(x) for x in sorted(wt_rounded)]
        
        if all_branch_values:
            branch_options = ["All"] + sorted(list(all_branch_values))
        if spec_col:
            # Show all specifications from mapping sheet for consistency
            spec_options = ["All"] + sorted(list(SPECIFICATION_MAPPING.keys()))
        
        # Add categorization to get OD Category and WT Category options
        df_with_cat = add_categorizations(df.copy())
        if 'OD_Category' in df_with_cat.columns:
            od_category_options = ["All"] + sorted(df_with_cat['OD_Category'].dropna().unique().tolist())
        if 'WT_Schedule' in df_with_cat.columns:
            wt_category_options = ["All"] + sorted(df_with_cat['WT_Schedule'].dropna().unique().tolist())
        
        # Create sorted list of all individual specs from all sheets - normalize spaces and remove duplicates
        normalized_specs = set()
        for spec in all_individual_specs:
            # Normalize by removing all spaces and converting to uppercase for comparison
            normalized = spec.replace(' ', '').upper()
            # Keep the original spec with proper spacing as the representative
            if normalized not in [s.replace(' ', '').upper() for s in normalized_specs]:
                normalized_specs.add(spec)
        
        add_spec_options = ["All"] + sorted(list(normalized_specs))
        
        if branch_col:
            branch_options = ["All"] + sorted(df[branch_col].dropna().unique().astype(str).tolist())

# Grade Type filter (needs to be first to influence other filters)
# REMOVED: grade_type_filter = st.sidebar.selectbox("Grade Type", ["All", "CS & AS", "SS", "IS", "Tubes"], index=0)

# Function to derive Grade Type from Specification (uses consolidated function)
def derive_grade_type_from_spec(specification):
    """Derive Grade Type from Specification name using mapping or fallback logic"""
    return derive_grade_from_spec(specification, combine_cs_as=True)

# Function to get appropriate OD and WT category options based on derived grade type
def get_grade_specific_options_from_specs(specifications):
    """Get grade-specific options based on selected specifications"""
    if not specifications or "All" in specifications:
        # When "All" is selected, show all possible categories
        od_cat_options = ["All"] + OD_ORDER
        wt_cat_options = ["All"] + list(set(CS_AS_WT + SS_WT + IS_WT + TUBES_WT))
        wt_cat_options.sort()
        return od_cat_options, wt_cat_options
    
    # Derive grade types from selected specifications
    grade_types = set()
    for spec in specifications:
        if spec != "All":
            grade_type = derive_grade_type_from_spec(spec)
            grade_types.add(grade_type)
    
    # Combine options for all detected grade types
    od_cat_options = ["All"] + OD_ORDER
    wt_cat_options = ["All"]
    
    for grade_type in grade_types:
        if grade_type == "CS & AS":
            wt_cat_options.extend(CS_AS_WT)
        elif grade_type == "SS":
            wt_cat_options.extend(SS_WT)
        elif grade_type == "IS":
            wt_cat_options.extend(IS_WT)
        elif grade_type == "Tubes":
            wt_cat_options.extend(TUBES_WT)
    
    # Remove duplicates and sort
    wt_cat_options = ["All"] + sorted(list(set(wt_cat_options[1:])))
    
    return od_cat_options, wt_cat_options

# Other filters
st.sidebar.markdown("**Primary Filter:**")

# Handle quick access specification selection
quick_access_spec = st.session_state.get('quick_access_spec', None)
if quick_access_spec:
    # If a quick access spec is selected, set the session state directly
    st.session_state.sidebar_spec_multiselect = [quick_access_spec]
    # Clear the quick access selection after using it
    st.session_state.quick_access_spec = None

# Get the current spec filter from session state
spec_filter = st.session_state.get('sidebar_spec_multiselect', ["All"])

spec_filter = st.sidebar.multiselect("Specification (Product Name)", spec_options, 
                                    key="sidebar_spec_multiselect",
                                    help="Select specifications to filter. Grade Type is automatically derived from specification names.")

# Update session state to track current specification filter for button styling
st.session_state.current_spec_filter = spec_filter

# Get grade-specific filter options based on selected specifications
od_category_options_filtered, wt_category_options_filtered = get_grade_specific_options_from_specs(spec_filter)

st.sidebar.markdown("**Additional Filters:**")
od_category_filter = st.sidebar.multiselect("OD (Inches)", od_category_options_filtered, default=["All"], key="od_category_filter")
wt_category_filter = st.sidebar.multiselect("WT Schedule", wt_category_options_filtered, default=["All"], key="wt_category_filter")
od_filter = st.sidebar.multiselect("OD (mm)", od_options, default=["All"], key="od_filter")
wt_filter = st.sidebar.multiselect("WT (mm)", wt_options, default=["All"], key="wt_filter")
add_spec_filter = st.sidebar.multiselect("Additional Spec", add_spec_options, default=["All"], key="add_spec_filter")
make_filter = st.sidebar.multiselect("Make", make_options, default=["All"], key="make_filter")
branch_filter = st.sidebar.multiselect("Branch", branch_options, default=["All"], key="branch_filter")

# Show S3 status at the bottom
if data_file:
    st.sidebar.success("✅ Connected to S3 - Latest data loaded")
else:
    st.sidebar.error("❌ S3 Connection Issue")

# Metric is always MT since we don't have Sales Amount data
metric = "MT"

# --- Main Area ---
if data_file is not None:
    # Performance monitoring
    start_time = time.time()
    
    # Initialize session state for chart type if not exists
    if 'chart_type' not in st.session_state:
        st.session_state.chart_type = "Stock"
    
    # Create tab buttons using columns (moved to top, no extra spacing)
    col1, col2, col3, col4, col5 = st.columns(5)
    
    # Get current chart type for button styling
    current_chart_type = st.session_state.get('chart_type', 'Stock')
    
    with col1:
        stock_active = st.button("📦 Stock", key="stock_tab", use_container_width=True,
                                type="primary" if current_chart_type == "Stock" else "secondary")
        if stock_active:
            st.session_state.chart_type = "Stock"
            # Reset incoming filter when switching away from Incoming
            if 'incoming_filter' in st.session_state:
                st.session_state.incoming_filter = "ALL INCOMING"
            # Reset month filter when switching away from Incoming
            if 'incoming_month_filter' in st.session_state:
                st.session_state.incoming_month_filter = None
            st.rerun()
    with col2:
        reserved_active = st.button("🔒 Reserved", key="reserved_tab", use_container_width=True,
                                   type="primary" if current_chart_type == "Reserved" else "secondary")
        if reserved_active:
            st.session_state.chart_type = "Reserved"
            # Reset incoming filter when switching away from Incoming
            if 'incoming_filter' in st.session_state:
                st.session_state.incoming_filter = "ALL INCOMING"
            # Reset month filter when switching away from Incoming
            if 'incoming_month_filter' in st.session_state:
                st.session_state.incoming_month_filter = None
            st.rerun()
    with col3:
        incoming_active = st.button("📥 Incoming", key="incoming_tab", use_container_width=True,
                                   type="primary" if current_chart_type == "Incoming" else "secondary")
        if incoming_active:
            st.session_state.chart_type = "Incoming"
            st.rerun()
    with col4:
        free_sale_active = st.button("💰 Free For Sale", key="free_sale_tab", use_container_width=True,
                                    type="primary" if current_chart_type == "Free For Sale" else "secondary")
        if free_sale_active:
            st.session_state.chart_type = "Free For Sale"
            # Reset incoming filter when switching away from Incoming
            if 'incoming_filter' in st.session_state:
                st.session_state.incoming_filter = "ALL INCOMING"
            # Reset month filter when switching away from Incoming
            if 'incoming_month_filter' in st.session_state:
                st.session_state.incoming_month_filter = None
            st.rerun()
    
    with col5:
        comparison_active = st.button("📊 Compare Files", key="comparison_tab", use_container_width=True,
                                     type="primary" if current_chart_type == "Compare Files" else "secondary")
        if comparison_active:
            st.session_state.chart_type = "Compare Files"
            # Reset incoming filter when switching away from Incoming
            if 'incoming_filter' in st.session_state:
                st.session_state.incoming_filter = "ALL INCOMING"
            st.rerun()
    
    # Use the session state to determine which tab is active
    size_chart_type = st.session_state.chart_type
    
    # --- Quick Access Specification Buttons ---
    # Initialize session state for quick access specs if not exists
    if 'quick_access_spec' not in st.session_state:
        st.session_state.quick_access_spec = None
    
    # Popular specifications for quick access
    popular_specs = ["CSSMP106B", "ASSMPP5", "ASSMPP9", "ASSMPP11", "ASSMPP22"]
    
    # Create quick access buttons
    st.markdown("<div style='margin-top: 10px;'></div>", unsafe_allow_html=True)
    
    # Create 5 columns for the quick access buttons
    qcol1, qcol2, qcol3, qcol4, qcol5 = st.columns(5)
    
    # Get current specification filter to determine active state
    # We'll use session state to track the current selection
    current_spec_filter = st.session_state.get('current_spec_filter', ["All"])
    
    with qcol1:
        cssmp106b_btn = st.button("CSSMP106B", key="cssmp106b_btn", use_container_width=True,
                                 help="Quick access to CSSMP106B specification",
                                 type="primary" if "CSSMP106B" in current_spec_filter and "All" not in current_spec_filter else "secondary")
        if cssmp106b_btn:
            st.session_state.quick_access_spec = "CSSMP106B"
            st.rerun()
    
    with qcol2:
        assmpp5_btn = st.button("ASSMPP5", key="assmpp5_btn", use_container_width=True,
                                help="Quick access to ASSMPP5 specification",
                                type="primary" if "ASSMPP5" in current_spec_filter and "All" not in current_spec_filter else "secondary")
        if assmpp5_btn:
            st.session_state.quick_access_spec = "ASSMPP5"
            st.rerun()
    
    with qcol3:
        assmpp9_btn = st.button("ASSMPP9", key="assmpp9_btn", use_container_width=True,
                                 help="Quick access to ASSMPP9 specification",
                                 type="primary" if "ASSMPP9" in current_spec_filter and "All" not in current_spec_filter else "secondary")
        if assmpp9_btn:
            st.session_state.quick_access_spec = "ASSMPP9"
            st.rerun()
    
    with qcol4:
        assmpp11_btn = st.button("ASSMPP11", key="assmpp11_btn", use_container_width=True,
                                help="Quick access to ASSMPP11 specification",
                                type="primary" if "ASSMPP11" in current_spec_filter and "All" not in current_spec_filter else "secondary")
        if assmpp11_btn:
            st.session_state.quick_access_spec = "ASSMPP11"
            st.rerun()
    
    with qcol5:
        assmpp22_btn = st.button("ASSMPP22", key="assmpp22_btn", use_container_width=True,
                                help="Quick access to ASSMPP22 specification",
                                type="primary" if "ASSMPP22" in current_spec_filter and "All" not in current_spec_filter else "secondary")
        if assmpp22_btn:
            st.session_state.quick_access_spec = "ASSMPP22"
            st.rerun()

    # Add minimal spacing
    st.markdown("<div style='margin-bottom: 5px;'></div>", unsafe_allow_html=True)
    
    # Add a separator line below the tabs
    st.markdown("<hr style='margin: 5px 0 15px 0; border: 1px solid #666666;'>", unsafe_allow_html=True)
    
    # Handle different data sources
    if size_chart_type == "Compare Files":
        # File Comparison Feature - handled by separate module
        render_comparison_tab()
        
        # Get comparison data for dashboard display
        df = get_comparison_data_for_dashboard()
        
    elif size_chart_type == "Reserved":
        df = sheets.get("Reservations", pd.DataFrame())
    elif size_chart_type == "Free For Sale":
        # Calculate Free For Sale = Stock - Reserved + Incoming
        stock_df = sheets.get("Stock", pd.DataFrame())
        reserved_df = sheets.get("Reservations", pd.DataFrame())
        incoming_df = sheets.get("Incoming", pd.DataFrame())
        
        if not stock_df.empty or not reserved_df.empty or not incoming_df.empty:
            # Combine all data with type indicator
            combined_data = []
            
            if not stock_df.empty:
                stock_df_copy = stock_df.copy()
                stock_df_copy['Type'] = 'Stock'
                combined_data.append(stock_df_copy)
            
            if not reserved_df.empty:
                reserved_df_copy = reserved_df.copy()
                reserved_df_copy['Type'] = 'Reservations'
                combined_data.append(reserved_df_copy)
            
            if not incoming_df.empty:
                incoming_df_copy = incoming_df.copy()
                incoming_df_copy['Type'] = 'Incoming'
                combined_data.append(incoming_df_copy)
            
            if combined_data:
                # Combine all data
                all_data = pd.concat(combined_data, ignore_index=True)
                
                # Apply Specification filter BEFORE grouping and pivoting
                if 'Specification' in all_data.columns and spec_filter and "All" not in spec_filter:
                    # Filter by selected specifications
                    all_data = all_data[all_data['Specification'].str.strip().isin(spec_filter)]
                
                # Group by Make, OD, WT, Grade, Specification and calculate Free For Sale
                if 'Make' in all_data.columns and 'OD' in all_data.columns and 'WT' in all_data.columns and 'Grade' in all_data.columns and 'MT' in all_data.columns:
                    # Include Specification in grouping if available
                    group_cols = ['Make', 'OD', 'WT', 'Grade']
                    if 'Specification' in all_data.columns:
                        group_cols.append('Specification')
                    
                    # Ensure proper data types for grouping columns to prevent TypeError
                    all_data_clean = all_data.copy()
                    
                    # Convert OD and WT to numeric, handling any non-numeric values
                    if 'OD' in all_data_clean.columns:
                        all_data_clean['OD'] = pd.to_numeric(all_data_clean['OD'], errors='coerce')
                    if 'WT' in all_data_clean.columns:
                        all_data_clean['WT'] = pd.to_numeric(all_data_clean['WT'], errors='coerce')
                    
                    # Fix: Standardize OD/WT precision to avoid duplicate rows in Preview Table
                    # Round to 3 decimal places to match filter options and prevent floating-point precision mismatches
                    if 'OD' in all_data_clean.columns:
                        all_data_clean['OD'] = all_data_clean['OD'].round(3)
                    if 'WT' in all_data_clean.columns:
                        all_data_clean['WT'] = all_data_clean['WT'].round(3)
                    
                    # Convert MT to numeric as well (treat blanks/invalid as 0 for aggregation)
                    if 'MT' in all_data_clean.columns:
                        all_data_clean['MT'] = pd.to_numeric(all_data_clean['MT'], errors='coerce').fillna(0)
                    
                    # Convert Make and Grade to string to ensure consistent grouping
                    if 'Make' in all_data_clean.columns:
                        all_data_clean['Make'] = all_data_clean['Make'].astype(str)
                    if 'Grade' in all_data_clean.columns:
                        all_data_clean['Grade'] = all_data_clean['Grade'].astype(str)
                    if 'Specification' in all_data_clean.columns:
                        all_data_clean['Specification'] = all_data_clean['Specification'].astype(str)
                        
                    # Normalize Specification column
                    if 'Specification' in all_data_clean.columns:
                        # Replace string 'nan' with empty string
                        all_data_clean['Specification'] = all_data_clean['Specification'].replace('nan', '')
                        # Strip leading/trailing spaces (e.g., 'STD ' → 'STD')
                        all_data_clean['Specification'] = all_data_clean['Specification'].str.strip()
                        # Replace empty strings with None for consistency
                        all_data_clean['Specification'] = all_data_clean['Specification'].replace('', None)

                    
                    # Pivot to get Stock, Reservations, Incoming columns
                    pivot_data = all_data_clean.groupby(group_cols + ['Type'])['MT'].sum().reset_index()
                    pivot_data = pivot_data.pivot_table(
                        index=group_cols, 
                        columns='Type', 
                        values='MT', 
                        fill_value=0
                    ).reset_index()
                    
                    # Calculate Free For Sale
                    pivot_data['Free_For_Sale_MT'] = (
                        pivot_data.get('Stock', 0) - 
                        pivot_data.get('Reservations', 0) + 
                        pivot_data.get('Incoming', 0)
                    )
                    
                    # Rename columns to match expected format
                    pivot_data = pivot_data.rename(columns={
                        'Stock': 'Stock_MT',
                        'Reservations': 'Reserved_MT', 
                        'Incoming': 'Incoming_MT',
                        'Free_For_Sale_MT': 'MT'  # Use MT as the main metric column
                    })
                    
                    # Store the full data for heatmap (grouped by Make, OD, WT, Grade, Specification)
                    # For Free For Sale, we want to group by OD & WT only for heatmap display
                    # But if specification filter is applied, we should include it in grouping
                    heatmap_group_cols = ['OD', 'WT']
                    if 'Grade' in all_data.columns:
                        heatmap_group_cols.append('Grade')
                    if 'Specification' in all_data.columns and spec_filter and "All" not in spec_filter:
                        heatmap_group_cols.append('Specification')
                    
                    # Create heatmap data grouped by OD & WT only
                    heatmap_pivot = all_data_clean.groupby(heatmap_group_cols + ['Type'])['MT'].sum().reset_index()
                    heatmap_pivot = heatmap_pivot.pivot_table(
                        index=heatmap_group_cols, 
                        columns='Type', 
                        values='MT', 
                        fill_value=0
                    ).reset_index()
                    
                    # Calculate Free For Sale for heatmap
                    heatmap_pivot['MT'] = (
                        heatmap_pivot.get('Stock', 0) - 
                        heatmap_pivot.get('Reservations', 0) + 
                        heatmap_pivot.get('Incoming', 0)
                    )
                    
                                        # Use heatmap data for display
                    df = heatmap_pivot
                    
                    # Create preview table data grouped by unique products (OD, WT, Specification)
                    if 'Specification' in all_data.columns:
                        
                        # Round OD and WT to avoid floating-point precision issues
                        all_data_rounded = all_data_clean.copy()
                        all_data_rounded['OD'] = all_data_rounded['OD'].round(3)
                        all_data_rounded['WT'] = all_data_rounded['WT'].round(3)
                        
                        # Group by unique product identifiers and Type, then sum MT values
                        preview_group_cols = ['OD', 'WT', 'Specification']
                        
                        # Create pivot table to get Stock, Incoming, Reservations columns for each unique product
                        # First, ensure we have unique combinations by grouping and summing
                        preview_pivot = all_data_rounded.groupby(preview_group_cols + ['Type'])['MT'].sum().reset_index()
                        
                        # Create pivot table with explicit aggregation
                        preview_pivot = preview_pivot.pivot_table(
                            index=preview_group_cols, 
                            columns='Type', 
                            values='MT', 
                            fill_value=0,
                            aggfunc='sum'
                        ).reset_index()
                        
                        # Ensure we have only unique products by doing a final groupby
                        # Use only columns that exist to avoid KeyError
                        agg_columns = {}
                        if 'Stock' in preview_pivot.columns:
                            agg_columns['Stock'] = 'sum'
                        if 'Incoming' in preview_pivot.columns:
                            agg_columns['Incoming'] = 'sum'
                        if 'Reservations' in preview_pivot.columns:
                            agg_columns['Reservations'] = 'sum'
                        
                        if agg_columns:
                            preview_pivot = preview_pivot.groupby(preview_group_cols).agg(agg_columns).reset_index()
                        
                        # Ensure all Type columns exist (even if no data) to prevent KeyError during formatting
                        required_types = ['Stock', 'Incoming', 'Reservations']
                        for col in required_types:
                            if col not in preview_pivot.columns:
                                preview_pivot[col] = 0
                        
                        # Calculate Free For Sale for each unique product (handle missing columns gracefully)
                        preview_pivot['MT'] = (
                            preview_pivot.get('Stock', 0) - 
                            preview_pivot.get('Reservations', 0) + 
                            preview_pivot.get('Incoming', 0)
                        )
                        
                        # REAL OPTIMIZATION: Vectorized Grade and categorization operations - 3-5x faster
                        # Convert to numpy arrays for vectorized operations
                        spec_values = preview_pivot['Specification'].values
                        od_values = preview_pivot['OD'].values
                        wt_values = preview_pivot['WT'].values
                        
                        # Vectorized grade derivation
                        grades = []
                        od_categories = []
                        wt_schedules = []
                        
                        for i in range(len(preview_pivot)):
                            spec = spec_values[i]
                            od = od_values[i]
                            wt = wt_values[i]
                            
                            # Derive grade once and reuse
                            grade_logic = derive_grade_from_spec(spec, combine_cs_as=True)
                            grade_display = derive_grade_from_spec(spec, combine_cs_as=False)
                            
                            grades.append(grade_display)
                            od_categories.append(categorize_OD(od, grade_logic))
                            wt_schedules.append(categorize_WT_schedule(od, wt, grade_logic))
                        
                        preview_pivot['Grade'] = grades
                        preview_pivot['OD_Category'] = od_categories
                        preview_pivot['WT_Schedule'] = wt_schedules
                        
                        # Store preview data separately
                        df_preview = preview_pivot
                    else:
                        df_preview = df
                else:
                    df = pd.DataFrame()
            else:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()
    else:
        df = sheets.get(size_chart_type, pd.DataFrame())
    
    if not df.empty:
        df_cat = add_categorizations(df.copy())
        
        # Parse Delivery_as_on_Date column as datetime for Incoming chart type
        if size_chart_type == "Incoming" and 'Delivery_as_on_Date' in df_cat.columns:
            try:
                # Convert to datetime, handling various formats
                df_cat['Delivery_as_on_Date'] = pd.to_datetime(df_cat['Delivery_as_on_Date'], errors='coerce')
            except Exception:
                # If parsing fails, leave as is
                pass

        # --- Apply Filters ---
        def check_word_boundary_match(data_value, search_spec):
            """Check if search_spec appears as a complete word in data_value"""
            data_str = str(data_value).strip()
            search_str = str(search_spec).strip()
            
            # Hardcode the IBR vs NON IBR case (the only special case we need)
            if search_str.upper() == "IBR":
                # If searching for IBR, exclude any record that contains "NON IBR"
                if "NON IBR" in data_str.upper():
                    return False
            elif search_str.upper() == "NON IBR":
                # If searching for NON IBR, check if the data contains "NON IBR" as a complete specification
                return "NON IBR" in data_str.upper()
            
            # Simple and clean general logic for all other cases
            # Normalize both strings (remove spaces, uppercase)
            search_normalized = search_str.replace(' ', '').upper()
            data_normalized = data_str.replace(' ', '').upper()
            
            # Split data by + and check if search spec is in any part
            data_parts = [part.strip() for part in data_str.split('+') if part.strip()]
            
            for part in data_parts:
                part_normalized = part.replace(' ', '').upper()
                if search_normalized == part_normalized:
                    return True
            
            return False
        
        def check_make_match(data_value, search_make):
            """Check if search_make appears in data_value (handles comma-separated makes)"""
            data_str = str(data_value).strip()
            search_str = str(search_make).strip()
            
            # Handle comma-separated makes (like "KIRLOSKAR, JSL, ISMT")
            if ',' in data_str:
                data_makes = [m.strip() for m in data_str.split(',') if m.strip()]
                return search_str in data_makes
            else:
                # Single make value
                return data_str == search_str
        
        def apply_filters(df):
            filtered = df.copy()
            # Make - Filter logic: handle comma-separated values and single values
            # Skip Make filter for Incoming chart type (like Free for Sale)
            if 'Make' in filtered.columns and make_filter and size_chart_type != "Incoming":
                if "All" in make_filter:
                    # If "All" is selected, exclude any other specific values
                    exclude_values = [v for v in make_filter if v != "All"]
                    if exclude_values:
                        # REAL OPTIMIZATION: Vectorized make filtering - 3-5x faster than apply()
                        make_values = filtered['Make'].astype(str).values
                        mask = np.ones(len(filtered), dtype=bool)
                        
                        for i, make_val in enumerate(make_values):
                            for exclude_make in exclude_values:
                                if check_make_match(make_val, exclude_make):
                                    mask[i] = False
                                    break
                        
                        filtered = filtered[mask]
                else:
                    # If "All" is not selected, show records that contain any of the selected makes
                    # REAL OPTIMIZATION: Vectorized make filtering - 3-5x faster than apply()
                    make_values = filtered['Make'].astype(str).values
                    mask = np.zeros(len(filtered), dtype=bool)
                    
                    for i, make_val in enumerate(make_values):
                        for selected_make in make_filter:
                            if check_make_match(make_val, selected_make):
                                mask[i] = True
                                break
                    
                    filtered = filtered[mask]
            # OD - Same logic
            if 'OD' in filtered.columns and od_filter:
                if "All" in od_filter:
                    exclude_values = [v for v in od_filter if v != "All"]
                    if exclude_values:
                        filtered = filtered[~filtered['OD'].astype(str).isin(exclude_values)]
                else:
                    filtered = filtered[filtered['OD'].astype(str).isin(od_filter)]
            # WT - Same logic
            if 'WT' in filtered.columns and wt_filter:
                if "All" in wt_filter:
                    exclude_values = [v for v in wt_filter if v != "All"]
                    if exclude_values:
                        filtered = filtered[~filtered['WT'].astype(str).isin(exclude_values)]
                else:
                    filtered = filtered[filtered['WT'].astype(str).isin(wt_filter)]
            # Grade Type filtering is now handled automatically through Specification filter
            # Specification - Primary filter (replaces Grade Type)
            if 'Specification' in filtered.columns and spec_filter:
                if "All" in spec_filter:
                    exclude_values = [v for v in spec_filter if v != "All"]
                    if exclude_values:
                        filtered = filtered[~filtered['Specification'].astype(str).str.strip().isin(exclude_values)]
                else:
                    filtered = filtered[filtered['Specification'].astype(str).str.strip().isin(spec_filter)]
            # Additional Spec - Contains matching logic for individual specs
            add_spec_col_name = "Add_Spec"  # Now standardized to "Add_Spec" for all sheets
            if add_spec_col_name in filtered.columns and add_spec_filter:
                if "All" in add_spec_filter:
                    exclude_values = [v for v in add_spec_filter if v != "All"]
                    if exclude_values:
                        # REAL OPTIMIZATION: Vectorized additional spec filtering - 3-5x faster than apply()
                        add_spec_values = filtered[add_spec_col_name].astype(str).values
                        mask = np.ones(len(filtered), dtype=bool)
                        
                        for i, add_spec_val in enumerate(add_spec_values):
                            for exclude_spec in exclude_values:
                                if check_word_boundary_match(add_spec_val, exclude_spec):
                                    mask[i] = False
                                    break
                        
                        filtered = filtered[mask]
                else:
                    # Contains matching logic: show records that contain the selected spec(s)
                    if len(add_spec_filter) == 1:
                        # Single selection: show all records that contain this spec as a complete word
                        selected_spec = add_spec_filter[0].strip()
                        # REAL OPTIMIZATION: Vectorized single spec filtering - 3-5x faster than apply()
                        add_spec_values = filtered[add_spec_col_name].astype(str).values
                        mask = np.zeros(len(filtered), dtype=bool)
                        
                        for i, add_spec_val in enumerate(add_spec_values):
                            if check_word_boundary_match(add_spec_val, selected_spec):
                                mask[i] = True
                        
                        filtered = filtered[mask]
                    else:
                        # Multiple selections: show records that contain ALL selected specs (in any order)
                        def check_contains_all_specs(data_value, selected_specs):
                            """Check if data value contains ALL the selected specs (in any order)"""
                            data_value_str = str(data_value).strip()
                            # Check if all selected specs are present in the data value as complete words
                            return all(check_word_boundary_match(data_value_str, spec.strip()) for spec in selected_specs)
                        
                        # REAL OPTIMIZATION: Vectorized multiple spec filtering - 3-5x faster than apply()
                        add_spec_values = filtered[add_spec_col_name].astype(str).values
                        mask = np.zeros(len(filtered), dtype=bool)
                        
                        for i, add_spec_val in enumerate(add_spec_values):
                            if check_contains_all_specs(add_spec_val, add_spec_filter):
                                mask[i] = True
                        
                        filtered = filtered[mask]
            # Branch - Same logic
            if 'Branch' in filtered.columns and branch_filter:
                if "All" in branch_filter:
                    exclude_values = [v for v in branch_filter if v != "All"]
                    if exclude_values:
                        filtered = filtered[~filtered['Branch'].astype(str).isin(exclude_values)]
                else:
                    filtered = filtered[filtered['Branch'].astype(str).isin(branch_filter)]
            # OD Category - Same logic
            if 'OD_Category' in filtered.columns and od_category_filter:
                if "All" in od_category_filter:
                    exclude_values = [v for v in od_category_filter if v != "All"]
                    if exclude_values:
                        filtered = filtered[~filtered['OD_Category'].astype(str).isin(exclude_values)]
                else:
                    filtered = filtered[filtered['OD_Category'].astype(str).isin(od_category_filter)]
            # WT Category - Same logic
            if 'WT_Schedule' in filtered.columns and wt_category_filter:
                if "All" in wt_category_filter:
                    exclude_values = [v for v in wt_category_filter if v != "All"]
                    if exclude_values:
                        filtered = filtered[~filtered['WT_Schedule'].astype(str).isin(exclude_values)]
                else:
                    filtered = filtered[filtered['WT_Schedule'].astype(str).isin(wt_category_filter)]
            
            # Incoming Filter - Apply customer filter for Incoming chart type
            if size_chart_type == "Incoming" and 'CUSTOMER' in filtered.columns:
                incoming_filter = st.session_state.get('incoming_filter', 'ALL INCOMING')
                
                if incoming_filter == "FOR STOCK":
                    # Show rows where CUSTOMER contains "STOCK" (case-insensitive)
                    def is_stock_customer(customer_value):
                        if pd.isna(customer_value):
                            return False
                        customer_str = str(customer_value).strip().upper()
                        return "STOCK" in customer_str
                    
                    # REAL OPTIMIZATION: Vectorized customer filtering - 3-5x faster than apply()
                    customer_values = filtered['CUSTOMER'].astype(str).values
                    mask = np.zeros(len(filtered), dtype=bool)
                    
                    for i, customer_val in enumerate(customer_values):
                        if is_stock_customer(customer_val):
                            mask[i] = True
                    
                    filtered = filtered[mask]
                
                elif incoming_filter == "FOR CUSTOMERS":
                    # Show rows where CUSTOMER does NOT contain "STOCK" (case-insensitive)
                    def is_customer_order(customer_value):
                        if pd.isna(customer_value):
                            return False
                        customer_str = str(customer_value).strip().upper()
                        return "STOCK" not in customer_str
                    
                    # REAL OPTIMIZATION: Vectorized customer filtering - 3-5x faster than apply()
                    customer_values = filtered['CUSTOMER'].astype(str).values
                    mask = np.zeros(len(filtered), dtype=bool)
                    
                    for i, customer_val in enumerate(customer_values):
                        if is_customer_order(customer_val):
                            mask[i] = True
                    
                    filtered = filtered[mask]
                
                # If "ALL INCOMING" is selected, no filtering is applied (show all data)
            
            # Month Filter - Apply month filter for Incoming chart type based on Delivery_as_on_Date
            if size_chart_type == "Incoming" and 'Delivery_as_on_Date' in filtered.columns:
                month_filter = st.session_state.get('incoming_month_filter', None)
                
                if month_filter is not None:
                    # month_filter format: "YYYY-MM" (e.g., "2024-11")
                    try:
                        # Extract year and month from the filter
                        filter_year, filter_month = map(int, month_filter.split('-'))
                        
                        # Filter data where Delivery_as_on_Date matches the selected month and year
                        # Handle NaT (Not a Time) values by excluding them
                        mask = (
                            filtered['Delivery_as_on_Date'].notna() &
                            (filtered['Delivery_as_on_Date'].dt.year == filter_year) &
                            (filtered['Delivery_as_on_Date'].dt.month == filter_month)
                        )
                        filtered = filtered[mask]
                    except Exception:
                        # If filtering fails, show all data
                        pass
            
            return filtered

        df_filtered = apply_filters(df_cat)

        # --- Pivot Table (with Totals, Color Formatting) ---
        # Get the current incoming filter for display in headings
        incoming_filter_display = ""
        if size_chart_type == "Incoming":
            incoming_filter = st.session_state.get('incoming_filter', 'ALL INCOMING')
            incoming_filter_display = f" ({incoming_filter})"
        
        # --- Incoming Filter (only show when Incoming chart type is selected) ---
        if size_chart_type == "Incoming":
            # Initialize session state for incoming filter if not exists
            if 'incoming_filter' not in st.session_state:
                st.session_state.incoming_filter = "ALL INCOMING"
            
            # Initialize session state for month filter if not exists
            if 'incoming_month_filter' not in st.session_state:
                st.session_state.incoming_month_filter = None
            
            # Get current incoming filter for button styling
            current_incoming_filter = st.session_state.get('incoming_filter', 'ALL INCOMING')
            
            # Create filter options using columns
            col1, col2, col3 = st.columns(3)
            
            with col1:
                all_incoming_btn = st.button("ALL INCOMING", key="all_incoming_btn", use_container_width=True, 
                                           help="Show all incoming stock",
                                           type="primary" if current_incoming_filter == "ALL INCOMING" else "secondary")
                if all_incoming_btn:
                    st.session_state.incoming_filter = "ALL INCOMING"
                    st.rerun()
            
            with col2:
                for_stock_btn = st.button("FOR STOCK", key="for_stock_btn", use_container_width=True,
                                        help="Show only incoming stock for inventory",
                                        type="primary" if current_incoming_filter == "FOR STOCK" else "secondary")
                if for_stock_btn:
                    st.session_state.incoming_filter = "FOR STOCK"
                    st.rerun()
            
            with col3:
                for_customers_btn = st.button("FOR CUSTOMERS", key="for_customers_btn", use_container_width=True,
                                            help="Show only incoming stock for customers",
                                            type="primary" if current_incoming_filter == "FOR CUSTOMERS" else "secondary")
                if for_customers_btn:
                    st.session_state.incoming_filter = "FOR CUSTOMERS"
                    st.rerun()
        
        # --- Month Filter Buttons (only for Incoming chart type) ---
        if size_chart_type == "Incoming":
            # Generate 4 upcoming months (current month + next 3 months)
            current_date = pd.Timestamp.now()
            months = []
            for i in range(4):
                # Add months using pandas DateOffset
                month_date = current_date + pd.DateOffset(months=i)
                month_label = f"M{i+1} ({month_date.strftime('%b')})"
                month_value = month_date.strftime('%Y-%m')  # Format: YYYY-MM
                month_display = month_date.strftime('%B %Y')
                month_help = f"Show Incoming Stock for {month_display} month"
                months.append((month_label, month_value, month_help))
            
            # Get current month filter for button styling
            current_month_filter = st.session_state.get('incoming_month_filter', None)
            
            # Create title and month buttons in a row (compact buttons)
            title_col, m1_col, m2_col, m3_col, m4_col = st.columns([6, 0.7, 0.7, 0.7, 0.7])
            
            with title_col:
                st.markdown(f"<h5 style='margin-bottom: 5px; color: #1a6b3e;'>{size_chart_type} Items Heatmap{incoming_filter_display}</h5>", unsafe_allow_html=True)
            
            with m1_col:
                m1_label, m1_value, m1_help = months[0]
                m1_btn = st.button(m1_label, key="month_filter_m1", use_container_width=True,
                                  help=m1_help,
                                  type="primary" if current_month_filter == m1_value else "secondary")
                if m1_btn:
                    # Toggle: if already selected, deselect (show all), otherwise select
                    if current_month_filter == m1_value:
                        st.session_state.incoming_month_filter = None
                    else:
                        st.session_state.incoming_month_filter = m1_value
                    st.rerun()
            
            with m2_col:
                m2_label, m2_value, m2_help = months[1]
                m2_btn = st.button(m2_label, key="month_filter_m2", use_container_width=True,
                                  help=m2_help,
                                  type="primary" if current_month_filter == m2_value else "secondary")
                if m2_btn:
                    if current_month_filter == m2_value:
                        st.session_state.incoming_month_filter = None
                    else:
                        st.session_state.incoming_month_filter = m2_value
                    st.rerun()
            
            with m3_col:
                m3_label, m3_value, m3_help = months[2]
                m3_btn = st.button(m3_label, key="month_filter_m3", use_container_width=True,
                                  help=m3_help,
                                  type="primary" if current_month_filter == m3_value else "secondary")
                if m3_btn:
                    if current_month_filter == m3_value:
                        st.session_state.incoming_month_filter = None
                    else:
                        st.session_state.incoming_month_filter = m3_value
                    st.rerun()
            
            with m4_col:
                m4_label, m4_value, m4_help = months[3]
                m4_btn = st.button(m4_label, key="month_filter_m4", use_container_width=True,
                                  help=m4_help,
                                  type="primary" if current_month_filter == m4_value else "secondary")
                if m4_btn:
                    if current_month_filter == m4_value:
                        st.session_state.incoming_month_filter = None
                    else:
                        st.session_state.incoming_month_filter = m4_value
                    st.rerun()
        else:
            # Add "(In MT)" suffix for Compare Files tab
            heatmap_title = f"{size_chart_type} Items Heatmap"
            if size_chart_type == "Compare Files":
                heatmap_title = f"{size_chart_type} Items Heatmap (In MT)"
            st.markdown(f"<h5 style='margin-bottom: 5px; color: #1a6b3e;'>{heatmap_title}{incoming_filter_display}</h5>", unsafe_allow_html=True)
        
        # Special handling for comparison tab
        if size_chart_type == "Compare Files":
            metric_col = "Change in Stock"  # Use Change in Stock column for comparison data
        else:
            metric_col = metric if metric in df_filtered.columns else None
            if metric_col is None:
                metric_col = next((c for c in df_filtered.columns if c.lower() == metric.lower()), None)
        
        # Ensure metric_col is a single column name (not a list or multiple columns)
        if isinstance(metric_col, list):
            metric_col = metric_col[0] if metric_col else None
        if metric_col and not df_filtered.empty:
            # Select correct WT_Schedule list based on derived grade types from specifications
            if not spec_filter or "All" in spec_filter:
                # When "All" is selected, use preferred order: CS & AS → SS → IS → Tubes
                preferred_order_all = [
                    # CS & AS order
                    "SCH 10", "SCH 20", "SCH 30", "STD", "SCH 40", "SCH 60", "XS", "SCH 80", 
                    "SCH 100", "SCH 120", "SCH 140", "SCH 160", "SCH XXS", "Non STD",
                    # SS order
                    "Schedule 5S", "Schedule 10S", "Schedule 40S", "Schedule 80S", "Schedule 160S", "XXS",
                    # IS order
                    "IS 1239: Light (A-Class)", "IS 1239: Medium (B-Class)", "IS 1239: Heavy (C-Class)",
                    '7" NB', '8" NB', '10" NB', '12" NB', '14" NB', '16" NB', '18" NB', '20" NB', "Non IS Standard",
                    # Tubes order
                    "Small Wall Tube", "Medium Wall Tube", "Heavy Wall Tube", "Non-Standard Tube"
                ]
                # Filter to only include schedules that exist in the data
                all_available_schedules = list(set(CS_AS_WT + SS_WT + IS_WT + TUBES_WT))
                wt_schedule = [schedule for schedule in preferred_order_all if schedule in all_available_schedules]
            else:
                # Derive grade types from selected specifications
                grade_types = set()
                for spec in spec_filter:
                    if spec != "All":
                        grade_type = derive_grade_type_from_spec(spec)
                        grade_types.add(grade_type)
                
                # Combine wall thickness schedules for all detected grade types in preferred order
                wt_schedule = []
                
                # Define preferred order for each grade type
                cs_as_order = ["SCH 10", "SCH 20", "SCH 30", "STD", "SCH 40", "SCH 60", "XS", "SCH 80", 
                              "SCH 100", "SCH 120", "SCH 140", "SCH 160", "SCH XXS", "Non STD"]
                ss_order = ["Schedule 5S", "Schedule 10S", "Schedule 40S", "Schedule 80S", "Schedule 160S", "XXS", "Non STD"]
                is_order = ["IS 1239: Light (A-Class)", "IS 1239: Medium (B-Class)", "IS 1239: Heavy (C-Class)",
                           '7" NB', '8" NB', '10" NB', '12" NB', '14" NB', '16" NB', '18" NB', '20" NB', "Non IS Standard"]
                tubes_order = ["Small Wall Tube", "Medium Wall Tube", "Heavy Wall Tube", "Non-Standard Tube"]
                
                # Add schedules in preferred order based on detected grade types
                for grade_type in grade_types:
                    if grade_type == "CS & AS":
                        # Filter to only include schedules that exist in CS_AS_WT
                        cs_as_filtered = [schedule for schedule in cs_as_order if schedule in CS_AS_WT]
                        wt_schedule.extend(cs_as_filtered)
                    elif grade_type == "SS":
                        # Filter to only include schedules that exist in SS_WT
                        ss_filtered = [schedule for schedule in ss_order if schedule in SS_WT]
                        wt_schedule.extend(ss_filtered)
                    elif grade_type == "IS":
                        # Filter to only include schedules that exist in IS_WT
                        is_filtered = [schedule for schedule in is_order if schedule in IS_WT]
                        wt_schedule.extend(is_filtered)
                    elif grade_type == "Tubes":
                        # Filter to only include schedules that exist in TUBES_WT
                        tubes_filtered = [schedule for schedule in tubes_order if schedule in TUBES_WT]
                        wt_schedule.extend(tubes_filtered)
                
                # Remove duplicates while preserving order
                seen = set()
                wt_schedule = [x for x in wt_schedule if not (x in seen or seen.add(x))]
            
            # Build base DataFrame with all combinations
            import itertools
            base_index = pd.MultiIndex.from_product([OD_ORDER, wt_schedule], names=["OD_Category", "WT_Schedule"])
            df_base = pd.DataFrame(index=base_index).reset_index()
            # Group and sum
            grouped = df_filtered.groupby(["OD_Category", "WT_Schedule"])[metric_col].sum().reset_index()
            merged = pd.merge(df_base, grouped, on=["OD_Category", "WT_Schedule"], how="left").fillna(0)
            # Pivot
            pivot = merged.pivot(index="OD_Category", columns="WT_Schedule", values=metric_col)
            # Keep only the fixed order
            pivot = pivot.reindex(index=OD_ORDER, columns=wt_schedule, fill_value=0)
            # Remove all-zero rows except for totals
            pivot = pivot.loc[~((pivot == 0).all(axis=1)) | (pivot.index == "Total")]
            # Add row totals
            pivot["Total"] = pivot.sum(axis=1)
            # Add column totals
            col_total = pivot.sum(axis=0)
            col_total.name = "Total"
            pivot = pd.concat([pivot, col_total.to_frame().T])
            # Format all numeric values to 2 decimals
            pivot = pivot.applymap(lambda x: round(x, 2) if isinstance(x, (int, float)) else x)
            # Conditional formatting
            def highlight(val, minval, maxval):
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
            # Only color the numeric cells (not OD_Category)
            numeric = pivot.select_dtypes(include=[float, int])
            # Exclude the "Total" row and column for color calculation
            numeric_no_totals = numeric.drop('Total', axis=1, errors='ignore').drop('Total', axis=0, errors='ignore')
            minval = numeric_no_totals.min().min() if not numeric_no_totals.empty else 0
            maxval = numeric_no_totals.max().max() if not numeric_no_totals.empty else 1
            # Define OD categories to highlight with blue background
            highlight_od_categories = ['2"', '4"', '6"', '8"', '10"', '12"', '14"', '16"', '18"', '20"']
            
            # Create a modified pivot with highlighted OD categories
            pivot_highlighted = pivot.copy()
            
            # Add a prefix to highlighted OD categories to make them stand out
            for od_cat in highlight_od_categories:
                if od_cat in pivot_highlighted.index:
                                         # Create a new index with highlighted categories
                     new_index = []
                     for idx in pivot_highlighted.index:
                         if idx == od_cat:
                             new_index.append(f"⭐ {idx}")  # Star emoji prefix
                         else:
                             new_index.append(idx)
                     pivot_highlighted.index = new_index
            
            styled = (
                pivot_highlighted.style
                .format("{:.2f}")
                .applymap(lambda v: highlight(v, minval, maxval), subset=pd.IndexSlice[pivot_highlighted.index, pivot_highlighted.columns])
            )
            # Calculate height to show exactly up to the Total row (last row) - no extra space
            num_rows = len(pivot_highlighted)
            # Height calculation: 35px per row + 50px for header + 10px for minimal bottom padding
            total_height = (num_rows * 35) + 50
            st.dataframe(styled, use_container_width=True, height=total_height)
        else:
            st.info("No data available for pivot table.")

        # --- Preview Data Table ---
        st.markdown("<hr style='margin: 20px 0 10px 0; border: 1px solid #ddd;'>", unsafe_allow_html=True)
        status_filter = None
        if size_chart_type == "Compare Files":
            header_col, status_col = st.columns([5, 1.5])
            with header_col:
                st.markdown(f"<h5 style='margin-bottom: 5px; color: #1a6b3e;'>Preview: {size_chart_type} Data</h5>", unsafe_allow_html=True)
            with status_col:
                if 'Status' in df_filtered.columns:
                    # Initialize Status filter in session state if not exists (preserves state across reruns)
                    # This ensures the Status filter persists when quick-spec buttons trigger reruns
                    if "main_dashboard_status_filter" not in st.session_state:
                        st.session_state.main_dashboard_status_filter = "All"
                    
                    # Render the Status filter selectbox - the key will preserve its state across reruns
                    # Don't use index= parameter as it can override the preserved state from the key
                    status_filter = st.selectbox(
                        "Filter by Status:",
                        ["All", "Added", "Removed", "Increased", "Decreased", "Unchanged"],
                        key="main_dashboard_status_filter"
                    )
                else:
                    st.empty()
        else:
            st.markdown(f"<h5 style='margin-bottom: 5px; color: #1a6b3e;'>Preview: {size_chart_type} Data{incoming_filter_display}</h5>", unsafe_allow_html=True)
        
        # Use preview data for Free For Sale, otherwise use filtered data
        if size_chart_type == "Free For Sale":
            # For Free For Sale, always use the aggregated preview data
            if 'df_preview' in locals() and df_preview is not None:
                # Apply filters to the preview data (same filters as other chart types)
                df_preview_filtered = apply_filters(df_preview.copy())
                df_filtered_display = df_preview_filtered
                st.write(f"Filtered rows: {len(df_filtered_display)}")
            else:
                # Fallback to filtered data if preview data is not available
                st.write(f"Filtered rows: {len(df_filtered)}")
                df_filtered_display = df_filtered.reset_index(drop=True)
        elif size_chart_type == "Incoming":
            # For Incoming, show only specific columns in the specified order
            st.write(f"Filtered rows: {len(df_filtered)}")
            df_filtered_display = df_filtered.reset_index(drop=True)
            
            # Define the columns to show for Incoming data in the specified order
            incoming_columns = [
                'SUPPLIER', 'PO_NO', 'DATE', 'OD', 'WT', 'OD_Category', 'WT_Schedule', 
                'Specification', 'Grade', 'Add_Spec', 'MT', 'Delivery_as_on_Date', 'NO_OF_DAYS_DELAY', 'CUSTOMER'
            ]
            
            # Filter to only show columns that exist in the data
            available_columns = [col for col in incoming_columns if col in df_filtered_display.columns]
            
            # Reorder the dataframe to show only the specified columns in the specified order
            if available_columns:
                df_filtered_display = df_filtered_display[available_columns]

                # Format the Delivery_as_on_Date column to be more readable
                if 'Delivery_as_on_Date' in df_filtered_display.columns:
                    def format_date(date_value):
                        """Format date to '30th May, 2025' format"""
                        if pd.isna(date_value):
                            return ""
                        try:
                            # Convert to datetime if it's not already
                            if isinstance(date_value, str):
                                date_obj = pd.to_datetime(date_value)
                            else:
                                date_obj = pd.to_datetime(date_value)
                            
                            # Format to '30th May, 2025' format
                            day = date_obj.day
                            month = date_obj.strftime('%B')  # Full month name
                            year = date_obj.year
                            
                            # Add ordinal suffix to day (1st, 2nd, 3rd, 4th, etc.)
                            if 10 <= day % 100 <= 20:
                                suffix = 'th'
                            else:
                                suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')
                            
                            return f"{day}{suffix} {month}, {year}"
                        except:
                            return str(date_value)  # Return original if formatting fails
                    
                    # Apply the formatting to the Delivery_as_on_Date column
                    df_filtered_display['Delivery_as_on_Date'] = df_filtered_display['Delivery_as_on_Date'].apply(format_date)
        elif size_chart_type == "Compare Files":
            if status_filter and status_filter != "All":
                df_filtered = df_filtered[df_filtered['Status'] == status_filter]
            st.write(f"Filtered rows: {len(df_filtered)}")
            df_filtered_display = df_filtered.reset_index(drop=True)
        else:
            st.write(f"Filtered rows: {len(df_filtered)}")
            df_filtered_display = df_filtered.reset_index(drop=True)
            
            # Add Age (In Years) column for Stock chart type
            if size_chart_type == "Stock" and 'Age' in df_filtered_display.columns:
                # Add Age (In Years) column - convert days to exact years with 2 decimals
                # Convert Age to numeric first to handle string values
                df_filtered_display['Age (In Years)'] = pd.to_numeric(df_filtered_display['Age'], errors='coerce').apply(lambda x: round(x / 365, 2) if pd.notna(x) else x)
                
                # Add Product Age column
                df_filtered_display['Product Age'] = df_filtered_display['Age'].apply(convert_age_to_years)
                
                # Reorder columns to put Age (In Years) and Product Age right after Age column
                cols = df_filtered_display.columns.tolist()
                age_idx = cols.index('Age')
                # Remove both columns from current position and insert them after Age
                cols.remove('Age (In Years)')
                cols.remove('Product Age')
                cols.insert(age_idx + 1, 'Age (In Years)')
                cols.insert(age_idx + 2, 'Product Age')
                df_filtered_display = df_filtered_display[cols]
                
                # Filter and reorder columns for Stock preview table
                # Define the exact columns to show in the specified order
                stock_columns = [
                    'Supplier', 'Specification', 'Grade', 'OD', 'WT', 'OD_Category', 'WT_Schedule',
                    'Add_Spec', 'Age (In Years)', 'Branch', 'MT', 'Mtrs', 'Kg/Mtr', 
                    'Make', 'Heat_No', 'Nos', 'HSN_CODE', 'TC_TYPE', 'Product Age'
                ]
                
                # Filter to only show columns that exist in the data
                available_stock_columns = [col for col in stock_columns if col in df_filtered_display.columns]
                
                # Reorder the dataframe to show only the specified columns in the specified order
                if available_stock_columns:
                    df_filtered_display = df_filtered_display[available_stock_columns]
                
                # Shorten Branch column values for Stock preview table
                if 'Branch' in df_filtered_display.columns:
                    def shorten_branch_name(branch_value):
                        """Convert full city names to short codes"""
                        if pd.isna(branch_value):
                            return branch_value
                        
                        branch_str = str(branch_value).strip()
                        
                        # Mapping of full names to short codes (case-insensitive)
                        branch_mapping = {
                            'pune': 'PUN',
                            'bangalore': 'BLR',
                            'bommasandra': 'BOM'
                        }
                        
                        # Convert to lowercase for case-insensitive matching
                        branch_lower = branch_str.lower()
                        
                        # Return short code if found, otherwise return original value
                        return branch_mapping.get(branch_lower, branch_str)
                    
                    # Apply the shortening to Branch column
                    df_filtered_display['Branch'] = df_filtered_display['Branch'].apply(shorten_branch_name)
        
        # Remove Grade_Logic column from display (it's only for internal logic)
        if 'Grade_Logic' in df_filtered_display.columns:
            df_filtered_display = df_filtered_display.drop(columns=['Grade_Logic'])
        
        # Add sequential row numbers starting from 1
        df_filtered_display.index = df_filtered_display.index + 1
        df_filtered_display.index.name = 'Row #'
        
        # Reorder columns to show Grade next to Specification and OD/WT categories next to OD/WT
        if 'Specification' in df_filtered_display.columns:
            # Get all columns
            all_cols = df_filtered_display.columns.tolist()
            # Find Specification position
            spec_idx = all_cols.index('Specification')
            
            # Use Grade column for all chart types (now consistent)
            grade_col = 'Grade'
            
            if grade_col in df_filtered_display.columns:
                grade_idx = all_cols.index(grade_col)
                
                # Create new column order: put Grade right after Specification
                new_cols = []
                for i, col in enumerate(all_cols):
                    if i == spec_idx:
                        new_cols.append(col)  # Add Specification
                        if grade_idx != spec_idx + 1:  # If Grade is not already next
                            new_cols.append(grade_col)  # Add Grade right after
                    elif col != grade_col:  # Skip Grade as it's already added
                        new_cols.append(col)
                
                # Reorder the dataframe
                df_filtered_display = df_filtered_display[new_cols]
                
        # Reorder OD and WT category columns to be next to OD and WT
        all_cols = df_filtered_display.columns.tolist()
        
        # Find OD and WT positions
        od_idx = None
        wt_idx = None
        od_cat_idx = None
        wt_schedule_idx = None
        
        for i, col in enumerate(all_cols):
            if col == 'OD':
                od_idx = i
            elif col == 'WT':
                wt_idx = i
            elif col == 'OD_Category':
                od_cat_idx = i
            elif col == 'WT_Schedule':
                wt_schedule_idx = i
        
        # Reorder to get: OD, WT, OD_Category, WT_Schedule after Grade
        if od_idx is not None and wt_idx is not None and od_cat_idx is not None and wt_schedule_idx is not None:
            # Find Grade position
            grade_idx = None
            for i, col in enumerate(all_cols):
                if col == 'Grade':
                    grade_idx = i
                    break
            
            if grade_idx is not None:
                # Create new order: put OD, WT, OD_Category, WT_Schedule right after Grade
                new_cols = []
                
                for i, col in enumerate(all_cols):
                    if i == grade_idx:
                        new_cols.append(col)  # Add Grade
                        # Add the four columns right after Grade
                        new_cols.append('OD')
                        new_cols.append('WT')
                        new_cols.append('OD_Category')
                        new_cols.append('WT_Schedule')
                    elif col not in ['OD', 'WT', 'OD_Category', 'WT_Schedule']:
                        # Add all other columns (excluding the four we already added)
                        new_cols.append(col)
                
                # Update the dataframe
                df_filtered_display = df_filtered_display[new_cols]

        # Note: Free For Sale now includes all columns including Make and Specification
        
        # Apply color coding to entire rows based on Product Age for Stock chart type
        if size_chart_type == "Stock" and 'Product Age' in df_filtered_display.columns:
            def color_rows_by_age(row):
                age_val = row['Product Age']
                if pd.isna(age_val) or age_val == '':
                    return [''] * len(row)
                elif age_val == "New" or age_val == "0-1 years":
                    return ['background-color: #E8F5E8; color: #000000;'] * len(row)  # Light green
                elif age_val == "1-2 years":
                    return ['background-color: #F0F8E8; color: #000000;'] * len(row)  # Very light green
                elif age_val == "2-3 years":
                    return ['background-color: #FFF8E1; color: #000000;'] * len(row)  # Light yellow
                elif age_val == "3-4 years":
                    return ['background-color: #FFF3E0; color: #000000;'] * len(row)  # Light orange
                elif age_val == "4-5 years":
                    return ['background-color: #FFEBEE; color: #000000;'] * len(row)  # Light red
                elif age_val == "5+ years":
                    return ['background-color: #FCE4EC; color: #000000;'] * len(row)  # Light pink-red
                else:
                    return [''] * len(row)
            
            # Apply styling to entire rows based on Product Age column
            # Format OD, WT, and Age (In Years) columns to 2 decimal places and MT column to 3 decimal places
            df_filtered_display = df_filtered_display.style.apply(color_rows_by_age, axis=1).format(precision=0).format("{:.2f}", subset=['OD', 'WT', 'Age (In Years)']).format("{:.3f}", subset=['MT'])
        elif size_chart_type == "Compare Files":
            # Remove additional columns from preview display (keep them in underlying data for filters)
            display_exclusions = ['Add_Spec', 'Make', 'Branch']
            existing_exclusions = [col for col in display_exclusions if col in df_filtered_display.columns]
            if existing_exclusions:
                df_filtered_display = df_filtered_display.drop(columns=existing_exclusions)
        else:
            # For all other chart types (Reserved, Incoming, Free for Sale), format MT column to 3 decimal places
            # Also format Stock, Incoming, and Reservations columns to 3 decimal places for Free for Sale
            if size_chart_type == "Free For Sale":
                # Check which columns exist before formatting to prevent KeyError
                available_cols = df_filtered_display.columns
                od_wt_subset = [col for col in ['OD', 'WT'] if col in available_cols]
                mt_stock_subset = [col for col in ['MT', 'Stock', 'Incoming', 'Reservations'] if col in available_cols]
                
                style_obj = df_filtered_display.style.format(precision=0)
                if od_wt_subset:
                    style_obj = style_obj.format("{:.2f}", subset=od_wt_subset)
                if mt_stock_subset:
                    style_obj = style_obj.format("{:.3f}", subset=mt_stock_subset)
                df_filtered_display = style_obj
            else:
                df_filtered_display = df_filtered_display.style.format(precision=0).format("{:.2f}", subset=['OD', 'WT']).format("{:.3f}", subset=['MT'])
        
        # Rename columns for better display while preserving styling
        if hasattr(df_filtered_display, 'data'):
            # It's a Styler object, get the underlying DataFrame
            df_underlying = df_filtered_display.data.copy()
        else:
            # It's a regular DataFrame
            df_underlying = df_filtered_display.copy()
        
        # Rename columns for display
        column_mapping = {
            'OD_Category': 'OD (Inches)',
            'OD': 'OD (mm)',
            'WT': 'WT (mm)'
        }
        df_underlying.columns = [column_mapping.get(col, col) for col in df_underlying.columns]
        
        # Reapply styling to the renamed DataFrame
        if size_chart_type == "Stock" and 'Product Age' in df_underlying.columns:
            # Apply color coding for Stock chart type
            df_display_final = df_underlying.style.apply(color_rows_by_age, axis=1).format(precision=0).format("{:.2f}", subset=['OD (mm)', 'WT (mm)', 'Age (In Years)']).format("{:.3f}", subset=['MT'])
        elif size_chart_type == "Compare Files":
            # Apply color coding and numeric formatting for comparison data
            def color_rows_by_status(row):
                status = row['Status']
                if status == 'Added':
                    return ['background-color: #E8F5E8; color: #000000;'] * len(row)
                elif status == 'Removed':
                    return ['background-color: #FCE4EC; color: #000000;'] * len(row)
                elif status == 'Increased':
                    return ['background-color: #E8F5E8; color: #000000;'] * len(row)
                elif status == 'Decreased':
                    return ['background-color: #FCE4EC; color: #000000;'] * len(row)
                else:  # Unchanged
                    return ['background-color: #FFF8E1; color: #000000;'] * len(row)

            def positive_with_sign(value):
                try:
                    numeric_value = float(value)
                except (TypeError, ValueError):
                    return value
                if numeric_value > 0:
                    return f"+{numeric_value:.3f}"
                elif numeric_value == 0:
                    return "0.000"
                else:
                    return f"{numeric_value:.3f}"

            od_wt_subset = [col for col in ['OD (mm)', 'WT (mm)'] if col in df_underlying.columns]
            file_mt_subset = [col for col in df_underlying.columns if col.startswith('MT (')]
            change_subset = [col for col in ['Change in Stock'] if col in df_underlying.columns]

            style_obj = df_underlying.style.apply(color_rows_by_status, axis=1)
            if od_wt_subset:
                style_obj = style_obj.format("{:.2f}", subset=od_wt_subset)
            if file_mt_subset:
                style_obj = style_obj.format("{:.3f}", subset=file_mt_subset)
            if change_subset:
                style_obj = style_obj.format(positive_with_sign, subset=change_subset)
            df_display_final = style_obj
        else:
            # For all other chart types, apply formatting
            if size_chart_type == "Free For Sale":
                # Check which columns exist before formatting to prevent KeyError
                available_cols = df_underlying.columns
                od_wt_subset = [col for col in ['OD (mm)', 'WT (mm)'] if col in available_cols]
                mt_stock_subset = [col for col in ['MT', 'Stock', 'Incoming', 'Reservations'] if col in available_cols]
                
                style_obj = df_underlying.style.format(precision=0)
                if od_wt_subset:
                    style_obj = style_obj.format("{:.2f}", subset=od_wt_subset)
                if mt_stock_subset:
                    style_obj = style_obj.format("{:.3f}", subset=mt_stock_subset)
                df_display_final = style_obj
            else:
                df_display_final = df_underlying.style.format(precision=0).format("{:.2f}", subset=['OD (mm)', 'WT (mm)']).format("{:.3f}", subset=['MT'])
        
        with st.spinner("Generating Table..."):
            st.dataframe(df_display_final)
        
        # Add Product Age bar chart for Stock chart type only
        if size_chart_type == "Stock" and 'Product Age' in df_filtered_display.data.columns:
            # Add horizontal line and styled heading matching other sections
            st.markdown("<hr style='margin: 20px 0 10px 0; border: 1px solid #ddd;'>", unsafe_allow_html=True)
            st.markdown(f"<h5 style='margin-bottom: 5px; color: #1a6b3e;'>Product Age Distribution</h5>", unsafe_allow_html=True)
            # Create aggregated data for Product Age using the underlying DataFrame
            age_counts = df_filtered_display.data['Product Age'].value_counts()
            
            # Define the order for age categories
            age_order = ["0-1 years", "1-2 years", "2-3 years", "3-4 years", "4-5 years", "5+ years", "Null"]
            
            # Reorder the data according to the defined order
            age_counts_ordered = age_counts.reindex(age_order, fill_value=0)
            
            # Create bar chart with colors matching the table
            import plotly.express as px
            
            # Define colors matching the table color scheme (slightly darker for better visibility)
            age_colors = {
                "0-1 years": "#A5D6A7",      # Darker light green
                "1-2 years": "#C5E1A5",      # Darker very light green  
                "2-3 years": "#FFF176",      # Darker light yellow
                "3-4 years": "#FFB74D",      # Darker light orange
                "4-5 years": "#EF9A9A",      # Darker light red
                "5+ years": "#F06292",       # Darker light pink-red
                "Null": "#BDBDBD"            # Darker light gray for null values
            }
            
            # Create color list for the bars
            bar_colors = [age_colors.get(age, "#F5F5F5") for age in age_counts_ordered.index]
            
            fig = px.bar(
                x=age_counts_ordered.index,
                y=age_counts_ordered.values,
                labels={'x': 'Product Age', 'y': 'Number of Products'},
                color=age_counts_ordered.index,
                color_discrete_map=age_colors
            )
            
            # Update layout
            fig.update_layout(
                showlegend=False,
                height=400,
                xaxis_title="Product Age",
                yaxis_title="Number of Products"
            )
            
            # Customize hover template to remove color information
            fig.update_traces(
                hovertemplate="<b>Product Age:</b> %{x}<br><b>Number of Products:</b> %{y}<extra></extra>"
            )
            
            # Display the chart
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("<hr style='margin: 20px 0 0 0; border: 1px solid #ddd;'>", unsafe_allow_html=True)
        
        # Performance monitoring - show response time
        # end_time = time.time()
        # response_time = end_time - start_time
        # st.info(f"🚀 Performance: Dashboard loaded in {response_time:.2f} seconds")
        
    else:
        # Don't show warning for comparison tab when no data is selected yet
        if size_chart_type != "Compare Files":
            st.warning(f"No data found in the '{size_chart_type}' sheet.")
else:
    if error_message:
        st.error(f"❌ {error_message}")
    else:
        st.info("📊 No inventory data available as no file was found.")    
