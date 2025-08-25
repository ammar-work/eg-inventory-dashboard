import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import boto3
import io
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()  # this loads variables from .env into os.environ

# --- Token Authentication ---
# Check for authentication token
# params = st.query_params
# auth_token = params.get('auth_token', None)

# # Verify token
# if not auth_token or auth_token != st.secrets.get("SECRET_TOKEN"):
#     # Set page config for unauthorized access
#     st.set_page_config(page_title="Access Denied", layout="centered")
    
#     # Hide Streamlit branding on access denied page
#     st.markdown("""
#     <style>
#         #MainMenu {visibility: hidden;}
#         footer {visibility: hidden;}
#         header {visibility: hidden;}
#         .stDeployButton {display: none;}
#         .stApp > header {background-color: transparent;}
#         .stApp > footer {background-color: transparent;}
#         .stApp > .main > .block-container {padding-top: 1rem;}
#     </style>
#     """, unsafe_allow_html=True)
    
#     # Show unauthorized access message (clean, no logging visible to user)
#     st.markdown("""
#     <div style="text-align: center; padding: 100px 20px; font-family: Arial, sans-serif;">
#         <h1 style="color: #d32f2f; font-size: 48px; margin-bottom: 20px;">🚫</h1>
#         <h2 style="color: #d32f2f; font-size: 32px; margin-bottom: 20px;">Access Denied!</h2>
#         <p style="color: #666; font-size: 18px; line-height: 1.5;">You don't have access to view this page.</p>
#     </div>
#     """, unsafe_allow_html=True)
    
#     # Stop execution
#     st.stop()

# --- AWS S3 Configuration ---
# These should be set as environment variables for security
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# --- S3 Functions ---
def get_s3_client():
    """Get S3 client with error handling"""
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
    """Get the most recently uploaded .xlsx file from S3"""
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
def load_specification_mapping():
    """Load specification to grade type mapping from Excel file"""
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

# Hide Streamlit branding and elements
st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .stDeployButton {display: none;}
    .stApp > header {background-color: transparent;}
    .stApp > footer {background-color: transparent;}
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
st.sidebar.header("Controls")

# Show S3 status and refresh button
if data_file:
    st.sidebar.success("✅ Connected to S3 - Latest data loaded")
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
    # SCH 10
    if (od, wt) in [
        (10.3, 1.24), (13.7, 1.65), (17.1, 1.65), (21.3, 2.11), (26.7, 2.11), (33.4, 2.77),
        (42.2, 2.77), (48.3, 2.77), (60.3, 2.77), (73.0, 3.05), (88.9, 3.05), (101.6, 3.05),
        (114.3, 3.05), (141.3, 3.40), (168.3, 3.40), (219.1, 3.76), (273.0, 4.19), (273.1, 4.19),
        (323.8, 4.57), (355.6, 6.35), (406.4, 6.35), (457.0, 6.35), (508.0, 6.35), (559.0, 6.35),
        (610.0, 6.35), (609.6, 6.35)
    ]:
        return "SCH 10"
    # SCH 20
    if (od, wt) in [
        (219.1, 6.35), (273.0, 6.35), (273.1, 6.35), (323.8, 6.35), (323.8, 7.1),
        (355.6, 7.92), (406.4, 7.92), (457.0, 7.92), (508.0, 9.53), (559.0, 9.53),
        (610.0, 9.53), (609.6, 9.53)
    ]:
        return "SCH 20"
    # SCH 30
    if (od, wt) in [
        (21.3, 2.41), (26.7, 2.41), (33.4, 2.90), (42.2, 2.97), (48.3, 3.18), (60.3, 3.18),
        (73.0, 4.78), (88.9, 4.78), (101.6, 4.78), (114.3, 4.78), (219.1, 7.04), (273.0, 7.80),
        (273.1, 7.80), (323.8, 8.38), (355.6, 9.53), (406.4, 9.53), (457.0, 11.13), (508.0, 12.70),
        (559.0, 12.70), (610.0, 14.27), (609.6, 14.27)
    ]:
        return "SCH 30"
    # STD (Standard Weight) - Same as SCH 40 for NPS 1/8" to NPS 10"
    if (od, wt) in [
        (10.3, 1.73), (13.7, 2.24), (17.1, 2.31), (21.3, 2.77), (26.7, 2.87), (33.4, 3.38),
        (42.2, 3.56), (48.3, 3.68), (60.3, 3.91), (73.0, 5.16), (88.9, 5.49), (101.6, 5.74),
        (114.3, 6.02), (141.3, 6.55), (168.3, 7.11), (219.1, 8.18), (273.0, 9.27), (273.1, 9.27),
        (323.8, 9.53), (355.6, 9.53), (406.4, 9.53), (457.0, 9.53), (457.2, 9.53), (508.0, 9.53),
        (559.0, 9.53), (558.8, 9.53), (610.0, 9.53), (609.6, 9.53), (660.4, 9.53), (711.2, 9.53),
        (711, 9.53), (762, 9.53), (812.8, 9.53), (863.6, 9.53), (914.4, 9.53), (914, 9.53),
        (965.2, 9.53), (1016, 9.53), (1066.8, 9.53), (1117.6, 9.53), (1168.4, 9.53), (1219.2, 9.53),
        (1219, 12.70), (1524, 12.70)
    ]:
        return "STD"
    # SCH 40 - Same as STD for NPS 1/8" to NPS 10"
    if (od, wt) in [
        (10.3, 1.73), (13.7, 2.24), (17.1, 2.31), (21.3, 2.77), (26.7, 2.87), (33.4, 3.38),
        (42.2, 3.56), (48.3, 3.68), (60.3, 3.91), (73.0, 5.16), (88.9, 5.49), (101.6, 5.74),
        (114.3, 6.02), (141.3, 6.55), (168.3, 7.11), (219.1, 8.18), (273.0, 9.27), (273.1, 9.27),
        (323.8, 10.31), (355.6, 11.13), (355.6, 14.3), (406.4, 12.70), (457.0, 14.27), (508.0, 15.09),
        (610.0, 17.48), (609.6, 17.48)
    ]:
        return "SCH 40"
    # SCH 60
    if (od, wt) in [
        (219.1, 10.31), (273.0, 12.70), (273.1, 12.70), (323.8, 14.27), (355.6, 15.09),
        (406.4, 16.66), (457.0, 19.05), (457.0, 22.23), (508.0, 20.62), (559.0, 22.23),
        (610.0, 24.61), (609.6, 24.61)
    ]:
        return "SCH 60"
    # XS (Extra Strong) - Same as SCH 80 for NPS 1/8" to NPS 8"
    if (od, wt) in [
        (10.3, 2.41), (13.7, 3.02), (17.1, 3.20), (21.3, 3.73), (26.7, 3.91), (33.4, 4.55),
        (42.2, 4.85), (48.3, 5.08), (60.3, 5.54), (73.0, 7.01), (88.9, 7.62), (101.6, 8.08),
        (114.3, 8.56), (141.3, 9.53), (168.3, 10.97), (219.1, 12.70), (273.0, 12.70), (273.1, 12.70),
        (323.8, 12.70), (355.6, 12.70), (406.4, 12.70), (457.0, 12.70), (508.0, 12.70), (559.0, 12.70),
        (610.0, 12.70), (609.6, 12.70), (660.4, 12.70), (711.2, 12.70), (762, 12.70), (812.8, 12.70),
        (863.6, 12.70), (914.4, 12.70), (914, 12.70), (965.2, 12.70), (1016, 12.70), (1066.8, 12.70),
        (1117.6, 12.70), (1168.4, 12.70), (1219.2, 12.70), (1219, 12.70), (1524, 12.70)
    ]:
        return "XS"
    # SCH 80 - Same as XS for NPS 1/8" to NPS 8"
    if (od, wt) in [
        (10.3, 2.41), (13.7, 3.02), (17.1, 3.20), (21.3, 3.73), (26.7, 3.91), (33.4, 4.55),
        (42.2, 4.85), (48.3, 5.08), (60.3, 5.54), (73.0, 7.01), (88.9, 7.62), (101.6, 8.08),
        (114.3, 8.56), (141.3, 9.53), (168.3, 10.97), (219.1, 12.70), (273.0, 15.09), (273.1, 15.09),
        (323.8, 17.48), (355.6, 19.05), (406.4, 21.44), (406.4, 25.4), (457.0, 23.83), (508.0, 26.19),
        (559.0, 28.58), (610.0, 30.96), (609.6, 30.96)
    ]:
        return "SCH 80"
    # SCH 100
    if (od, wt) in [
        (219.1, 15.09), (273.0, 18.26), (273.1, 18.26), (323.8, 21.44), (355.6, 23.83),
        (406.4, 26.19), (457.0, 29.36), (508.0, 32.54), (559.0, 34.93), (610.0, 38.89), (609.6, 38.89)
    ]:
        return "SCH 100"
    # SCH 120
    if (od, wt) in [
        (114.3, 11.13), (141.3, 12.70), (168.3, 14.27), (219.1, 18.26), (273.0, 21.44),
        (273.1, 21.44), (323.8, 25.40), (355.6, 27.79), (406.4, 30.96), (457.0, 34.93),
        (508.0, 38.10), (559.0, 41.28), (610.0, 46.02), (609.6, 46.02)
    ]:
        return "SCH 120"
    # SCH 140
    if (od, wt) in [
        (219.1, 20.62), (273.0, 25.40), (273.1, 25.40), (323.8, 28.58), (355.6, 31.75),
        (406.4, 36.53), (457.0, 39.67), (508.0, 44.45), (559.0, 47.63), (610.0, 52.37), (609.6, 52.37)
    ]:
        return "SCH 140"
    # SCH 160
    if (od, wt) in [
        (21.3, 4.78), (26.7, 5.56), (33.4, 6.35), (42.2, 6.35), (48.3, 7.14), (60.3, 8.74),
        (73.0, 9.53), (88.9, 11.13), (114.3, 13.49), (141.3, 15.88), (168.3, 18.26), (219.1, 23.01),
        (273.0, 28.58), (273.1, 28.58), (273.1, 32), (323.8, 33.32), (355.6, 35.71), (406.4, 40.49),
        (457.0, 45.24), (508.0, 50.01), (559.0, 53.98), (610.0, 59.54), (609.6, 59.54)
    ]:
        return "SCH 160"
    # XXS (Double Extra Strong)
    if (od, wt) in [
        (10.3, 4.83), (13.7, 6.05), (17.1, 6.40), (21.3, 7.47), (26.7, 7.82), (33.4, 9.09),
        (42.2, 9.70), (48.3, 10.15), (60.3, 11.07), (73.0, 14.02), (88.9, 15.24), (114.3, 17.12),
        (141.3, 19.05), (168.3, 21.95), (219.1, 22.23), (273.0, 25.40), (273.1, 25.40), (323.8, 25.40)
    ]:
        return "SCH XXS"
    return "Non STD"

def categorize_stainless(od, wt):
    try:
        od = float(od)
        wt = float(wt)
    except:
        return "Non STD"
    # Schedule 5S
    if (od, wt) in [
        (10.3, 1.24), (13.7, 1.65), (17.1, 1.65), (21.3, 1.65), (26.7, 1.65), (33.4, 2.11),
        (42.2, 2.11), (48.3, 2.11), (60.3, 2.77), (73.0, 2.77), (88.9, 2.77), (114.3, 2.77),
        (141.3, 3.40), (168.3, 3.40), (219.1, 3.76), (273.0, 4.19), (323.8, 4.57), (355.6, 4.78),
        (406.4, 4.78), (457.0, 4.78), (508.0, 5.54), (610.0, 6.35), (609.6, 6.35)
    ]:
        return "Schedule 5S"
    # Schedule 10S
    if (od, wt) in [
        (10.3, 1.24), (13.7, 1.65), (17.1, 1.65), (21.3, 2.11), (26.7, 2.11), (33.4, 2.77),
        (42.2, 2.77), (48.3, 2.77), (60.3, 2.77), (73.0, 3.05), (88.9, 3.05), (114.3, 3.05),
        (141.3, 3.40), (168.3, 3.40), (219.1, 3.76), (273.0, 4.19), (323.8, 4.57), (355.6, 4.78),
        (406.4, 4.78), (457.0, 4.78), (508.0, 5.54), (610.0, 6.35), (609.6, 6.35)
    ]:
        return "Schedule 10S"
    # Schedule 40S
    if (od, wt) in [
        (10.3, 1.73), (13.7, 2.24), (17.1, 2.31), (21.3, 2.77), (26.7, 2.87), (33.4, 3.38),
        (42.2, 3.56), (48.3, 3.68), (60.3, 3.91), (73.0, 5.16), (88.9, 5.49), (101.6, 5.74),
        (114.3, 6.02), (141.3, 6.55), (168.3, 7.11), (219.1, 8.18), (273.0, 9.27), (323.8, 9.53),
        (355.6, 9.53), (406.4, 9.53), (457.0, 9.53), (508.0, 9.53), (610.0, 9.53), (609.6, 9.53)
    ]:
        return "Schedule 40S"
    # Schedule 80S
    if (od, wt) in [
        (10.3, 2.41), (13.7, 3.02), (17.1, 3.20), (21.3, 3.73), (26.7, 3.91), (33.4, 4.55),
        (42.2, 4.85), (48.3, 5.08), (60.3, 5.54), (73.0, 7.01), (88.9, 7.62), (101.6, 8.08),
        (114.3, 8.56), (141.3, 9.53), (168.3, 10.97), (219.1, 12.70), (273.0, 15.09), (273.1, 15.09),
        (323.8, 17.48), (355.6, 19.05), (406.4, 21.44), (406.4, 25.4), (457.0, 23.83), (508.0, 26.19),
        (559.0, 28.58), (610.0, 30.96), (609.6, 30.96)
    ]:
        return "Schedule 80S"
    # Schedule 160S
    if (od, wt) in [
        (21.3, 4.78), (26.7, 5.56), (33.4, 6.35), (42.2, 6.35), (48.3, 7.14), (60.3, 8.74),
        (73.0, 9.53), (88.9, 11.13), (114.3, 13.49), (141.3, 15.88), (168.3, 18.26), (219.1, 23.01),
        (273.0, 28.58), (323.8, 33.32), (355.6, 35.71), (406.4, 40.49), (457.0, 45.24), (508.0, 50.01),
        (559.0, 53.98), (610.0, 59.54), (609.6, 59.54)
    ]:
        return "Schedule 160S"
    # XXS (Double Extra Strong)
    if (od, wt) in [
        (10.3, 4.83), (13.7, 6.05), (17.1, 6.40), (21.3, 7.47), (26.7, 7.82), (33.4, 9.09),
        (42.2, 9.70), (48.3, 10.15), (60.3, 11.07), (73.0, 14.02), (88.9, 15.24), (114.3, 17.12),
        (141.3, 19.05), (168.3, 21.95), (219.1, 22.23), (273.0, 25.40), (323.8, 25.40)
    ]:
        return "SCH XXS"
    return "Non STD"

def categorize_is(od, wt):
    try:
        od = float(od)
        wt = float(wt)
    except:
        return "Non IS Standard"
    # Light (A-Class)
    if (od, wt) in [
        (10.32, 1.80), (13.49, 1.80), (17.10, 1.80), (21.3, 2.00), (21.43, 2.00), (27.20, 2.35),
        (33.70, 2.65), (33.80, 2.65), (42.90, 2.65), (48.40, 2.90), (48.30, 2.90), (60.30, 2.90),
        (76.20, 3.25), (88.90, 3.25), (114.30, 3.65)
    ]:
        return "IS 1239: Light (A-Class)"
    # Medium (B-Class)
    if (od, wt) in [
        (10.32, 2.00), (13.49, 2.35), (17.10, 2.35), (21.3, 2.65), (21.43, 2.65), (27.20, 2.65),
        (33.80, 3.25), (33.70, 3.25), (42.90, 3.25), (48.40, 3.25), (48.30, 3.25), (60.30, 3.65),
        (76.20, 3.65), (76.10, 3.60), (88.90, 4.05), (114.30, 4.50), (139.70, 4.85), (165.10, 4.85)
    ]:
        return "IS 1239: Medium (B-Class)"
    # Heavy (C-Class)
    if (od, wt) in [
        (10.32, 2.65), (13.49, 2.90), (17.10, 2.90), (21.43, 3.25), (27.20, 3.25), (33.80, 4.05),
        (33.70, 4), (21.3, 3.2), (42.90, 4.05), (48.40, 4.05), (48.30, 4.05), (60.30, 4.47),
        (76.20, 4.47), (76.10, 4.50), (88.90, 4.85), (114.30, 5.40), (139.70, 5.40), (165.10, 5.40)
    ]:
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
def add_categorizations(df):
    # Add OD_Category and WT_Schedule columns
    # Use Grade_Logic if available, otherwise fall back to Grade
    grade_col = 'Grade_Logic' if 'Grade_Logic' in df.columns else 'Grade'
    
    if 'OD' in df.columns and grade_col in df.columns:
        df['OD_Category'] = df.apply(lambda row: categorize_OD(row['OD'], row[grade_col]), axis=1)
    else:
        df['OD_Category'] = "Unknown"
    if 'OD' in df.columns and 'WT' in df.columns and grade_col in df.columns:
        df['WT_Schedule'] = df.apply(lambda row: categorize_WT_schedule(row['OD'], row['WT'], row[grade_col]), axis=1)
    else:
        df['WT_Schedule'] = "Unknown"
    return df

# Helper to load all relevant sheets
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
            
            # Standardize column names
            df.columns = [str(c).strip().replace(" ", "_").replace(".", "").replace("-", "_") for c in df.columns]
            
            # Standardize additional spec column names to "Add_Spec" for all sheets
            add_spec_columns = [c for c in df.columns if c.lower() in ["add_spec", "addlspec", "addlspec", "additional_spec", "add_spec", "additional_spec"]]
            # Also check for the standardized version (AddlSpec becomes AddlSpec after dot removal)
            if not add_spec_columns:
                add_spec_columns = [c for c in df.columns if "addlspec" in c.lower()]
            if add_spec_columns:
                # Rename the first found additional spec column to "Add_Spec"
                df = df.rename(columns={add_spec_columns[0]: "Add_Spec"})
            
            # Derive Grade from Specification if Grade column doesn't exist
            if 'Grade' not in df.columns and 'Specification' in df.columns:
                def derive_grade_from_spec(spec):
                    """Derive Grade Type from Specification using mapping or fallback logic"""
                    if pd.isna(spec):
                        return "Unknown"
                    
                    spec_str = str(spec).strip()
                    
                    # First try to get from mapping
                    if spec_str in SPECIFICATION_MAPPING:
                        grade_type = SPECIFICATION_MAPPING[spec_str]
                        return grade_type  # Return original grade type for display
                    
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
                        return "AS"  # AS type
                    elif spec_upper.startswith("CS"):
                        return "CS"  # CS type
                    elif spec_upper.startswith("SS"):
                        return "SS"       # SS type
                    elif spec_upper.startswith("IS"):
                        return "IS"       # IS type
                    elif spec_upper.startswith("T"):
                        return "Tubes"    # Tubes type
                    
                    # Default fallback
                    return "Unknown"
                
                # Add Grade column derived from Specification (for display)
                df['Grade'] = df['Specification'].apply(derive_grade_from_spec)
                
                # Add Grade_Logic column for internal categorization (CS & AS combined)
                def derive_grade_for_logic(spec):
                    """Derive Grade Type for internal logic (CS & AS combined)"""
                    if pd.isna(spec):
                        return "Unknown"
                    
                    spec_str = str(spec).strip()
                    
                    # First try to get from mapping
                    if spec_str in SPECIFICATION_MAPPING:
                        grade_type = SPECIFICATION_MAPPING[spec_str]
                        # Convert AS/CS to "CS & AS" for existing logic compatibility
                        if grade_type in ["AS", "CS"]:
                            return "CS & AS"
                        elif grade_type == "TUBES":
                            return "Tubes"  # Convert to match existing logic
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
                        return "CS & AS"  # AS type
                    elif spec_upper.startswith("CS"):
                        return "CS & AS"  # CS type
                    elif spec_upper.startswith("SS"):
                        return "SS"       # SS type
                    elif spec_upper.startswith("IS"):
                        return "IS"       # IS type
                    elif spec_upper.startswith("T"):
                        return "Tubes"    # Tubes type
                    
                    # Default fallback
                    return "Unknown"
                
                # Add Grade_Logic column for internal categorization
                df['Grade_Logic'] = df['Specification'].apply(derive_grade_for_logic)
            
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
    sheets = load_inventory_data(data_file)
    # Collect filter options from all sheets to ensure comprehensive coverage
    all_individual_specs = set()
    
    # Process each sheet for additional spec options
    for sheet_name, df in sheets.items():
        if not df.empty and "Add_Spec" in df.columns:
            all_add_spec_values = df["Add_Spec"].dropna().unique().astype(str).tolist()
            
            for value in all_add_spec_values:
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

        # Collect Make options from all sheets (Stock, Incoming, Reservations)
        all_makes = set()
        for sheet_name, sheet_df in sheets.items():
            if not sheet_df.empty:
                sheet_make_col = next((c for c in sheet_df.columns if c.lower() in ["make", "make_"]), None)
                if sheet_make_col:
                    make_values = sheet_df[sheet_make_col].dropna().unique().astype(str).tolist()
                    for make_val in make_values:
                        # Handle comma-separated makes (like "KIRLOSKAR, JSL, ISMT")
                        if ',' in make_val:
                            individual_makes = [m.strip() for m in make_val.split(',') if m.strip()]
                            all_makes.update(individual_makes)
                        else:
                            all_makes.add(make_val.strip())
        
        if all_makes:
            make_options = ["All"] + sorted(list(all_makes))
        if od_col:
            od_options = ["All"] + sorted(df[od_col].dropna().unique().astype(str).tolist(), key=lambda x: float(x) if x.replace('.','',1).isdigit() else x)
        if wt_col:
            wt_options = ["All"] + sorted(df[wt_col].dropna().unique().astype(str).tolist(), key=lambda x: float(x) if x.replace('.','',1).isdigit() else x)
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

# Function to derive Grade Type from Specification
def derive_grade_type_from_spec(specification):
    """Derive Grade Type from Specification name using mapping or fallback logic"""
    if pd.isna(specification):
        return "Unknown"
    
    spec_str = str(specification).strip()
    
    # First try to get from mapping
    if spec_str in SPECIFICATION_MAPPING:
        grade_type = SPECIFICATION_MAPPING[spec_str]
        # Convert AS/CS to "CS & AS" for existing logic compatibility
        if grade_type in ["AS", "CS"]:
            return "CS & AS"
        elif grade_type == "TUBES":
            return "Tubes"  # Convert to match existing logic
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
        return "CS & AS"  # AS type
    elif spec_upper.startswith("CS"):
        return "CS & AS"  # CS type
    elif spec_upper.startswith("SS"):
        return "SS"       # SS type
    elif spec_upper.startswith("IS"):
        return "IS"       # IS type
    elif spec_upper.startswith("T"):
        return "Tubes"    # Tubes type
    
    # Default fallback
    return "Unknown"

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
spec_filter = st.sidebar.multiselect("Specification (Product Name)", spec_options, default=["All"], 
                                    help="Select specifications to filter. Grade Type is automatically derived from specification names.")

# Get grade-specific filter options based on selected specifications
od_category_options_filtered, wt_category_options_filtered = get_grade_specific_options_from_specs(spec_filter)

st.sidebar.markdown("**Additional Filters:**")
make_filter = st.sidebar.multiselect("Make", make_options, default=["All"])
add_spec_filter = st.sidebar.multiselect("Additional Spec", add_spec_options, default=["All"])
od_category_filter = st.sidebar.multiselect("OD Category", od_category_options_filtered, default=["All"])
wt_category_filter = st.sidebar.multiselect("WT Category", wt_category_options_filtered, default=["All"])
od_filter = st.sidebar.multiselect("OD", od_options, default=["All"])
wt_filter = st.sidebar.multiselect("WT", wt_options, default=["All"])
branch_filter = st.sidebar.multiselect("Branch", branch_options, default=["All"])

# Metric is always MT since we don't have Sales Amount data
metric = "MT"

# --- Main Area ---
if data_file is not None:
    
    # Initialize session state for chart type if not exists
    if 'chart_type' not in st.session_state:
        st.session_state.chart_type = "Stock"
    
    # Create tab buttons using columns (moved to top, no extra spacing)
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        stock_active = st.button("📦 Stock", key="stock_tab", use_container_width=True)
        if stock_active:
            st.session_state.chart_type = "Stock"
    with col2:
        reserved_active = st.button("🔒 Reserved", key="reserved_tab", use_container_width=True)
        if reserved_active:
            st.session_state.chart_type = "Reserved"
    with col3:
        incoming_active = st.button("📥 Incoming", key="incoming_tab", use_container_width=True)
        if incoming_active:
            st.session_state.chart_type = "Incoming"
    with col4:
        free_sale_active = st.button("💰 Free For Sale", key="free_sale_tab", use_container_width=True)
        if free_sale_active:
            st.session_state.chart_type = "Free For Sale"
    
    # Use the session state to determine which tab is active
    size_chart_type = st.session_state.chart_type
    
    # Add a separator line below the tabs
    st.markdown("<hr style='margin: 5px 0 15px 0; border: 1px solid #666666;'>", unsafe_allow_html=True)
    
    # Handle different data sources
    if size_chart_type == "Reserved":
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
                    
                    # Pivot to get Stock, Reservations, Incoming columns
                    pivot_data = all_data.groupby(group_cols + ['Type'])['MT'].sum().reset_index()
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
                    heatmap_pivot = all_data.groupby(heatmap_group_cols + ['Type'])['MT'].sum().reset_index()
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
                        all_data_rounded = all_data.copy()
                        all_data_rounded['OD'] = all_data_rounded['OD'].round(2)
                        all_data_rounded['WT'] = all_data_rounded['WT'].round(2)
                        
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
                        
                        # Calculate Free For Sale for each unique product (handle missing columns gracefully)
                        preview_pivot['MT'] = (
                            preview_pivot.get('Stock', 0) - 
                            preview_pivot.get('Reservations', 0) + 
                            preview_pivot.get('Incoming', 0)
                        )
                        
                        # Add Grade Type column derived from Specification
                        def derive_grade_type_for_preview(spec):
                            """Derive Grade Type from Specification for preview table"""
                            if pd.isna(spec):
                                return "Unknown"
                            
                            spec_str = str(spec).strip()
                            
                            # First try to get from mapping
                            if spec_str in SPECIFICATION_MAPPING:
                                grade_type = SPECIFICATION_MAPPING[spec_str]
                                return grade_type  # Return original grade type for display
                            
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
                                return "AS"  # AS type
                            elif spec_upper.startswith("CS"):
                                return "CS"  # CS type
                            elif spec_upper.startswith("SS"):
                                return "SS"       # SS type
                            elif spec_upper.startswith("IS"):
                                return "IS"       # IS type
                            elif spec_upper.startswith("T"):
                                return "Tubes"    # Tubes type
                            
                            # Default fallback
                            return "Unknown"
                        
                        # Add Grade column (for display consistency with other chart types)
                        preview_pivot['Grade'] = preview_pivot['Specification'].apply(derive_grade_type_for_preview)
                        
                        # Add OD_Category and WT_Schedule columns for preview data
                        # We need to derive Grade for categorization (CS & AS combined for internal logic)
                        def derive_grade_for_categorization(spec):
                            """Derive Grade for categorization (CS & AS combined)"""
                            if pd.isna(spec):
                                return "Unknown"
                            
                            spec_str = str(spec).strip()
                            
                            # First try to get from mapping
                            if spec_str in SPECIFICATION_MAPPING:
                                grade_type = SPECIFICATION_MAPPING[spec_str]
                                # Convert AS/CS to "CS & AS" for categorization
                                if grade_type in ["AS", "CS"]:
                                    return "CS & AS"
                                elif grade_type == "TUBES":
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
                                return "CS & AS"  # AS type
                            elif spec_upper.startswith("CS"):
                                return "CS & AS"  # CS type
                            elif spec_upper.startswith("SS"):
                                return "SS"       # SS type
                            elif spec_upper.startswith("IS"):
                                return "IS"       # IS type
                            elif spec_upper.startswith("T"):
                                return "Tubes"    # Tubes type
                            
                            # Default fallback
                            return "Unknown"
                        
                        # Add categorizations using internal grade logic (but don't display Grade_Logic column)
                        preview_pivot['OD_Category'] = preview_pivot.apply(lambda row: categorize_OD(row['OD'], derive_grade_for_categorization(row['Specification'])), axis=1)
                        preview_pivot['WT_Schedule'] = preview_pivot.apply(lambda row: categorize_WT_schedule(row['OD'], row['WT'], derive_grade_for_categorization(row['Specification'])), axis=1)
                        
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
            if 'Make' in filtered.columns and make_filter:
                if "All" in make_filter:
                    # If "All" is selected, exclude any other specific values
                    exclude_values = [v for v in make_filter if v != "All"]
                    if exclude_values:
                        # Exclude records that contain any of the excluded makes
                        mask = filtered['Make'].astype(str).apply(
                            lambda x: not any(check_make_match(x, exclude_make) for exclude_make in exclude_values)
                        )
                        filtered = filtered[mask]
                else:
                    # If "All" is not selected, show records that contain any of the selected makes
                    mask = filtered['Make'].astype(str).apply(
                        lambda x: any(check_make_match(x, selected_make) for selected_make in make_filter)
                    )
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
                        # Exclude records that contain any of the excluded individual specs using word boundary matching
                        mask = filtered[add_spec_col_name].astype(str).apply(
                            lambda x: not any(check_word_boundary_match(x, exclude_spec) for exclude_spec in exclude_values)
                        )
                        filtered = filtered[mask]
                else:
                    # Contains matching logic: show records that contain the selected spec(s)
                    if len(add_spec_filter) == 1:
                        # Single selection: show all records that contain this spec as a complete word
                        selected_spec = add_spec_filter[0].strip()
                        mask = filtered[add_spec_col_name].astype(str).apply(
                            lambda x: check_word_boundary_match(x, selected_spec)
                        )
                    else:
                        # Multiple selections: show records that contain ALL selected specs (in any order)
                        def check_contains_all_specs(data_value, selected_specs):
                            """Check if data value contains ALL the selected specs (in any order)"""
                            data_value_str = str(data_value).strip()
                            # Check if all selected specs are present in the data value as complete words
                            return all(check_word_boundary_match(data_value_str, spec.strip()) for spec in selected_specs)
                        
                        mask = filtered[add_spec_col_name].astype(str).apply(
                            lambda x: check_contains_all_specs(x, add_spec_filter)
                        )
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
            return filtered

        df_filtered = apply_filters(df_cat)

        # --- Pivot Table (with Totals, Color Formatting) ---
        st.markdown(f"<h5 style='margin-bottom: 5px; color: #1a6b3e;'>{size_chart_type} Items Heatmap</h5>", unsafe_allow_html=True)
        metric_col = metric if metric in df_filtered.columns else None
        if metric_col is None:
            metric_col = next((c for c in df_filtered.columns if c.lower() == metric.lower()), None)
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
                    # Single red color for all negative values
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
        st.markdown(f"<h5 style='margin-bottom: 5px; color: #1a6b3e;'>Preview: {size_chart_type} Data</h5>", unsafe_allow_html=True)
        
        # Use preview data for Free For Sale, otherwise use filtered data
        if size_chart_type == "Free For Sale":
            # For Free For Sale, always use the aggregated preview data
            if 'df_preview' in locals() and df_preview is not None:
                df_filtered_display = df_preview.copy()
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
        else:
            st.write(f"Filtered rows: {len(df_filtered)}")
            df_filtered_display = df_filtered.reset_index(drop=True)
        
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
        
        st.dataframe(df_filtered_display)
        st.markdown("<hr style='margin: 20px 0 0 0; border: 1px solid #ddd;'>", unsafe_allow_html=True)
    else:
        st.warning(f"No data found in the '{size_chart_type}' sheet.")
else:
    if error_message:
        st.error(f"❌ {error_message}")
    else:
        st.info("📊 No inventory data available as no file was found.")    
