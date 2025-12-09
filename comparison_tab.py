"""
Comparison Tab Module for Inventory Dashboard

This module contains all the functionality for comparing inventory data between two Excel files.
It includes functions for listing available files, creating comparison data, and displaying results.
"""

import streamlit as st
import pandas as pd
import numpy as np
import io
import os
from functools import lru_cache
from datetime import timezone, timedelta

# Import required modules for S3 functionality
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

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


def derive_grade_from_spec(spec, combine_cs_as=False):
    """Derive grade from specification - simplified version"""
    if pd.isna(spec) or spec == '':
        return ''
    
    spec_str = str(spec).upper()
    
    # Simple grade extraction logic
    if 'CS' in spec_str:
        return 'CS'
    elif 'MS' in spec_str:
        return 'MS'
    elif 'SS' in spec_str:
        return 'SS'
    else:
        return ''

def categorize_OD(od, grade):
    """Categorize OD based on value and grade - simplified version"""
    if pd.isna(od):
        return 'Unknown'
    
    try:
        od_val = float(od)
        if od_val < 50:
            return 'Small'
        elif od_val < 100:
            return 'Medium'
        else:
            return 'Large'
    except:
        return 'Unknown'

def add_categorizations(df):
    """Add OD_Category and WT_Schedule columns - simplified version"""
    if df.empty:
        return df
    
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # Add Grade column if not present
    if 'Grade' not in df.columns and 'Specification' in df.columns:
        df['Grade'] = df['Specification'].apply(derive_grade_from_spec, combine_cs_as=False)
    
    # Add OD_Category
    if 'OD' in df.columns and 'Grade' in df.columns:
        df['OD_Category'] = df.apply(lambda row: categorize_OD(row['OD'], row['Grade']), axis=1)
    else:
        df['OD_Category'] = "Unknown"
    
    # Add WT_Schedule
    if 'WT' in df.columns:
        df['WT_Schedule'] = df['WT'].apply(lambda x: 'Thin' if x < 3 else 'Medium' if x < 6 else 'Thick')
    else:
        df['WT_Schedule'] = "Unknown"
    
    return df

def load_inventory_data(file):
    """Load inventory data from Excel file - simplified version for comparison"""
    try:
        xls = pd.ExcelFile(file)
    except Exception:
        # Invalid Excel file structure
        raise ValueError("The uploaded file does not match the required structure for comparison. Please select another file.")
    
    try:
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
                add_spec_columns = [c for c in df.columns if c.lower() in ["add_spec", "addlspec", "addl_spec", "additional_spec", "add_spec", "additional_spec"]]
                if not add_spec_columns:
                    add_spec_columns = [c for c in df.columns if "addlspec" in c.lower() or "addl_spec" in c.lower() or "add_spec" in c.lower()]
                
                if add_spec_columns:
                    # Rename the first found additional spec column to "Add_Spec"
                    df = df.rename(columns={add_spec_columns[0]: "Add_Spec"})
                
                # Optimized data cleaning using vectorized operations
                df = df.dropna(how='all')  # Remove completely empty rows
                df = df.fillna('')  # Fill NaN values with empty string
                
                sheets[sheet] = df
            else:
                sheets[sheet] = pd.DataFrame()
        
        return sheets
    except Exception:
        # File structure doesn't match expected format
        raise ValueError("The uploaded file does not match the required structure for comparison. Please select another file.")


def derive_grade_from_spec(spec, combine_cs_as=False):
    """Derive Grade Type from Specification - simplified version"""
    if pd.isna(spec):
        return "Unknown"
    
    spec_str = str(spec).strip()
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


def categorize_OD_CS_AS(od):
    """Categorize OD for CS/AS grades"""
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
    """Categorize OD for SS grades"""
    return categorize_OD_CS_AS(od)


def categorize_OD_IS(od):
    """Categorize OD for IS grades"""
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
    """Categorize OD for Tube grades"""
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
    """Categorize OD based on grade"""
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


def categorize_carbon(od, wt):
    """Categorize WT Schedule for Carbon/AS grades"""
    try:
        od = float(od)
        wt = float(wt)
    except:
        return "Non STD"
    
    # Simplified categorization for comparison - just return basic schedule
    if wt <= 3:
        return "SCH 10"
    elif wt <= 5:
        return "STD"
    elif wt <= 8:
        return "SCH 40"
    elif wt <= 12:
        return "XS"
    elif wt <= 20:
        return "SCH 80"
    else:
        return "SCH 160"


def categorize_stainless(od, wt):
    """Categorize WT Schedule for Stainless grades"""
    try:
        od = float(od)
        wt = float(wt)
    except:
        return "Non STD"
    
    # Simplified categorization for comparison
    if wt <= 3:
        return "Schedule 5S"
    elif wt <= 5:
        return "Schedule 10S"
    elif wt <= 8:
        return "Schedule 40S"
    elif wt <= 12:
        return "Schedule 80S"
    else:
        return "Schedule 160S"


def categorize_is(od, wt):
    """Categorize WT Schedule for IS grades"""
    try:
        od = float(od)
        wt = float(wt)
    except:
        return "Non IS Standard"
    
    # Simplified categorization for comparison
    if wt <= 2.5:
        return "IS 1239: Light (A-Class)"
    elif wt <= 4:
        return "IS 1239: Medium (B-Class)"
    else:
        return "IS 1239: Heavy (C-Class)"


def categorize_WT_Tube(od, wt):
    """Categorize WT Schedule for Tube grades"""
    try:
        od = float(od)
        wt = float(wt)
    except:
        return "Non-Standard Tube"
    
    # Simplified categorization for comparison
    if wt <= 1.5:
        return "Small Wall Tube"
    elif wt <= 3:
        return "Medium Wall Tube"
    else:
        return "Heavy Wall Tube"


def categorize_WT_schedule(od, wt, grade):
    """Categorize WT Schedule based on grade"""
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


def add_categorizations(df):
    """Add OD_Category and WT_Schedule columns - simplified version"""
    # Add Grade column if not present
    if 'Grade' not in df.columns and 'Specification' in df.columns:
        df['Grade'] = df['Specification'].apply(derive_grade_from_spec, combine_cs_as=False)
    
    # Add OD_Category
    if 'OD' in df.columns and 'Grade' in df.columns:
        df['OD_Category'] = df.apply(lambda row: categorize_OD(row['OD'], row['Grade']), axis=1)
    else:
        df['OD_Category'] = "Unknown"
    
    # Add WT_Schedule
    if 'OD' in df.columns and 'WT' in df.columns and 'Grade' in df.columns:
        df['WT_Schedule'] = df.apply(lambda row: categorize_WT_schedule(row['OD'], row['WT'], row['Grade']), axis=1)
    else:
        df['WT_Schedule'] = "Unknown"
    
    return df


def list_available_files_from_s3(prefix=None):
    """
    List all available Excel files from S3 bucket with readable labels.
    """
    # Use the imported S3 client from main dashboard
    s3_client = get_s3_client()
    if not s3_client:
        return [], "S3 client not available"

    try:
        # Resolve prefix: env/secrets -> provided parameter
        # S3_PREFIX should be set in Streamlit secrets or environment variables
        env_prefix = os.getenv("S3_PREFIX")
        effective_prefix = env_prefix if env_prefix is not None else prefix
        if effective_prefix is None:
            return [], "S3_PREFIX not configured. Please set S3_PREFIX."

        def list_with_prefix(pref: str):
            files = []
            continuation_token = None
            while True:
                kwargs = {
                    "Bucket": S3_BUCKET_NAME,
                    "Prefix": pref or "",
                }
                if continuation_token:
                    kwargs["ContinuationToken"] = continuation_token
                response = s3_client.list_objects_v2(**kwargs)
                for obj in response.get("Contents", []):
                    key_lower = obj["Key"].lower()
                    if key_lower.endswith(".xlsx") or key_lower.endswith(".xlsm"):
                        filename = obj['Key'].split('/')[-1]
                        # Convert UTC to IST (UTC+5:30) for display
                        ist_timezone = timezone(timedelta(hours=5, minutes=30))
                        last_modified_utc = obj['LastModified']
                        # Convert to IST if timezone-aware, otherwise assume UTC
                        if last_modified_utc.tzinfo is not None:
                            last_modified_ist = last_modified_utc.astimezone(ist_timezone)
                        else:
                            # If naive datetime, assume UTC and convert to IST
                            last_modified_utc = last_modified_utc.replace(tzinfo=timezone.utc)
                            last_modified_ist = last_modified_utc.astimezone(ist_timezone)
                        upload_date = last_modified_ist.strftime('%Y-%m-%d')
                        # Show only the date as label (for Compare Files tab dropdown)
                        label = upload_date
                        files.append({
                            "key": obj['Key'],
                            "label": label,
                            "uploaded_at": upload_date,
                            "filename": filename,
                            "last_modified": obj['LastModified']  # Keep UTC for sorting/comparison
                        })
                if response.get("IsTruncated"):
                    continuation_token = response.get("NextContinuationToken")
                else:
                    break
            return files

        # List files with the effective prefix (from S3_PREFIX configuration)
        xlsx_files = list_with_prefix(effective_prefix)

        if not xlsx_files:
            return [], "No Excel files found in S3 bucket"

        # Sort by upload date (newest first)
        xlsx_files.sort(key=lambda x: x['last_modified'], reverse=True)
        
        # Deduplicate by filename - keep only the latest version of each file
        # This handles S3 versioning where the same file may appear multiple times
        # Since files are sorted newest first, we keep the first occurrence of each filename
        seen_filenames = set()
        deduplicated_files = []
        for file_info in xlsx_files:
            filename = file_info['filename']
            if filename not in seen_filenames:
                seen_filenames.add(filename)
                deduplicated_files.append(file_info)
        
        # Use deduplicated list (already sorted by date, newest first)
        xlsx_files = deduplicated_files
        
        # Always show Date+Time for all files for consistency (in IST)
        ist_timezone = timezone(timedelta(hours=5, minutes=30))
        for file_info in xlsx_files:
            # Convert UTC to IST for both date and time display
            last_modified_utc = file_info['last_modified']
            if last_modified_utc.tzinfo is not None:
                last_modified_ist = last_modified_utc.astimezone(ist_timezone)
            else:
                # If naive datetime, assume UTC and convert to IST
                last_modified_utc = last_modified_utc.replace(tzinfo=timezone.utc)
                last_modified_ist = last_modified_utc.astimezone(ist_timezone)
            # Recalculate date and time from IST to ensure consistency
            date = last_modified_ist.strftime('%Y-%m-%d')
            time_str = last_modified_ist.strftime('%I:%M %p IST')  # 12-hour format with AM/PM and IST timezone
            file_info['label'] = f"{date} {time_str}"
            # Update uploaded_at to IST date for consistency
            file_info['uploaded_at'] = date

        return xlsx_files, None

    except Exception as e:
        return [], f"Failed to list files from S3: {e}"


def get_file_from_s3_by_key(file_key):
    """
    Read a specific file from S3 by its key (read-only access).
    """
    # Use the imported S3 client from main dashboard
    s3_client = get_s3_client()
    if not s3_client:
        return None, None, "S3 client not available"
    
    try:
        # Read the file object (read-only access)
        file_response = s3_client.get_object(
            Bucket=S3_BUCKET_NAME, 
            Key=file_key
        )
        
        file_data = io.BytesIO(file_response['Body'].read())
        file_data.name = file_key  # Set filename for pandas
        
        # Get upload date from response metadata
        upload_date = file_response.get('LastModified')
        
        return file_data, upload_date, None
        
    except Exception as e:
        return None, None, f"Failed to retrieve file from S3: {e}"


def calculate_free_for_sale(stock_df, reservations_df, incoming_df):
    """
    Calculate Free for Sale from Stock, Reservations, and Incoming sheets.
    Formula: Free for Sale = Stock - Reservations + Incoming
    Groups by Specification, OD, WT (matching product key logic).
    
    Args:
        stock_df: DataFrame from Stock sheet
        reservations_df: DataFrame from Reservations sheet
        incoming_df: DataFrame from Incoming sheet
    
    Returns:
        DataFrame with Free for Sale values, grouped by Specification, OD, WT
    """
    try:
        # Combine all data with type indicator
        combined_data = []
        
        if not stock_df.empty:
            stock_df_copy = stock_df.copy()
            stock_df_copy['Type'] = 'Stock'
            combined_data.append(stock_df_copy)
        
        if not reservations_df.empty:
            reservations_df_copy = reservations_df.copy()
            reservations_df_copy['Type'] = 'Reservations'
            combined_data.append(reservations_df_copy)
        
        if not incoming_df.empty:
            incoming_df_copy = incoming_df.copy()
            incoming_df_copy['Type'] = 'Incoming'
            combined_data.append(incoming_df_copy)
        
        if not combined_data:
            return pd.DataFrame()
        
        # Combine all data
        all_data = pd.concat(combined_data, ignore_index=True)
        
        # Ensure proper data types for grouping
        all_data_clean = all_data.copy()
        
        # Convert OD and WT to numeric, handling any non-numeric values
        if 'OD' in all_data_clean.columns:
            all_data_clean['OD'] = pd.to_numeric(all_data_clean['OD'], errors='coerce')
        if 'WT' in all_data_clean.columns:
            all_data_clean['WT'] = pd.to_numeric(all_data_clean['WT'], errors='coerce')
        
        # Round to 3 decimal places to match product key precision
        if 'OD' in all_data_clean.columns:
            all_data_clean['OD'] = all_data_clean['OD'].round(3)
        if 'WT' in all_data_clean.columns:
            all_data_clean['WT'] = all_data_clean['WT'].round(3)
        
        # Convert MT to numeric (treat blanks/invalid as 0)
        if 'MT' in all_data_clean.columns:
            all_data_clean['MT'] = pd.to_numeric(all_data_clean['MT'], errors='coerce').fillna(0)
        else:
            all_data_clean['MT'] = 0
        
        # Group by Specification, OD, WT (matching product key logic)
        group_cols = ['Specification', 'OD', 'WT']
        
        # Ensure Specification is string and handle NaN
        if 'Specification' in all_data_clean.columns:
            all_data_clean['Specification'] = all_data_clean['Specification'].astype(str)
            all_data_clean['Specification'] = all_data_clean['Specification'].replace('nan', '')
            all_data_clean['Specification'] = all_data_clean['Specification'].str.strip()
        
        # Pivot to get Stock, Reservations, Incoming columns
        pivot_data = all_data_clean.groupby(group_cols + ['Type'])['MT'].sum().reset_index()
        pivot_data = pivot_data.pivot_table(
            index=group_cols,
            columns='Type',
            values='MT',
            fill_value=0
        ).reset_index()
        
        # Calculate Free For Sale: Stock - Reservations + Incoming
        pivot_data['MT'] = (
            pivot_data.get('Stock', 0) -
            pivot_data.get('Reservations', 0) +
            pivot_data.get('Incoming', 0)
        )
        
        # Keep only the columns needed for comparison (matching other datasets structure)
        # Include Specification, OD, WT, MT, and preserve other columns if they exist
        result_cols = ['Specification', 'OD', 'WT', 'MT']
        
        # Add other columns that might be in the original data (for display in comparison)
        optional_cols = ['Make', 'Branch', 'Add_Spec']
        for col in optional_cols:
            if col in all_data_clean.columns:
                # For optional columns, take the first non-empty value per group
                if col not in result_cols:
                    result_cols.append(col)
                    # Get first non-empty value for each group
                    col_data = all_data_clean.groupby(group_cols)[col].first().reset_index()
                    pivot_data = pivot_data.merge(col_data, on=group_cols, how='left')
        
        # Select only the result columns that exist
        available_result_cols = [col for col in result_cols if col in pivot_data.columns]
        result_df = pivot_data[available_result_cols].copy()
        
        # Ensure MT is numeric and rounded
        if 'MT' in result_df.columns:
            result_df['MT'] = pd.to_numeric(result_df['MT'], errors='coerce').fillna(0.0).round(3)
        
        return result_df
        
    except Exception as e:
        st.error(f"Error calculating Free for Sale: {e}")
        return pd.DataFrame()


def create_comparison_data(file1_data, file2_data, file1_name, file2_name):
    """
    Create comparison data by aligning and comparing two inventory datasets.
    """
    try:
        # Validate required columns exist in both datasets
        required_cols = ['Specification', 'OD', 'WT']
        missing_cols_file1 = [col for col in required_cols if col not in file1_data.columns]
        missing_cols_file2 = [col for col in required_cols if col not in file2_data.columns]
        
        if missing_cols_file1 or missing_cols_file2:
            raise ValueError("The uploaded file does not match the required structure for comparison. Please select another file.")
        
        # Create unique identifiers for each product (Specification + OD + WT)
        def create_product_key(row):
            spec = str(row.get('Specification', '')).strip()
            od = str(row.get('OD', '')).strip()
            wt = str(row.get('WT', '')).strip()
            return f"{spec}|{od}|{wt}"
        
        # Add product keys to both datasets
        file1_data['product_key'] = file1_data.apply(create_product_key, axis=1)
        file2_data['product_key'] = file2_data.apply(create_product_key, axis=1)
        
        # Normalize MT values BEFORE comparison to handle float precision issues
        # Convert to numeric, handle errors, fill NaN with 0, and round to 3 decimals
        # This ensures consistent comparison especially for Reservations with formula-derived values
        if 'MT' in file1_data.columns:
            file1_data['mt_file1'] = pd.to_numeric(file1_data['MT'], errors='coerce').fillna(0.0).round(3)
        else:
            file1_data['mt_file1'] = 0.0
        
        if 'MT' in file2_data.columns:
            file2_data['mt_file2'] = pd.to_numeric(file2_data['MT'], errors='coerce').fillna(0.0).round(3)
        else:
            file2_data['mt_file2'] = 0.0
        
        # Create comparison DataFrame
        comparison_cols = ['Specification', 'OD', 'WT', 'Make', 'Branch', 'Add_Spec', 'OD_Category', 'WT_Schedule', 'Grade']
        
        # Get all unique product keys
        all_keys = set(file1_data['product_key'].unique()) | set(file2_data['product_key'].unique())
        
        comparison_data = []
        
        for key in all_keys:
            # Find data in both files
            file1_rows = file1_data[file1_data['product_key'] == key]
            file2_rows = file2_data[file2_data['product_key'] == key]
            
            has_file1 = not file1_rows.empty
            has_file2 = not file2_rows.empty

            # Aggregate MT values for identical products (sum all MT values for same product key)
            # MT values are already normalized (numeric, rounded to 3 decimals) from earlier step
            mt1 = file1_rows['mt_file1'].sum() if has_file1 else 0.0
            mt2 = file2_rows['mt_file2'].sum() if has_file2 else 0.0
            
            # Round aggregated sums to handle any floating-point precision issues from summation
            mt1 = round(float(mt1), 3)
            mt2 = round(float(mt2), 3)
            
            # Tolerance for comparing MT values (accounts for floating-point precision differences)
            # Values within 0.001 are considered equal
            TOLERANCE = 0.001
            
            # Calculate delta: New Sheet - Previous Sheet
            # Positive = Stock increased (Green)
            # Negative = Stock decreased (Red)
            # Zero = No change (Yellow) - only when both sheets have data
            # None = No data in either sheet (White)
            
            if not has_file1 and not has_file2:
                # No data in either sheet - skip this item (don't include in heatmap)
                continue
            elif not has_file1 and has_file2:
                # Added item - stock increased from 0 to positive value
                delta = mt2
                status = "Added"
                base_row = file2_rows.iloc[0]  # Use first row from file2 for base data
                is_zero_difference = False
            elif has_file1 and not has_file2:
                # Removed item - stock decreased from positive value to 0
                delta = -mt1
                status = "Removed"
                base_row = file1_rows.iloc[0]  # Use first row from file1 for base data
                is_zero_difference = False
            else:
                # Both sheets have data - determine status based on change direction
                delta = mt2 - mt1
                # Round delta to 3 decimals for consistency
                delta = round(delta, 3)
                
                # Use tolerance-based comparison to handle floating-point precision issues
                # This is critical for Reservations where formula-derived values may have tiny differences
                if abs(delta) <= TOLERANCE:
                    status = "Unchanged"
                elif delta > TOLERANCE:
                    status = "Increased"
                else:  # delta < -TOLERANCE
                    status = "Decreased"
                
                base_row = file1_rows.iloc[0]  # Use first row from file1 for base data
                
                # Mark actual zero differences (both sheets have same non-zero data)
                # Use tolerance-based comparison here too
                is_zero_difference = (abs(delta) <= TOLERANCE and abs(mt1) > TOLERANCE and abs(mt2) > TOLERANCE)
            
            # Create comparison row
            # Values are already rounded, so we can use them directly
            row_data = {
                'product_key': key,
                'status': status,
                'old_stock': mt1 if has_file1 else 0.0,
                'new_stock': mt2 if has_file2 else 0.0,
                'delta': delta,
                'file1_name': file1_name,
                'file2_name': file2_name,
                'is_zero_difference': is_zero_difference if 'is_zero_difference' in locals() else False
            }
            
            # Add comparison columns
            for col in comparison_cols:
                if col in base_row:
                    row_data[col] = base_row[col]
                else:
                    row_data[col] = ''
            
            comparison_data.append(row_data)
        
        comparison_df = pd.DataFrame(comparison_data)
        
        if not comparison_df.empty:
            # Ensure numeric columns are properly typed and rounded
            numeric_cols = ['old_stock', 'new_stock', 'delta']
            for col in numeric_cols:
                comparison_df[col] = pd.to_numeric(comparison_df[col], errors='coerce').fillna(0.0).round(3)

            # Normalize OD and WT for consistent filtering/grouping
            comparison_df['OD'] = pd.to_numeric(comparison_df['OD'], errors='coerce')
            comparison_df['WT'] = pd.to_numeric(comparison_df['WT'], errors='coerce')
            if 'OD' in comparison_df.columns:
                comparison_df['OD'] = comparison_df['OD'].round(3)
            if 'WT' in comparison_df.columns:
                comparison_df['WT'] = comparison_df['WT'].round(3)

            # Recalculate categorizations to avoid stale "Unknown" values
            comparison_df = comparison_df.drop(columns=['OD_Category', 'WT_Schedule'], errors='ignore')
            comparison_df = add_categorizations(comparison_df)
        
        return comparison_df
        
    except (KeyError, ValueError, AttributeError, IndexError) as e:
        # Handle validation errors gracefully
        return pd.DataFrame()  # Return empty DataFrame - error will be shown by caller
    except Exception as e:
        # Handle any other unexpected errors
        return pd.DataFrame()  # Return empty DataFrame - error will be shown by caller


def render_comparison_tab():
    """
    Render the complete comparison tab interface.
    This function contains all the UI logic for the comparison tab.
    """
    # File Comparison Feature
    
    # Get available files from S3
    available_files, files_error = list_available_files_from_s3()
    
    if files_error:
        st.error(f"❌ Error loading files: {files_error}")
    elif not available_files:
        st.warning("📁 No Excel files found in S3 bucket.")
    else:
        # Initialize session state for persisted selections
        if 'compare_file1_selection' not in st.session_state:
            st.session_state.compare_file1_selection = None
        if 'compare_file2_selection' not in st.session_state:
            st.session_state.compare_file2_selection = None
        if 'compare_dataset' not in st.session_state:
            st.session_state.compare_dataset = "Stock"  # Default to Stock
        
        # Check if stored selections are still available (handle case where files were deleted from S3)
        file1_options = [f["label"] for f in available_files]
        file2_options = [f["label"] for f in available_files]
        
        if st.session_state.compare_file1_selection and st.session_state.compare_file1_selection not in file1_options:
            # Stored file 1 is no longer available, clear it
            st.session_state.compare_file1_selection = None
            if 'comparison_data' in st.session_state:
                del st.session_state.comparison_data
            if 'comparison_file1_name' in st.session_state:
                del st.session_state.comparison_file1_name
            if 'comparison_dataset_name' in st.session_state:
                del st.session_state.comparison_dataset_name
        
        if st.session_state.compare_file2_selection and st.session_state.compare_file2_selection not in file2_options:
            # Stored file 2 is no longer available, clear it
            st.session_state.compare_file2_selection = None
            if 'comparison_data' in st.session_state:
                del st.session_state.comparison_data
            if 'comparison_file2_name' in st.session_state:
                del st.session_state.comparison_file2_name
            if 'comparison_dataset_name' in st.session_state:
                del st.session_state.comparison_dataset_name
        
        # Define available datasets
        available_datasets = ["Stock", "Reservations", "Incoming", "Free for Sale"]
        
        # Create three columns for file and dataset selection
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Use stored selection as default if available and still in options
            file1_default = None
            file1_default_index = 0
            if st.session_state.compare_file1_selection and st.session_state.compare_file1_selection in file1_options:
                file1_default = st.session_state.compare_file1_selection
                file1_default_index = file1_options.index(file1_default)
            file1_selection = st.selectbox(
                "Select Date 1:", 
                options=file1_options,
                index=file1_default_index,
                key="file1_comparison",
                help="Select the first (older) date to compare"
            )
        
        with col2:
            # Use stored selection as default if available and still in options
            file2_default = None
            file2_default_index = 0
            if st.session_state.compare_file2_selection and st.session_state.compare_file2_selection in file2_options:
                file2_default = st.session_state.compare_file2_selection
                file2_default_index = file2_options.index(file2_default)
            file2_selection = st.selectbox(
                "Select Date 2:", 
                options=file2_options,
                index=file2_default_index,
                key="file2_comparison",
                help="Select the second (newer) date to compare"
            )
        
        with col3:
            # Dataset selector
            dataset_selection = st.selectbox(
                "Select Dataset:",
                options=available_datasets,
                index=available_datasets.index(st.session_state.compare_dataset) if st.session_state.compare_dataset in available_datasets else 0,
                key="dataset_comparison",
                help="Select the dataset to compare (Stock, Reservations, Incoming, or Free for Sale)"
            )
        
        # Store dataset selection in session state
        st.session_state.compare_dataset = dataset_selection
        
        # Check if selections have changed
        selections_changed = (
            file1_selection != st.session_state.compare_file1_selection or
            file2_selection != st.session_state.compare_file2_selection or
            dataset_selection != st.session_state.get('comparison_dataset_name', None)
        )
        
        # Check if we have valid cached comparison data that matches current selections
        # We'll validate this after auto-sort, so we know the correct file order
        has_cached_data = False
        if ('comparison_data' in st.session_state and 
            not st.session_state.comparison_data.empty and
            st.session_state.get('comparison_file1_name') and
            st.session_state.get('comparison_file2_name') and
            st.session_state.get('comparison_dataset_name')):
            # We'll validate the cached data matches after we determine the auto-sorted file names
            # This will be checked later in the code after auto-sort
            has_cached_data = True
        
        # Find the selected files
        file1_data = None
        file2_data = None
        
        if file1_selection and file2_selection:
            # Find the file keys and timestamps for selected files
            file1_key = None
            file2_key = None
            file1_timestamp = None
            file2_timestamp = None
            
            for file_info in available_files:
                if file_info["label"] == file1_selection:
                    file1_key = file_info["key"]
                    file1_timestamp = file_info["last_modified"]
                if file_info["label"] == file2_selection:
                    file2_key = file_info["key"]
                    file2_timestamp = file_info["last_modified"]
            
            if file1_key and file2_key and file1_key != file2_key:
                # Auto-sort: Ensure File 1 is older than File 2
                files_were_swapped = False
                original_file1_selection = file1_selection
                original_file2_selection = file2_selection
                
                if file1_timestamp and file2_timestamp:
                    if file1_timestamp > file2_timestamp:
                        # File 1 is newer than File 2, swap them
                        file1_selection, file2_selection = file2_selection, file1_selection
                        file1_key, file2_key = file2_key, file1_key
                        file1_timestamp, file2_timestamp = file2_timestamp, file1_timestamp
                        files_were_swapped = True
                
                # Show info message if files were auto-sorted
                if files_were_swapped:
                    st.info("ℹ️ Files automatically sorted chronologically: File 1 (older) → File 2 (newer)")
                
                # Validate cached data matches the auto-sorted file names and dataset
                cached_data_valid = False
                if has_cached_data:
                    cached_file1 = st.session_state.get('comparison_file1_name')
                    cached_file2 = st.session_state.get('comparison_file2_name')
                    cached_dataset = st.session_state.get('comparison_dataset_name')
                    # Check if cached data matches the auto-sorted file names and current dataset
                    if (cached_file1 == file1_selection and 
                        cached_file2 == file2_selection and 
                        cached_dataset == dataset_selection):
                        cached_data_valid = True
                    else:
                        # Cached data doesn't match, need to reload
                        cached_data_valid = False
                
                # Check if we need to reload files (only if selections changed or cached data is invalid)
                need_to_reload = selections_changed or not cached_data_valid
                
                if need_to_reload:
                    # Clear old comparison data if selections changed
                    if selections_changed:
                        if 'comparison_data' in st.session_state:
                            del st.session_state.comparison_data
                        if 'comparison_file1_name' in st.session_state:
                            del st.session_state.comparison_file1_name
                        if 'comparison_file2_name' in st.session_state:
                            del st.session_state.comparison_file2_name
                        if 'comparison_dataset_name' in st.session_state:
                            del st.session_state.comparison_dataset_name
                    
                    # Read both files from S3 (read-only access)
                    with st.spinner("Processing..."):
                        file1_data, file1_date, file1_error = get_file_from_s3_by_key(file1_key)
                        file2_data, file2_date, file2_error = get_file_from_s3_by_key(file2_key)
                        
                        if file1_error:
                            st.error(f"❌ Error loading first file: {file1_error}")
                            # Clear stored selections on error
                            st.session_state.compare_file1_selection = None
                            st.session_state.compare_file2_selection = None
                            if 'comparison_dataset_name' in st.session_state:
                                del st.session_state.comparison_dataset_name
                        elif file2_error:
                            st.error(f"❌ Error loading second file: {file2_error}")
                            # Clear stored selections on error
                            st.session_state.compare_file1_selection = None
                            st.session_state.compare_file2_selection = None
                            if 'comparison_dataset_name' in st.session_state:
                                del st.session_state.comparison_dataset_name
                        else:
                            # Process both files
                            try:
                                # Load inventory data from both files
                                try:
                                    file1_sheets = load_inventory_data(file1_data)
                                except ValueError as e:
                                    st.error("❌ The uploaded file does not match the required structure for comparison. Please select another file.")
                                    # Clear stored selections on error
                                    st.session_state.compare_file1_selection = None
                                    st.session_state.compare_file2_selection = None
                                    if 'comparison_dataset_name' in st.session_state:
                                        del st.session_state.comparison_dataset_name
                                else:
                                    # File 1 loaded successfully, try loading file 2
                                    try:
                                        file2_sheets = load_inventory_data(file2_data)
                                    except ValueError as e:
                                        st.error("❌ The uploaded file does not match the required structure for comparison. Please select another file.")
                                        # Clear stored selections on error
                                        st.session_state.compare_file1_selection = None
                                        st.session_state.compare_file2_selection = None
                                        if 'comparison_dataset_name' in st.session_state:
                                            del st.session_state.comparison_dataset_name
                                    else:
                                        # Both files loaded successfully, proceed with processing
                                        
                                        # Get selected dataset from both files (dynamic based on user selection)
                                        dataset = st.session_state.get("compare_dataset", "Stock")
                                        
                                        # Handle Free for Sale dataset (calculated, not loaded)
                                        if dataset == "Free for Sale":
                                            # Calculate Free for Sale for both files
                                            file1_df = calculate_free_for_sale(
                                                file1_sheets.get("Stock", pd.DataFrame()),
                                                file1_sheets.get("Reservations", pd.DataFrame()),
                                                file1_sheets.get("Incoming", pd.DataFrame())
                                            )
                                            file2_df = calculate_free_for_sale(
                                                file2_sheets.get("Stock", pd.DataFrame()),
                                                file2_sheets.get("Reservations", pd.DataFrame()),
                                                file2_sheets.get("Incoming", pd.DataFrame())
                                            )
                                        else:
                                            # Load dataset directly from sheets (Stock, Reservations, or Incoming)
                                            file1_df = file1_sheets.get(dataset, pd.DataFrame())
                                            file2_df = file2_sheets.get(dataset, pd.DataFrame())
                                        
                                        # Check if dataset exists in both files
                                        if file1_df.empty and file2_df.empty:
                                            st.error(f"❌ Dataset '{dataset}' not found in one or both files. Try selecting another dataset.")
                                            # Clear stored selections on error
                                            st.session_state.compare_file1_selection = None
                                            st.session_state.compare_file2_selection = None
                                            if 'comparison_data' in st.session_state:
                                                del st.session_state.comparison_data
                                            if 'comparison_file1_name' in st.session_state:
                                                del st.session_state.comparison_file1_name
                                            if 'comparison_file2_name' in st.session_state:
                                                del st.session_state.comparison_file2_name
                                            if 'comparison_dataset_name' in st.session_state:
                                                del st.session_state.comparison_dataset_name
                                        elif file1_df.empty or file2_df.empty:
                                            st.error(f"❌ Dataset '{dataset}' not found in one or both files. Try selecting another dataset.")
                                            # Clear stored selections on error
                                            st.session_state.compare_file1_selection = None
                                            st.session_state.compare_file2_selection = None
                                            if 'comparison_data' in st.session_state:
                                                del st.session_state.comparison_data
                                            if 'comparison_file1_name' in st.session_state:
                                                del st.session_state.comparison_file1_name
                                            if 'comparison_file2_name' in st.session_state:
                                                del st.session_state.comparison_file2_name
                                            if 'comparison_dataset_name' in st.session_state:
                                                del st.session_state.comparison_dataset_name
                                        else:
                                            # Add categorizations to both datasets
                                            try:
                                                file1_filtered = add_categorizations(file1_df.copy())
                                                file2_filtered = add_categorizations(file2_df.copy())
                                            except Exception:
                                                st.error("❌ The uploaded file does not match the required structure for comparison. Please select another file.")
                                                # Clear stored selections on error
                                                st.session_state.compare_file1_selection = None
                                                st.session_state.compare_file2_selection = None
                                                if 'comparison_dataset_name' in st.session_state:
                                                    del st.session_state.comparison_dataset_name
                                            else:
                                                # Create comparison data
                                                comparison_data = create_comparison_data(file1_filtered, file2_filtered, file1_selection, file2_selection)
                                                
                                                if comparison_data.empty:
                                                    st.error("❌ The uploaded file does not match the required structure for comparison. Please select another file.")
                                                    # Clear stored selections on error
                                                    st.session_state.compare_file1_selection = None
                                                    st.session_state.compare_file2_selection = None
                                                    if 'comparison_dataset_name' in st.session_state:
                                                        del st.session_state.comparison_dataset_name
                                                else:
                                                    # Store comparison data in session state for main dashboard to use
                                                    st.session_state.comparison_data = comparison_data
                                                    st.session_state.comparison_file1_name = file1_selection
                                                    st.session_state.comparison_file2_name = file2_selection
                                                    st.session_state.comparison_dataset_name = dataset
                                                    
                                                    # Store the original (pre-auto-sort) selections for dropdown persistence
                                                    st.session_state.compare_file1_selection = original_file1_selection
                                                    st.session_state.compare_file2_selection = original_file2_selection
                                                    
                                                    # Show success message and let main dashboard handle the display
                                                    st.success(f"✅ Files loaded successfully! {dataset} Comparison data is ready.")
                                    
                            except (KeyError, ValueError, AttributeError, IndexError) as e:
                                # Handle validation/structure errors gracefully
                                st.error("❌ The uploaded file does not match the required structure for comparison. Please select another file.")
                                # Clear stored selections on error
                                st.session_state.compare_file1_selection = None
                                st.session_state.compare_file2_selection = None
                                if 'comparison_dataset_name' in st.session_state:
                                    del st.session_state.comparison_dataset_name
                            except Exception as e:
                                # Handle any other unexpected errors
                                st.error("❌ The uploaded file does not match the required structure for comparison. Please select another file.")
                                # Clear stored selections on error
                                st.session_state.compare_file1_selection = None
                                st.session_state.compare_file2_selection = None
                                if 'comparison_dataset_name' in st.session_state:
                                    del st.session_state.comparison_dataset_name
                else:
                    # Use cached data - selections haven't changed
                    # Data is already in session_state, no need to reload
                    pass
            elif file1_key == file2_key:
                st.warning("⚠️ Please select two different files for comparison.")
                # Clear stored selections if same file selected
                if selections_changed:
                    st.session_state.compare_file1_selection = None
                    st.session_state.compare_file2_selection = None
                    if 'comparison_dataset_name' in st.session_state:
                        del st.session_state.comparison_dataset_name
            else:
                st.error("❌ Could not find selected files.")
                # Clear stored selections if files not found (e.g., deleted from S3)
                st.session_state.compare_file1_selection = None
                st.session_state.compare_file2_selection = None
                # Clear comparison data if files are missing
                if 'comparison_data' in st.session_state:
                    del st.session_state.comparison_data
                if 'comparison_file1_name' in st.session_state:
                    del st.session_state.comparison_file1_name
                if 'comparison_file2_name' in st.session_state:
                    del st.session_state.comparison_file2_name
                if 'comparison_dataset_name' in st.session_state:
                    del st.session_state.comparison_dataset_name


def get_comparison_data_for_dashboard():
    """
    Get comparison data formatted for the main dashboard display.
    Returns the comparison data with proper column formatting.
    """
    try:
        if 'comparison_data' in st.session_state:
            comparison_data = st.session_state.comparison_data.copy()
            
            # Validate required columns exist
            required_cols = ['old_stock', 'new_stock', 'delta', 'status']
            missing_cols = [col for col in required_cols if col not in comparison_data.columns]
            if missing_cols:
                return pd.DataFrame()  # Return empty DataFrame if structure invalid
            
            # Store the zero difference information in session state for color coding
            if 'is_zero_difference' in comparison_data.columns:
                zero_diff_data = comparison_data[comparison_data['is_zero_difference'] == True]
                st.session_state.comparison_zero_differences = zero_diff_data
            
            # Get file names for column renaming
            file1_name = st.session_state.get('comparison_file1_name', 'File 1')
            file2_name = st.session_state.get('comparison_file2_name', 'File 2')
            
            # Extract dates from file names
            # New format: label is just date (YYYY-MM-DD) or date + time (YYYY-MM-DD HH:MM)
            # Old format (for backward compatibility): "filename (YYYY-MM-DD)"
            import re
            # Try new format first (just date or date + time)
            file1_date_match = re.search(r'(\d{4}-\d{2}-\d{2})', file1_name)
            file2_date_match = re.search(r'(\d{4}-\d{2}-\d{2})', file2_name)
            
            # If not found, try old format (date in parentheses)
            if not file1_date_match:
                file1_date_match = re.search(r'\((\d{4}-\d{2}-\d{2})\)', file1_name)
            if not file2_date_match:
                file2_date_match = re.search(r'\((\d{4}-\d{2}-\d{2})\)', file2_name)
            
            file1_date_str = file1_date_match.group(1) if file1_date_match else "Unknown"
            file2_date_str = file2_date_match.group(1) if file2_date_match else "Unknown"
            
            # Format stock values to 3 decimal places (keep numeric)
            comparison_data['old_stock'] = pd.to_numeric(comparison_data['old_stock'], errors='coerce').fillna(0.0).round(3)
            comparison_data['new_stock'] = pd.to_numeric(comparison_data['new_stock'], errors='coerce').fillna(0.0).round(3)
            comparison_data['delta'] = pd.to_numeric(comparison_data['delta'], errors='coerce').fillna(0.0).round(3)
            
            # Apply the same column formatting as in the comparison tab
            # Rename columns for better display
            file1_display = file1_date_str if file1_date_str != "Unknown" else "File 1"
            file2_display = file2_date_str if file2_date_str != "Unknown" else "File 2"
            comparison_data = comparison_data.rename(columns={
                'old_stock': f'MT ({file1_display})',
                'new_stock': f'MT ({file2_display})',
                'delta': 'Change in Stock',  # This creates the Change in Stock column from delta
                'status': 'Status'
            })
            
            # Remove internal columns and file name columns
            columns_to_remove = ['product_key', 'is_zero_difference', 'file1_name', 'file2_name']
            existing_columns_to_remove = [col for col in columns_to_remove if col in comparison_data.columns]
            if existing_columns_to_remove:
                comparison_data = comparison_data.drop(columns=existing_columns_to_remove)
            
            # Define preferred ordering for key columns (keep others for filtering)
            desired_columns = [
                'Specification', 'Grade', 'OD', 'WT', 'OD_Category', 'WT_Schedule',
                f'MT ({file1_display})', f'MT ({file2_display})', 'Change in Stock', 'Status',
                'Add_Spec', 'Make', 'Branch'
            ]
            ordered_columns = [col for col in desired_columns if col in comparison_data.columns]
            remaining_columns = [col for col in comparison_data.columns if col not in ordered_columns]
            comparison_data = comparison_data[ordered_columns + remaining_columns]
            
            # Include ALL items in heatmap (Added, Removed, Changed, Unchanged)
            return comparison_data
        else:
            return pd.DataFrame()
    except (KeyError, ValueError, AttributeError, IndexError):
        # Handle validation errors gracefully - return empty DataFrame
        return pd.DataFrame()
    except Exception:
        # Handle any other unexpected errors - return empty DataFrame
        return pd.DataFrame()