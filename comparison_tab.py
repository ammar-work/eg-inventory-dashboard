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
        # Resolve prefix: env override -> provided -> root
        env_prefix = os.getenv("S3_PREFIX")
        effective_prefix = env_prefix if env_prefix is not None else prefix
        if effective_prefix is None:
            effective_prefix = ""

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
                        upload_date = obj['LastModified'].strftime('%Y-%m-%d')
                        label = f"{filename} ({upload_date})"
                        files.append({
                            "key": obj['Key'],
                            "label": label,
                            "uploaded_at": upload_date,
                            "filename": filename,
                            "last_modified": obj['LastModified']
                        })
                if response.get("IsTruncated"):
                    continuation_token = response.get("NextContinuationToken")
                else:
                    break
            return files

        # First attempt with effective prefix
        xlsx_files = list_with_prefix(effective_prefix)
        # Fallback to root if none found
        if not xlsx_files and effective_prefix:
            xlsx_files = list_with_prefix("")

        if not xlsx_files:
            return [], "No Excel files found in S3 bucket"

        # Sort by upload date (newest first)
        xlsx_files.sort(key=lambda x: x['last_modified'], reverse=True)

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


def create_comparison_data(file1_data, file2_data, file1_name, file2_name):
    """
    Create comparison data by aligning and comparing two inventory datasets.
    """
    try:
        # Create unique identifiers for each product (Specification + OD + WT + Make + Branch + Add_Spec)
        def create_product_key(row):
            spec = str(row.get('Specification', '')).strip()
            od = str(row.get('OD', '')).strip()
            wt = str(row.get('WT', '')).strip()
            make = str(row.get('Make', '')).strip()
            branch = str(row.get('Branch', '')).strip()
            add_spec = str(row.get('Add_Spec', '')).strip()
            return f"{spec}|{od}|{wt}|{make}|{branch}|{add_spec}"
        
        # Add product keys to both datasets
        file1_data['product_key'] = file1_data.apply(create_product_key, axis=1)
        file2_data['product_key'] = file2_data.apply(create_product_key, axis=1)
        
        # Get MT values for comparison
        file1_data['mt_file1'] = file1_data.get('MT', 0)
        file2_data['mt_file2'] = file2_data.get('MT', 0)
        
        # Create comparison DataFrame
        comparison_cols = ['Specification', 'OD', 'WT', 'Make', 'Branch', 'Add_Spec', 'OD_Category', 'WT_Schedule', 'Grade']
        
        # Get all unique product keys
        all_keys = set(file1_data['product_key'].unique()) | set(file2_data['product_key'].unique())
        
        comparison_data = []
        
        for key in all_keys:
            # Find data in both files
            file1_rows = file1_data[file1_data['product_key'] == key]
            file2_rows = file2_data[file2_data['product_key'] == key]
            
            # Aggregate MT values for identical products (sum all MT values for same product key)
            mt1 = file1_rows['mt_file1'].sum() if not file1_rows.empty else None
            mt2 = file2_rows['mt_file2'].sum() if not file2_rows.empty else None
            
            # Calculate delta: New Sheet - Previous Sheet
            # Positive = Stock increased (Green)
            # Negative = Stock decreased (Red)
            # Zero = No change (Yellow) - only when both sheets have data
            # None = No data in either sheet (White)
            
            if mt1 is None and mt2 is None:
                # No data in either sheet - skip this item (don't include in heatmap)
                continue
            elif mt1 is None and mt2 is not None:
                # Added item - stock increased from 0 to positive value
                delta = mt2
                status = "Added"
                base_row = file2_rows.iloc[0]  # Use first row from file2 for base data
            elif mt1 is not None and mt2 is None:
                # Removed item - stock decreased from positive value to 0
                delta = -mt1
                status = "Removed"
                base_row = file1_rows.iloc[0]  # Use first row from file1 for base data
            else:
                # Both sheets have data - determine status based on change direction
                delta = mt2 - mt1
                if delta > 0:
                    status = "Increased"
                elif delta < 0:
                    status = "Decreased"
                else:
                    status = "Unchanged"
                base_row = file1_rows.iloc[0]  # Use first row from file1 for base data
                
                # Mark actual zero differences (both sheets have same non-zero data)
                is_zero_difference = (delta == 0 and mt1 != 0 and mt2 != 0)
            
            # Create comparison row
            row_data = {
                'product_key': key,
                'status': status,
                'old_stock': mt1,
                'new_stock': mt2,
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
        
        return pd.DataFrame(comparison_data)
        
    except Exception as e:
        st.error(f"Error creating comparison data: {e}")
        return pd.DataFrame()


def display_comparison_results(comparison_data, file1_name, file2_name):
    """
    Display comparison results with heatmap and detailed table.
    """
    try:
        # Summary statistics
        total_products = len(comparison_data)
        added_products = len(comparison_data[comparison_data['status'] == 'Added'])
        removed_products = len(comparison_data[comparison_data['status'] == 'Removed'])
        increased_products = len(comparison_data[comparison_data['status'] == 'Increased'])
        decreased_products = len(comparison_data[comparison_data['status'] == 'Decreased'])
        unchanged_products = len(comparison_data[comparison_data['status'] == 'Unchanged'])
        
        # Display summary
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Total Products", total_products)
        with col2:
            st.metric("Added", added_products, delta=f"+{added_products}")
        with col3:
            st.metric("Removed", removed_products, delta=f"-{removed_products}")
        with col4:
            st.metric("Increased", increased_products, delta=f"+{increased_products}")
        with col5:
            st.metric("Decreased", decreased_products, delta=f"-{decreased_products}")
        
        # Add a second row for Unchanged
        col6, col7, col8, col9, col10 = st.columns(5)
        with col6:
            st.metric("Unchanged", unchanged_products)
        
        # Create heatmap for delta changes
        if not comparison_data.empty and 'OD_Category' in comparison_data.columns and 'WT_Schedule' in comparison_data.columns:
            st.markdown("### 📊 Stock Change Heatmap")
            
            # Filter out added/removed products for heatmap (only show changed/unchanged)
            heatmap_data = comparison_data[comparison_data['status'].isin(['Changed', 'Unchanged'])]
            
            if not heatmap_data.empty:
                # Create pivot table for heatmap
                pivot_data = heatmap_data.groupby(['OD_Category', 'WT_Schedule'])['delta'].sum().reset_index()
                
                # Create pivot table
                pivot = pivot_data.pivot(index='OD_Category', columns='WT_Schedule', values='delta').fillna(0)
                
                # Add totals
                pivot['Total'] = pivot.sum(axis=1)
                col_total = pivot.sum(axis=0)
                col_total.name = 'Total'
                pivot = pd.concat([pivot, col_total.to_frame().T])
                
                # Format values
                pivot = pivot.round(2)
                
                # Color coding for heatmap
                def highlight_delta(val):
                    if pd.isna(val) or val == 0:
                        return "background-color: #FFFFFF; color: #CCCCCC;"
                    elif val > 0:
                        # Green for positive changes
                        intensity = min(abs(val) / pivot[pivot != 0].abs().max().max(), 1) if not pivot[pivot != 0].empty else 0
                        green_intensity = int(255 * (0.3 + 0.7 * intensity))
                        return f"background-color: #00{green_intensity:02x}00; color: #FFFFFF; font-weight: bold;"
                    else:
                        # Red for negative changes
                        intensity = min(abs(val) / pivot[pivot != 0].abs().max().max(), 1) if not pivot[pivot != 0].empty else 0
                        red_intensity = int(255 * (0.3 + 0.7 * intensity))
                        return f"background-color: #{red_intensity:02x}0000; color: #FFFFFF; font-weight: bold;"
                
                # Apply styling
                styled_pivot = pivot.style.applymap(highlight_delta)
                st.dataframe(styled_pivot, use_container_width=True)
            else:
                st.info("No data available for heatmap (only added/removed products).")
        
        # Add a button to clear comparison data
        if st.button("🗑️ Clear Comparison Data", help="Clear all comparison data and start fresh"):
            if 'comparison_data' in st.session_state:
                del st.session_state.comparison_data
            if 'comparison_file1_name' in st.session_state:
                del st.session_state.comparison_file1_name
            if 'comparison_file2_name' in st.session_state:
                del st.session_state.comparison_file2_name
            if 'comparison_zero_differences' in st.session_state:
                del st.session_state.comparison_zero_differences
            st.rerun()
        
        # Detailed comparison table
        st.markdown("### 📋 Detailed Comparison Table")
        
        # Filter and display data based on status
        status_filter = st.selectbox(
            "Filter by Status:",
            ["All", "Added", "Removed", "Increased", "Decreased", "Unchanged"],
            key="comparison_status_filter"
        )
        
        if status_filter != "All":
            filtered_data = comparison_data[comparison_data['status'] == status_filter]
        else:
            filtered_data = comparison_data
        
        if not filtered_data.empty:
            # Prepare display data
            display_data = filtered_data.copy()
            
            # Debug: Show available columns (remove this after testing)
            # st.write("Debug - Available columns:", list(display_data.columns))
            # st.write("Debug - Sample data:", display_data.head(2))
            
            # Get file names for column renaming
            file1_name = st.session_state.get('comparison_file1_name', 'File 1')
            file2_name = st.session_state.get('comparison_file2_name', 'File 2')
            
            # Extract dates from file names (assuming format: "filename (YYYY-MM-DD)")
            import re
            file1_date = re.search(r'\((\d{4}-\d{2}-\d{2})\)', file1_name)
            file2_date = re.search(r'\((\d{4}-\d{2}-\d{2})\)', file2_name)
            
            file1_date_str = file1_date.group(1) if file1_date else "Unknown"
            file2_date_str = file2_date.group(1) if file2_date else "Unknown"
            
            # Format stock values to 3 decimal places
            display_data['old_stock'] = display_data['old_stock'].apply(lambda x: round(float(x), 3) if pd.notna(x) and x is not None else x)
            display_data['new_stock'] = display_data['new_stock'].apply(lambda x: round(float(x), 3) if pd.notna(x) and x is not None else x)
            
            # Ensure proper display formatting
            display_data['old_stock'] = display_data['old_stock'].apply(lambda x: f"{x:.3f}" if pd.notna(x) and x is not None else x)
            display_data['new_stock'] = display_data['new_stock'].apply(lambda x: f"{x:.3f}" if pd.notna(x) and x is not None else x)
            
            # Rename columns for better display
            display_data = display_data.rename(columns={
                'old_stock': f'({file1_date_str}) sheet data',
                'new_stock': f'({file2_date_str}) sheet data',
                'delta': 'Change in Stock',
                'status': 'Status'
            })
            
            # Remove internal columns and file name columns
            columns_to_remove = ['product_key', 'is_zero_difference', 'file1_name', 'file2_name']
            existing_columns_to_remove = [col for col in columns_to_remove if col in display_data.columns]
            if existing_columns_to_remove:
                display_data = display_data.drop(columns=existing_columns_to_remove)
            
            # Define the desired column order - use Add_Spec (with underscore) like in Stock chart type
            desired_columns = [
                'Specification', 'Grade', 'OD', 'WT', 'Add_Spec', 'OD_Category', 'WT_Schedule',
                f'({file1_date_str}) sheet data', f'({file2_date_str}) sheet data', 'Change in Stock', 'Status',
                'Make', 'Branch'
            ]
            
            # Reorder columns - only include columns that exist in the dataframe
            available_columns = [col for col in desired_columns if col in display_data.columns]
            display_data = display_data[available_columns]
            
            # Debug: Show final columns (remove this after testing)
            # st.write("Debug - Final columns:", list(display_data.columns))
            
            # Add row numbers
            display_data.index = range(1, len(display_data) + 1)
            display_data.index.name = 'Row #'
            
            # Color code rows based on status
            def color_rows_by_status(row):
                status = row['Status']
                if status == 'Added':
                    return ['background-color: #E8F5E8; color: #000000;'] * len(row)  # Light green
                elif status == 'Removed':
                    return ['background-color: #FFEBEE; color: #000000;'] * len(row)  # Light red
                elif status == 'Increased':
                    return ['background-color: #E8F5E8; color: #000000;'] * len(row)  # Light green (same as Added)
                elif status == 'Decreased':
                    return ['background-color: #FFF3E0; color: #000000;'] * len(row)  # Light orange
                else:  # Unchanged
                    return [''] * len(row)
            
            # Apply styling
            styled_data = display_data.style.apply(color_rows_by_status, axis=1)
            st.dataframe(styled_data, use_container_width=True)
            
            st.write(f"Showing {len(filtered_data)} products (filtered by: {status_filter})")
        else:
            st.info(f"No products found with status: {status_filter}")
            
    except Exception as e:
        st.error(f"Error displaying comparison results: {e}")


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
        # Create two columns for file selection
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Select First File:**")
            file1_options = [f["label"] for f in available_files]
            file1_selection = st.selectbox(
                "File 1", 
                options=file1_options,
                key="file1_comparison",
                help="Select the first file to compare"
            )
        
        with col2:
            st.markdown("**Select Second File:**")
            file2_options = [f["label"] for f in available_files]
            file2_selection = st.selectbox(
                "File 2", 
                options=file2_options,
                key="file2_comparison",
                help="Select the second file to compare"
            )
        
        # Find the selected files
        file1_data = None
        file2_data = None
        
        if file1_selection and file2_selection:
            # Find the file keys for selected files
            file1_key = None
            file2_key = None
            
            for file_info in available_files:
                if file_info["label"] == file1_selection:
                    file1_key = file_info["key"]
                if file_info["label"] == file2_selection:
                    file2_key = file_info["key"]
            
            if file1_key and file2_key and file1_key != file2_key:
                # Read both files from S3 (read-only access)
                with st.spinner("Loading files for comparison..."):
                    file1_data, file1_date, file1_error = get_file_from_s3_by_key(file1_key)
                    file2_data, file2_date, file2_error = get_file_from_s3_by_key(file2_key)
                    
                    if file1_error:
                        st.error(f"❌ Error loading first file: {file1_error}")
                    elif file2_error:
                        st.error(f"❌ Error loading second file: {file2_error}")
                    else:
                        # Process both files
                        try:
                            # Load inventory data from both files
                            file1_sheets = load_inventory_data(file1_data)
                            file2_sheets = load_inventory_data(file2_data)
                            
                            # Get Stock data from both files
                            file1_stock = file1_sheets.get("Stock", pd.DataFrame())
                            file2_stock = file2_sheets.get("Stock", pd.DataFrame())
                            
                            if not file1_stock.empty and not file2_stock.empty:
                                # Add categorizations to both datasets
                                file1_filtered = add_categorizations(file1_stock.copy())
                                file2_filtered = add_categorizations(file2_stock.copy())
                                
                                # Create comparison data
                                comparison_data = create_comparison_data(file1_filtered, file2_filtered, file1_selection, file2_selection)
                                
                                if not comparison_data.empty:
                                    # Store comparison data in session state for main dashboard to use
                                    st.session_state.comparison_data = comparison_data
                                    st.session_state.comparison_file1_name = file1_selection
                                    st.session_state.comparison_file2_name = file2_selection
                                    
                                    # Show success message and let main dashboard handle the display
                                    st.success("✅ Files loaded successfully! Comparison data is ready.")
                                else:
                                    st.warning("⚠️ No matching data found between the two files for comparison.")
                            else:
                                st.error("❌ One or both files don't contain valid Stock data for comparison.")
                                
                        except Exception as e:
                            st.error(f"❌ Error processing files for comparison: {e}")
            elif file1_key == file2_key:
                st.warning("⚠️ Please select two different files for comparison.")
            else:
                st.error("❌ Could not find selected files.")


def get_comparison_data_for_dashboard():
    """
    Get comparison data formatted for the main dashboard display.
    Returns the comparison data with proper column formatting.
    """
    if 'comparison_data' in st.session_state:
        comparison_data = st.session_state.comparison_data.copy()
        
        # Store the zero difference information in session state for color coding
        zero_diff_data = comparison_data[comparison_data['is_zero_difference'] == True]
        st.session_state.comparison_zero_differences = zero_diff_data
        
        # Get file names for column renaming
        file1_name = st.session_state.get('comparison_file1_name', 'File 1')
        file2_name = st.session_state.get('comparison_file2_name', 'File 2')
        
        # Extract dates from file names (assuming format: "filename (YYYY-MM-DD)")
        import re
        file1_date = re.search(r'\((\d{4}-\d{2}-\d{2})\)', file1_name)
        file2_date = re.search(r'\((\d{4}-\d{2}-\d{2})\)', file2_name)
        
        file1_date_str = file1_date.group(1) if file1_date else "Unknown"
        file2_date_str = file2_date.group(1) if file2_date else "Unknown"
        
        # Format stock values to 3 decimal places
        comparison_data['old_stock'] = comparison_data['old_stock'].apply(lambda x: round(float(x), 3) if pd.notna(x) and x is not None else x)
        comparison_data['new_stock'] = comparison_data['new_stock'].apply(lambda x: round(float(x), 3) if pd.notna(x) and x is not None else x)
        
        # Ensure proper display formatting
        comparison_data['old_stock'] = comparison_data['old_stock'].apply(lambda x: f"{x:.3f}" if pd.notna(x) and x is not None else x)
        comparison_data['new_stock'] = comparison_data['new_stock'].apply(lambda x: f"{x:.3f}" if pd.notna(x) and x is not None else x)
        
        # Apply the same column formatting as in the comparison tab
        # Rename columns for better display
        comparison_data = comparison_data.rename(columns={
            'old_stock': f'({file1_date_str}) sheet data',
            'new_stock': f'({file2_date_str}) sheet data',
            'delta': 'Change in Stock',  # This creates the Change in Stock column from delta
            'status': 'Status'
        })
        
        # Remove internal columns and file name columns
        columns_to_remove = ['product_key', 'is_zero_difference', 'file1_name', 'file2_name']
        existing_columns_to_remove = [col for col in columns_to_remove if col in comparison_data.columns]
        if existing_columns_to_remove:
            comparison_data = comparison_data.drop(columns=existing_columns_to_remove)
        
        # Define the desired column order - use Add_Spec (with underscore) like in Stock chart type
        desired_columns = [
            'Specification', 'Grade', 'OD', 'WT', 'OD_Category', 'WT_Schedule',
            f'({file1_date_str}) sheet data', f'({file2_date_str}) sheet data', 'Change in Stock', 'Status',
             'Add_Spec','Make', 'Branch'
        ]
        
        # Reorder columns - only include columns that exist in the dataframe
        available_columns = [col for col in desired_columns if col in comparison_data.columns]
        comparison_data = comparison_data[available_columns]
        
        # Include ALL items in heatmap (Added, Removed, Changed, Unchanged)
        return comparison_data
    else:
        return pd.DataFrame()