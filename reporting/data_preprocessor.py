"""
Data Preprocessor for Inventory Data

This module contains the exact data cleaning and normalization logic
used by the dashboard before heatmap generation.

IMPORTANT: This module copies preprocessing logic from the dashboard
to ensure identical data handling. No modifications to original logic.
"""

import pandas as pd

# Import grade derivation function from heatmap_generator
try:
    from reporting.heatmap_generator import derive_grade_from_spec
except ImportError:
    # Fallback if import fails
    derive_grade_from_spec = None

from reporting.logger import get_logger

logger = get_logger(__name__)


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names: strip, replace spaces/periods/dashes with underscores.
    Copied from dashboard logic.
    """
    df = df.copy()
    df.columns = [str(c).strip().replace(" ", "_").replace(".", "").replace("-", "_") for c in df.columns]
    return df


def process_incoming_sheet_mt_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle duplicate MT columns in Incoming sheet.
    The 2nd MT column contains the correct Incoming Stock MT values.
    Copied from dashboard logic - EXACT implementation.
    """
    df = df.copy()
    
    try:
        # Capture original column names before standardization
        original_columns = df.columns.tolist()
        
        # Find columns with exact name "MT" (before pandas adds suffixes)
        # Pandas creates "MT", "MT.1", "MT.2", "MT.3" for duplicate headers
        # EXACT match to dashboard logic - case-sensitive
        mt_column_indices = []
        for idx, col in enumerate(original_columns):
            col_str = str(col).strip()  # Case-sensitive like dashboard
            # Check for exact "MT" or pandas-generated "MT.1", "MT.2", etc.
            if col_str == "MT" or (col_str.startswith("MT.") and col_str[3:].isdigit()):
                mt_column_indices.append(idx)
        
        # Validate: must have at least 2 MT columns
        if len(mt_column_indices) < 2:
            logger.warning(f"Incoming sheet has less than 2 MT columns. Found: {len(mt_column_indices)}")
            logger.warning(f"Available columns: {original_columns}")
            # Return standardized version even if validation fails
            df = standardize_column_names(df)
            return df
        
        # Select the 2nd MT column (index 1 in mt_column_indices list)
        second_mt_idx = mt_column_indices[1]
        second_mt_col_original = original_columns[second_mt_idx]
        
        logger.debug(f"Incoming sheet: Found {len(mt_column_indices)} MT columns")
        logger.debug(f"Incoming sheet: 2nd MT column original name: '{second_mt_col_original}'")
        
        # After standardization, this will become "MT1" (from "MT.1")
        # So we need to track it through standardization
        # Standardize the column name to predict what it will become
        standardized_second_mt = str(second_mt_col_original).strip().replace(" ", "_").replace(".", "").replace("-", "_")
        
        logger.debug(f"Incoming sheet: Standardized 2nd MT column name: '{standardized_second_mt}'")
        
        # Apply standardization
        df = standardize_column_names(df)
        
        logger.debug(f"Incoming sheet: Columns after standardization: {df.columns.tolist()}")
        
        # Now find the standardized second MT column and overwrite df["MT"]
        # EXACT match to dashboard: just copy, no numeric conversion here (happens later)
        if standardized_second_mt in df.columns:
            # Copy values directly (dashboard does this, numeric conversion happens later)
            df["MT"] = df[standardized_second_mt].copy()
            
            # Log values for debugging (convert to numeric just for logging)
            mt_sum_for_log = pd.to_numeric(df["MT"], errors='coerce').fillna(0).sum()
            mt_non_zero = (pd.to_numeric(df["MT"], errors='coerce').fillna(0) != 0).sum()
            logger.info(f"Incoming sheet: Copied MT values from '{standardized_second_mt}' to 'MT'")
            logger.info(f"Incoming sheet: MT column stats - Sum: {mt_sum_for_log:.2f}, Non-zero rows: {mt_non_zero}, Total rows: {len(df)}")
            
            # Drop all other MT columns (keep only "MT")
            # Drop columns that are MT-related: MT1, MT2, MT3, etc. (but keep "MT")
            mt_cols_to_drop = [col for col in df.columns if col.startswith("MT") and col != "MT"]
            if mt_cols_to_drop:
                df = df.drop(columns=mt_cols_to_drop)
                logger.debug(f"Incoming sheet: Dropped MT columns: {mt_cols_to_drop}")
        else:
            logger.error(f"Could not find standardized MT column '{standardized_second_mt}' after processing")
            logger.error(f"Available columns: {df.columns.tolist()}")
            logger.error(f"Original 2nd MT column name was: '{second_mt_col_original}'")
            
            # Fallback: Try to find MT columns that start with "MT" (exact match, not substring)
            # This matches the dashboard's logic more closely
            mt_cols = [col for col in df.columns if str(col).strip().upper() in ["MT", "MT1", "MT2", "MT3"] or 
                      (str(col).strip().upper().startswith("MT") and len(str(col).strip()) <= 3)]
            if mt_cols:
                logger.warning(f"Found alternative MT columns: {mt_cols}")
                if len(mt_cols) > 1:
                    # Use the 2nd one (index 1) if available - this should be the correct Incoming MT
                    logger.warning(f"Using 2nd MT column: '{mt_cols[1]}'")
                    df["MT"] = df[mt_cols[1]].copy()
                    # Log stats
                    mt_sum = pd.to_numeric(df["MT"], errors='coerce').fillna(0).sum()
                    mt_non_zero = (pd.to_numeric(df["MT"], errors='coerce').fillna(0) != 0).sum()
                    logger.info(f"Incoming sheet: MT column stats - Sum: {mt_sum:.2f}, Non-zero rows: {mt_non_zero}")
                elif len(mt_cols) == 1:
                    # Only one MT column found - use it (might be wrong, but better than 0)
                    logger.warning(f"Only one MT column found, using: '{mt_cols[0]}'")
                    df["MT"] = df[mt_cols[0]].copy()
                    # Log stats
                    mt_sum = pd.to_numeric(df["MT"], errors='coerce').fillna(0).sum()
                    mt_non_zero = (pd.to_numeric(df["MT"], errors='coerce').fillna(0) != 0).sum()
                    logger.info(f"Incoming sheet: MT column stats - Sum: {mt_sum:.2f}, Non-zero rows: {mt_non_zero}")
            else:
                logger.error(f"No MT columns found in Incoming sheet after standardization!")
                logger.error(f"This will result in all Incoming values being 0")
                # Create empty MT column (will be 0, which is wrong but prevents crash)
                df["MT"] = 0
    
    except Exception as e:
        logger.error(f"Error processing Incoming sheet MT columns: {str(e)}", exc_info=True)
        # Return standardized version even if MT handling fails
        df = standardize_column_names(df)
    
    return df


def rename_add_spec_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize additional spec column names to "Add_Spec" for all sheets.
    Copied from dashboard logic.
    """
    df = df.copy()
    
    # Standardize additional spec column names to "Add_Spec" for all sheets
    add_spec_columns = [c for c in df.columns if c.lower() in ["add_spec", "addlspec", "addl_spec", "additional_spec"]]
    # Also check for the standardized version (AddlSpec becomes AddlSpec after dot removal)
    if not add_spec_columns:
        add_spec_columns = [c for c in df.columns if "addlspec" in c.lower()]
    
    if add_spec_columns:
        # Rename the first found additional spec column to "Add_Spec"
        df = df.rename(columns={add_spec_columns[0]: "Add_Spec"})
    
    return df


def add_grade_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Grade and Grade_Logic columns derived from Specification.
    Copied from dashboard logic.
    """
    df = df.copy()
    
    if derive_grade_from_spec is None:
        logger.warning("derive_grade_from_spec not available. Skipping grade derivation.")
        return df
    
    # Optimized Grade derivation using vectorized operations
    if 'Grade' not in df.columns and 'Specification' in df.columns:
        # Add Grade column derived from Specification (for display)
        df['Grade'] = df['Specification'].apply(lambda x: derive_grade_from_spec(x, combine_cs_as=False))
        
        # Add Grade_Logic column for internal categorization (CS & AS combined)
        df['Grade_Logic'] = df['Specification'].apply(lambda x: derive_grade_from_spec(x, combine_cs_as=True))
    
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic data cleaning: remove empty rows, fill NaN with empty string.
    Copied from dashboard logic.
    """
    df = df.copy()
    
    # Optimized data cleaning using vectorized operations
    df = df.dropna(how='all')  # Remove completely empty rows
    df = df.fillna('')  # Fill NaN values with empty string
    
    return df


def normalize_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert OD, WT, MT to numeric and normalize precision.
    Copied from dashboard logic used before heatmap generation.
    """
    df = df.copy()
    
    # Convert OD and WT to numeric, handling any non-numeric values
    if 'OD' in df.columns:
        df['OD'] = pd.to_numeric(df['OD'], errors='coerce')
        # Round to 3 decimal places to match filter options and prevent floating-point precision mismatches
        df['OD'] = df['OD'].round(3)
    
    if 'WT' in df.columns:
        df['WT'] = pd.to_numeric(df['WT'], errors='coerce')
        # Round to 3 decimal places
        df['WT'] = df['WT'].round(3)
    
    # Convert MT to numeric as well (treat blanks/invalid as 0 for aggregation)
    if 'MT' in df.columns:
        df['MT'] = pd.to_numeric(df['MT'], errors='coerce').fillna(0)
    
    return df


def normalize_specification_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Specification column: convert to string, replace 'nan', strip spaces.
    Copied from dashboard logic.
    """
    df = df.copy()
    
    if 'Specification' in df.columns:
        # Convert to string
        df['Specification'] = df['Specification'].astype(str)
        
        # Replace string 'nan' with empty string
        df['Specification'] = df['Specification'].replace('nan', '')
        
        # Strip leading/trailing spaces (e.g., 'STD ' → 'STD')
        df['Specification'] = df['Specification'].str.strip()
        
        # Replace empty strings with None for consistency
        df['Specification'] = df['Specification'].replace('', None)
    
    return df


def normalize_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert Make and Grade columns to string for consistent grouping.
    Copied from dashboard logic.
    """
    df = df.copy()
    
    # Convert Make and Grade to string to ensure consistent grouping
    if 'Make' in df.columns:
        df['Make'] = df['Make'].astype(str)
    
    if 'Grade' in df.columns:
        df['Grade'] = df['Grade'].astype(str)
    
    return df


def preprocess_inventory_sheet(
    xls: pd.ExcelFile,
    sheet_name: str
) -> pd.DataFrame:
    """
    Preprocess a single inventory sheet with all cleaning and normalization steps.
    
    This function applies the EXACT same preprocessing logic as the dashboard.
    
    Args:
        xls: ExcelFile object
        sheet_name: Name of the sheet to process ("Stock", "Incoming", or "Reservations")
    
    Returns:
        Preprocessed DataFrame ready for heatmap generation
    """
    if sheet_name not in xls.sheet_names:
        logger.warning(f"Sheet '{sheet_name}' not found in Excel file")
        return pd.DataFrame()
    
    try:
        # Step 1: Read with fixed header row (Excel row 2 = pandas header=1)
        # All sheets (Stock, Reservations, Incoming) use header row 1 - fixed format
        header_row = 1
        df = pd.read_excel(xls, sheet_name=sheet_name, header=header_row)
        
        logger.debug(f"Preprocessing {sheet_name}: Read {len(df)} rows with header row {header_row}")
        logger.debug(f"Preprocessing {sheet_name}: Initial columns: {df.columns.tolist()[:10]}...")  # First 10 columns
        
        # Step 2: Handle Incoming sheet MT columns (must be done before column standardization)
        if sheet_name == "Incoming":
            # Check MT columns before processing
            mt_cols_before = [col for col in df.columns if "MT" in str(col).upper()]
            logger.debug(f"Incoming sheet: MT columns before processing: {mt_cols_before}")
            
            if not mt_cols_before:
                logger.error(f"Incoming sheet: No MT columns found! Available columns: {df.columns.tolist()}")
            
            df = process_incoming_sheet_mt_columns(df)
            
            # Verify MT column exists and has values after processing
            if 'MT' in df.columns:
                mt_sum = pd.to_numeric(df['MT'], errors='coerce').fillna(0).sum()
                logger.info(f"Incoming sheet: MT column sum after processing: {mt_sum:.2f}")
            else:
                logger.warning(f"Incoming sheet: MT column missing after processing!")
        else:
            # Step 3: Standardize column names (for Stock and Reservations)
            df = standardize_column_names(df)
        
        # Step 4: Rename Add_Spec column
        df = rename_add_spec_column(df)
        
        # Step 5: Add Grade columns
        df = add_grade_columns(df)
        
        # Step 6: Basic cleaning
        df = clean_dataframe(df)
        
        # Step 7: Normalize numeric columns (OD, WT, MT)
        df = normalize_numeric_columns(df)
        
        # Step 8: Normalize string columns (Make, Grade)
        df = normalize_string_columns(df)
        
        # Step 9: Normalize Specification column
        df = normalize_specification_column(df)
        
        # Step 10: Final verification - ensure MT column is numeric (especially for Incoming)
        if sheet_name == "Incoming" and 'MT' in df.columns:
            # Double-check MT column is numeric and log stats
            df['MT'] = pd.to_numeric(df['MT'], errors='coerce').fillna(0)
            mt_sum = df['MT'].sum()
            mt_non_zero = (df['MT'] != 0).sum()
            logger.info(f"Incoming sheet final check - MT Sum: {mt_sum:.2f}, Non-zero rows: {mt_non_zero}/{len(df)}")
            if mt_sum == 0 and len(df) > 0:
                logger.warning(f"WARNING: Incoming MT sum is 0 but sheet has {len(df)} rows - MT column may not be read correctly!")
        
        logger.info(f"Preprocessed {sheet_name}: {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"Error preprocessing sheet '{sheet_name}': {str(e)}", exc_info=True)
        return pd.DataFrame()


def preprocess_inventory_data(file_path: str) -> dict:
    """
    Preprocess all inventory sheets from Excel file.
    
    This function applies the EXACT same preprocessing logic as the dashboard's
    load_inventory_data() function, but without Streamlit dependencies.
    
    Args:
        file_path: Path to Excel file
    
    Returns:
        Dictionary with keys: 'Stock', 'Reservations', 'Incoming'
        Each value is a preprocessed pandas DataFrame
    """
    try:
        xls = pd.ExcelFile(file_path)
    except Exception as e:
        raise ValueError(f"Failed to open Excel file: {str(e)}")
    
    # Validate required sheets exist
    required_sheets = ["Stock", "Incoming", "Reservations"]
    missing_sheets = [sheet for sheet in required_sheets if sheet not in xls.sheet_names]
    if missing_sheets:
        raise ValueError(f"Missing required sheets: {missing_sheets}")
    
    sheets = {}
    
    for sheet_name in required_sheets:
        df = preprocess_inventory_sheet(xls, sheet_name)
        sheets[sheet_name] = df
    
    return sheets

