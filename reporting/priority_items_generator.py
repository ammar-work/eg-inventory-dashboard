"""
Priority Items Generator Module

This module generates a list of top specifications by Total Free For Sale MT.
The output is used to create the priority items table in the email body.

Logic:
- Calculates Free For Sale = Stock_MT + Incoming_MT - Reservation_MT
- Aggregates at Specification level
- Filters by threshold (>= 30 MT by default)
- Sorts descending and returns top N specifications

This module is pure data processing - no UI, email, or PDF logic.
"""

import pandas as pd
from typing import Tuple, Optional
from reporting.config import PRIORITY_THRESHOLD_MT, PRIORITY_TOP_N
from reporting.logger import get_logger

logger = get_logger(__name__)


def generate_priority_items(
    stock_df: pd.DataFrame,
    reservations_df: pd.DataFrame,
    incoming_df: pd.DataFrame,
    threshold_mt: Optional[float] = None,
    top_n: Optional[int] = None
) -> Tuple[bool, Optional[pd.DataFrame], Optional[str]]:
    """
    Generate top specifications by Total Free For Sale MT.
    
    This function:
    1. Calculates Free For Sale = Stock_MT + Incoming_MT - Reservation_MT
    2. Aggregates at Specification level
    3. Filters by threshold (default: >= 30 MT)
    4. Sorts descending and returns top N (default: 15)
    
    Args:
        stock_df: Preprocessed Stock DataFrame (must have 'Specification' and 'MT' columns)
        reservations_df: Preprocessed Reservations DataFrame (must have 'Specification' and 'MT' columns)
        incoming_df: Preprocessed Incoming DataFrame (must have 'Specification' and 'MT' columns)
        threshold_mt: Minimum Free For Sale MT threshold (default: from config)
        top_n: Number of top specifications to return (default: from config)
    
    Returns:
        Tuple of (success: bool, result_df: Optional[DataFrame], error_msg: Optional[str])
        - success: True if generation succeeded, False otherwise
        - result_df: DataFrame with columns ['Specification', 'Total_Free_For_Sale_MT'] if successful
        - error_msg: Error message if generation failed
    
    Raises:
        No exceptions raised - all errors are caught and returned in the tuple
    """
    try:
        # Use config defaults if not provided
        if threshold_mt is None:
            threshold_mt = PRIORITY_THRESHOLD_MT
        if top_n is None:
            top_n = PRIORITY_TOP_N
        
        logger.info(f"Generating priority items: threshold={threshold_mt} MT, top_n={top_n}")
        
        # Validate required columns
        required_columns = ['Specification', 'MT']
        
        for df_name, df in [('Stock', stock_df), ('Reservations', reservations_df), ('Incoming', incoming_df)]:
            if df.empty:
                logger.warning(f"{df_name} DataFrame is empty - will use 0 MT for this source")
                continue
            
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                error_msg = f"{df_name} DataFrame missing required columns: {missing_cols}"
                logger.error(error_msg)
                return False, None, error_msg
        
        # Step 1: Aggregate MT by Specification for each DataFrame
        logger.debug("Aggregating MT by Specification for each source")
        
        # Stock aggregation
        if not stock_df.empty:
            stock_agg = stock_df.groupby('Specification')['MT'].sum().reset_index()
            stock_agg.columns = ['Specification', 'Stock_MT']
            # Ensure MT is numeric
            stock_agg['Stock_MT'] = pd.to_numeric(stock_agg['Stock_MT'], errors='coerce').fillna(0)
        else:
            stock_agg = pd.DataFrame(columns=['Specification', 'Stock_MT'])
            logger.warning("Stock DataFrame is empty - using 0 MT for all specifications")
        
        # Reservations aggregation
        if not reservations_df.empty:
            reservations_agg = reservations_df.groupby('Specification')['MT'].sum().reset_index()
            reservations_agg.columns = ['Specification', 'Reservation_MT']
            # Ensure MT is numeric
            reservations_agg['Reservation_MT'] = pd.to_numeric(
                reservations_agg['Reservation_MT'], errors='coerce'
            ).fillna(0)
        else:
            reservations_agg = pd.DataFrame(columns=['Specification', 'Reservation_MT'])
            logger.warning("Reservations DataFrame is empty - using 0 MT for all specifications")
        
        # Incoming aggregation
        if not incoming_df.empty:
            incoming_agg = incoming_df.groupby('Specification')['MT'].sum().reset_index()
            incoming_agg.columns = ['Specification', 'Incoming_MT']
            # Ensure MT is numeric
            incoming_agg['Incoming_MT'] = pd.to_numeric(
                incoming_agg['Incoming_MT'], errors='coerce'
            ).fillna(0)
        else:
            incoming_agg = pd.DataFrame(columns=['Specification', 'Incoming_MT'])
            logger.warning("Incoming DataFrame is empty - using 0 MT for all specifications")
        
        # Step 2: Merge all aggregations
        logger.debug("Merging aggregated data from all sources")
        
        # Start with all unique specifications from all sources
        all_specs = set()
        if not stock_agg.empty:
            all_specs.update(stock_agg['Specification'].dropna().unique())
        if not reservations_agg.empty:
            all_specs.update(reservations_agg['Specification'].dropna().unique())
        if not incoming_agg.empty:
            all_specs.update(incoming_agg['Specification'].dropna().unique())
        
        # Create base DataFrame with all specifications
        result_df = pd.DataFrame({'Specification': list(all_specs)})
        
        # Merge each aggregation (left join to keep all specifications)
        result_df = result_df.merge(stock_agg, on='Specification', how='left')
        result_df = result_df.merge(reservations_agg, on='Specification', how='left')
        result_df = result_df.merge(incoming_agg, on='Specification', how='left')
        
        # Fill NaN values with 0 (for specifications that don't exist in a source)
        result_df['Stock_MT'] = result_df['Stock_MT'].fillna(0)
        result_df['Reservation_MT'] = result_df['Reservation_MT'].fillna(0)
        result_df['Incoming_MT'] = result_df['Incoming_MT'].fillna(0)
        
        # Step 3: Calculate Total Free For Sale MT
        # Formula: Free For Sale = Stock_MT + Incoming_MT - Reservation_MT
        logger.debug("Calculating Total Free For Sale MT")
        result_df['Total_Free_For_Sale_MT'] = (
            result_df['Stock_MT'] +
            result_df['Incoming_MT'] -
            result_df['Reservation_MT']
        )
        
        # Ensure Total_Free_For_Sale_MT is numeric and rounded to 2 decimal places
        result_df['Total_Free_For_Sale_MT'] = pd.to_numeric(
            result_df['Total_Free_For_Sale_MT'], errors='coerce'
        ).fillna(0).round(2)
        
        # Step 4: Filter by threshold
        logger.debug(f"Filtering specifications with Free For Sale >= {threshold_mt} MT")
        filtered_df = result_df[result_df['Total_Free_For_Sale_MT'] >= threshold_mt].copy()
        
        if filtered_df.empty:
            logger.warning(f"No specifications found with Free For Sale >= {threshold_mt} MT")
            # Return empty DataFrame with correct columns
            return True, pd.DataFrame(columns=['Specification', 'Total_Free_For_Sale_MT']), None
        
        # Step 5: Sort descending by Total_Free_For_Sale_MT
        logger.debug("Sorting by Total Free For Sale MT (descending)")
        filtered_df = filtered_df.sort_values('Total_Free_For_Sale_MT', ascending=False)
        
        # Step 6: Select top N
        logger.debug(f"Selecting top {top_n} specifications")
        top_df = filtered_df.head(top_n).copy()
        
        # Step 7: Select only required columns
        final_df = top_df[['Specification', 'Total_Free_For_Sale_MT']].copy()
        
        # Ensure Specification is string and clean
        final_df['Specification'] = final_df['Specification'].astype(str)
        final_df['Specification'] = final_df['Specification'].str.strip()
        
        # Reset index for clean output
        final_df = final_df.reset_index(drop=True)
        
        logger.info(f"Generated {len(final_df)} priority items")
        logger.debug(f"Top specification: {final_df.iloc[0]['Specification']} "
                    f"({final_df.iloc[0]['Total_Free_For_Sale_MT']:.2f} MT)")
        
        return True, final_df, None
        
    except Exception as e:
        error_msg = f"Error generating priority items: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, None, error_msg

