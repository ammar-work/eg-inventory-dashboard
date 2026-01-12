"""
Recipient Resolver Module

This module provides dynamic email recipient fetching from ERP MySQL database
with safe fallback to environment-based recipients.

CRITICAL SAFETY:
- READ-ONLY database access only
- No writes, deletes, or updates
- No PII logging (email addresses, names)
- Parameterized queries only
- Graceful fallback on any failure
"""

import os
from typing import List, Optional, Tuple
import logging

from reporting.logger import get_logger

logger = get_logger(__name__)

# Try to import MySQL connector (graceful fallback if not installed)
MYSQL_AVAILABLE = False
MYSQL_CONNECTOR_TYPE = None

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
    MYSQL_AVAILABLE = True
    MYSQL_CONNECTOR_TYPE = 'mysql.connector'
except ImportError:
    try:
        import pymysql
        MYSQL_AVAILABLE = True
        MYSQL_CONNECTOR_TYPE = 'pymysql'
    except ImportError:
        MYSQL_AVAILABLE = False
        MYSQL_CONNECTOR_TYPE = None


def _get_db_connection() -> Optional[object]:
    """
    Create a read-only MySQL database connection.
    
    Returns:
        MySQL connection object or None if connection fails
        
    Environment Variables Required:
        ERP_DB_HOST: Database hostname
        ERP_DB_PORT: Database port (default: 3306)
        ERP_DB_NAME: Database name
        ERP_DB_USER: Database username (read-only user)
        ERP_DB_PASSWORD: Database password
    """
    if not MYSQL_AVAILABLE:
        logger.debug("MySQL connector not available. Skipping DB connection.")
        return None
    
    try:
        db_host = os.getenv('ERP_DB_HOST')
        db_port = int(os.getenv('ERP_DB_PORT', '3306'))
        db_name = os.getenv('ERP_DB_NAME')
        db_user = os.getenv('ERP_DB_USER')
        db_password = os.getenv('ERP_DB_PASSWORD')
        
        # Validate required configuration
        if not all([db_host, db_name, db_user, db_password]):
            missing = []
            if not db_host:
                missing.append('ERP_DB_HOST')
            if not db_name:
                missing.append('ERP_DB_NAME')
            if not db_user:
                missing.append('ERP_DB_USER')
            if not db_password:
                missing.append('ERP_DB_PASSWORD')
            
            logger.debug(f"Database configuration incomplete. Missing: {', '.join(missing)}")
            return None
        
        # Create connection (read-only)
        if MYSQL_CONNECTOR_TYPE == 'mysql.connector':
            # Using mysql-connector-python
            connection = mysql.connector.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password,
                autocommit=False  # Explicitly disable autocommit for safety
            )
        elif MYSQL_CONNECTOR_TYPE == 'pymysql':
            # Using pymysql
            connection = pymysql.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password,
                autocommit=False,
                read_only=True  # Explicit read-only mode
            )
        else:
            raise ImportError("No MySQL connector available")
        
        logger.info(f"Database connection established to {db_host}:{db_port}/{db_name}")
        return connection
        
    except Exception as e:
        logger.warning(f"Failed to connect to ERP database: {str(e)}")
        return None


def _build_where_clause(filters: dict) -> Tuple[str, List]:
    """
    Build parameterized WHERE clause from filters.
    
    Args:
        filters: Dictionary of filter name -> value(s)
        
    Returns:
        Tuple of (WHERE clause string, parameter list)
        
    Example:
        filters = {'status': '1', 'department': 'Sales,Marketing'}
        Returns: ("WHERE status = %s AND department IN (%s, %s)", ['1', 'Sales', 'Marketing'])
    """
    conditions = []
    params = []
    
    for filter_name, filter_value in filters.items():
        if not filter_value or not filter_value.strip():
            continue
        
        # Handle comma-separated values (IN clause)
        values = [v.strip() for v in filter_value.split(',') if v.strip()]
        if not values:
            continue
        
        if len(values) == 1:
            # Single value - use equality
            conditions.append(f"{filter_name} = %s")
            params.append(values[0])
        else:
            # Multiple values - use IN clause
            placeholders = ','.join(['%s'] * len(values))
            conditions.append(f"{filter_name} IN ({placeholders})")
            params.extend(values)
    
    if not conditions:
        return "", []
    
    where_clause = "WHERE " + " AND ".join(conditions)
    return where_clause, params


def fetch_recipients_from_db() -> Tuple[bool, List[str], Optional[str]]:
    """
    Fetch email recipients from ERP MySQL database based on config-driven filters.
    
    This function:
    1. Connects to MySQL database (read-only)
    2. Applies filters from environment variables
    3. Returns list of email addresses
    4. Handles all failures gracefully
    
    Returns:
        Tuple of (success: bool, recipients: List[str], error_message: Optional[str])
        - success: True if recipients were fetched successfully, False otherwise
        - recipients: List of email addresses (empty list if failed)
        - error_message: Error message if fetch failed, None if successful
        
    Environment Variables for Filtering (all optional except status):
        ERP_RECIPIENT_FILTER_STATUS: Status filter (default: 1)
        ERP_RECIPIENT_FILTER_DEPARTMENT: Department filter (comma-separated)
        ERP_RECIPIENT_FILTER_DESIGNATION: Designation filter (comma-separated)
        
    Database Configuration:
        ERP_DB_HOST: Database hostname
        ERP_DB_PORT: Database port (default: 3306)
        ERP_DB_NAME: Database name
        ERP_DB_USER: Database username
        ERP_DB_PASSWORD: Database password
        
    Safety:
        - READ-ONLY access only
        - Parameterized queries
        - No PII logging
        - Graceful fallback on failure
    """
    if not MYSQL_AVAILABLE:
        error_msg = "MySQL connector not available"
        logger.debug(error_msg)
        return False, [], error_msg
    
    connection = None
    cursor = None
    
    try:
        # Step 1: Connect to database
        connection = _get_db_connection()
        if not connection:
            error_msg = "Failed to establish database connection"
            logger.warning(error_msg)
            return False, [], error_msg
        
        # Step 2: Build filters from environment variables
        filters = {}
        
        # Status filter (default: 1 if not provided)
        status_filter = os.getenv('ERP_RECIPIENT_FILTER_STATUS', '1')
        if status_filter and status_filter.strip():
            filters['status'] = status_filter.strip()
        
        # Department filter (optional)
        dept_filter = os.getenv('ERP_RECIPIENT_FILTER_DEPARTMENT', '')
        if dept_filter and dept_filter.strip():
            filters['department'] = dept_filter.strip()
        
        # Designation filter (optional)
        desig_filter = os.getenv('ERP_RECIPIENT_FILTER_DESIGNATION', '')
        if desig_filter and desig_filter.strip():
            filters['designation'] = desig_filter.strip()
        
        # Log applied filters (without values for safety)
        applied_filters = list(filters.keys())
        logger.info(f"Fetching recipients from ERP database with filters: {', '.join(applied_filters)}")
        
        # Step 3: Build parameterized query
        where_clause, params = _build_where_clause(filters)
        
        # Base query - select only email and name (name for future use, not logged)
        query = f"SELECT email, name FROM users {where_clause}"
        
        # Step 4: Execute query
        cursor = connection.cursor()
        cursor.execute(query, params)
        
        # Step 5: Fetch results
        results = cursor.fetchall()
        
        # Step 6: Extract email addresses
        recipients = []
        for row in results:
            if row and len(row) > 0 and row[0]:  # row[0] is email
                email = str(row[0]).strip()
                if email and '@' in email:  # Basic email validation
                    recipients.append(email)
        
        # Step 7: Log results (count only, no PII)
        logger.info(f"Fetched {len(recipients)} recipient(s) from ERP database")
        
        if len(recipients) == 0:
            logger.warning("No recipients found in database with applied filters")
            return False, [], "No recipients found in database"
        
        # Success
        return True, recipients, None
        
    except Exception as e:
        error_msg = f"Error fetching recipients from database: {str(e)}"
        logger.warning(error_msg, exc_info=True)
        return False, [], error_msg
        
    finally:
        # Clean up resources
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        
        if connection:
            try:
                connection.close()
                logger.debug("Database connection closed")
            except Exception:
                pass

