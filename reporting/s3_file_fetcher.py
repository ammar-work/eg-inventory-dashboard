"""
S3 File Fetcher Module (READ-ONLY)

This module provides read-only access to S3 to fetch the latest inventory Excel file.
It performs STRICTLY read-only operations - no writes, deletes, or modifications to S3.

CRITICAL SAFETY:
- Only uses: list_objects_v2, head_object, get_object
- NO uploads, deletes, overwrites, or metadata changes
- 100% non-destructive to client S3 bucket
"""

import os
import boto3
from typing import Tuple, Optional
from datetime import datetime
from pathlib import Path

from reporting.logger import get_logger
from reporting.config import REPORTS_DIR

logger = get_logger(__name__)


def _get_s3_client():
    """
    Initialize boto3 S3 client with read-only credentials.
    
    Returns:
        boto3 S3 client or None if configuration is missing
    
    Environment Variables Required:
        AWS_ACCESS_KEY_ID: AWS access key
        AWS_SECRET_ACCESS_KEY: AWS secret key
        AWS_REGION: AWS region (default: us-east-1)
    """
    try:
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = os.getenv('AWS_REGION', 'us-east-1')
        
        if not aws_access_key or not aws_secret_key:
            logger.error("AWS credentials not configured. Missing AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY")
            return None
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )
        
        logger.debug(f"S3 client initialized for region: {aws_region}")
        return s3_client
        
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {str(e)}", exc_info=True)
        return None


def fetch_latest_inventory_file(
    use_local_file: Optional[str] = None
) -> Tuple[bool, Optional[str], Optional[datetime], Optional[str]]:
    """
    Fetch the latest inventory Excel file from S3 or use local file override.
    
    This function performs STRICTLY read-only S3 operations:
    - list_objects_v2: List files in S3 bucket
    - get_object: Download file from S3
    
    NO write, delete, or modification operations are performed.
    
    Args:
        use_local_file: Optional local file path to use instead of S3.
                       If provided, S3 operations are skipped entirely.
                       Useful for development and testing.
    
    Returns:
        Tuple of (success: bool, local_file_path: Optional[str], last_modified: Optional[datetime], error_message: Optional[str])
        - success: True if file was fetched successfully, False otherwise
        - local_file_path: Path to downloaded/local file, or None if failed
        - last_modified: S3 object LastModified timestamp (timezone-aware datetime), or None if use_local_file or failed
        - error_message: Error message if fetch failed, None if successful
    
    Environment Variables Required (if use_local_file is None):
        AWS_ACCESS_KEY_ID: AWS access key
        AWS_SECRET_ACCESS_KEY: AWS secret key
        AWS_REGION: AWS region (default: us-east-1)
        INVENTORY_S3_BUCKET: S3 bucket name
        INVENTORY_S3_PREFIX: S3 folder prefix (optional)
        INVENTORY_FILE_EXTENSION: File extension to filter (default: .xlsx)
    
    Example:
        success, file_path, error = fetch_latest_inventory_file()
        if success:
            # Use file_path for processing
            pass
    """
    try:
        # Local file override (for testing/development)
        if use_local_file:
            logger.info(f"Using local file override: {use_local_file}")
            
            if not os.path.exists(use_local_file):
                error_msg = f"Local file not found: {use_local_file}"
                logger.error(error_msg)
                return False, None, None, error_msg
            
            if not os.path.isfile(use_local_file):
                error_msg = f"Local path is not a file: {use_local_file}"
                logger.error(error_msg)
                return False, None, None, error_msg
            
            logger.info(f"Local file validated: {use_local_file}")
            # Return None for last_modified when using local file
            return True, use_local_file, None, None
        
        # S3 fetch logic (read-only operations only)
        logger.info("Fetching latest inventory file from S3...")
        
        # Read configuration from environment variables
        bucket_name = os.getenv('INVENTORY_S3_BUCKET')
        s3_prefix = os.getenv('INVENTORY_S3_PREFIX', '')
        file_extension = os.getenv('INVENTORY_FILE_EXTENSION', '.xlsx')
        
        if not bucket_name:
            error_msg = "INVENTORY_S3_BUCKET environment variable is not set"
            logger.error(error_msg)
            return False, None, None, error_msg
        
        logger.info(f"S3 Bucket: {bucket_name}")
        logger.info(f"S3 Prefix: {s3_prefix if s3_prefix else '(root)'}")
        logger.info(f"File extension filter: {file_extension}")
        
        # Initialize S3 client
        s3_client = _get_s3_client()
        if not s3_client:
            error_msg = "Failed to initialize S3 client"
            return False, None, None, error_msg
        
        # List objects in S3 (READ-ONLY: list_objects_v2)
        logger.info("Listing objects in S3 bucket...")
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=bucket_name,
                Prefix=s3_prefix
            )
            
            all_objects = []
            for page in pages:
                if 'Contents' in page:
                    all_objects.extend(page['Contents'])
            
            logger.info(f"Found {len(all_objects)} objects in S3 bucket")
            
        except Exception as e:
            error_msg = f"Failed to list objects from S3: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, None, None, error_msg
        
        if not all_objects:
            error_msg = f"No files found in S3 bucket '{bucket_name}' with prefix '{s3_prefix}'"
            logger.error(error_msg)
            return False, None, None, error_msg
        
        # Filter for Excel files
        valid_extensions = ['.xlsx', '.xls']
        if file_extension not in valid_extensions:
            logger.warning(f"File extension '{file_extension}' not in standard Excel extensions. Using .xlsx")
            file_extension = '.xlsx'
        
        excel_files = []
        for obj in all_objects:
            key = obj.get('Key', '')
            size = obj.get('Size', 0)
            
            # Filter by extension
            if not key.lower().endswith(file_extension.lower()):
                continue
            
            # Ignore zero-byte files
            if size == 0:
                logger.debug(f"Skipping zero-byte file: {key}")
                continue
            
            # Ignore temp/hidden files (common patterns)
            key_lower = key.lower()
            if any(pattern in key_lower for pattern in ['~$', '.tmp', '.temp', '._']):
                logger.debug(f"Skipping temp/hidden file: {key}")
                continue
            
            excel_files.append(obj)
        
        logger.info(f"Found {len(excel_files)} valid Excel files after filtering")
        
        if not excel_files:
            error_msg = f"No valid Excel files found in S3 bucket '{bucket_name}' with prefix '{s3_prefix}'"
            logger.error(error_msg)
            return False, None, None, error_msg
        
        # Find latest file by LastModified timestamp (not filename)
        latest_file = max(excel_files, key=lambda x: x['LastModified'])
        latest_key = latest_file['Key']
        latest_modified = latest_file['LastModified']  # This is already timezone-aware from S3
        latest_size = latest_file['Size']
        
        logger.info(f"Latest file identified: {latest_key}")
        logger.info(f"S3 file last modified at: {latest_modified}")
        logger.info(f"File size: {latest_size} bytes")
        
        # Download file from S3 (READ-ONLY: get_object)
        logger.info("Downloading file from S3...")
        
        # Create local directory for downloaded files
        input_dir = Path(REPORTS_DIR) / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate local filename (use timestamp to avoid conflicts)
        timestamp = latest_modified.strftime('%Y%m%d_%H%M%S')
        local_filename = f"latest_inventory_{timestamp}{file_extension}"
        local_file_path = input_dir / local_filename
        
        try:
            # Download file (READ-ONLY operation)
            s3_client.download_file(
                Bucket=bucket_name,
                Key=latest_key,
                Filename=str(local_file_path)
            )
            
            # Verify downloaded file
            if not os.path.exists(local_file_path):
                error_msg = f"Downloaded file not found at expected path: {local_file_path}"
                logger.error(error_msg)
                return False, None, None, error_msg
            
            downloaded_size = os.path.getsize(local_file_path)
            if downloaded_size == 0:
                error_msg = f"Downloaded file is empty: {local_file_path}"
                logger.error(error_msg)
                # Clean up empty file
                try:
                    os.remove(local_file_path)
                except Exception:
                    pass
                return False, None, None, error_msg
            
            if downloaded_size != latest_size:
                logger.warning(f"Downloaded file size ({downloaded_size}) differs from S3 size ({latest_size})")
            
            logger.info(f"File downloaded successfully: {local_file_path}")
            logger.info(f"Downloaded size: {downloaded_size} bytes")
            
            # Return success with file path and LastModified timestamp
            return True, str(local_file_path), latest_modified, None
            
        except Exception as e:
            error_msg = f"Failed to download file from S3: {str(e)}"
            logger.error(error_msg, exc_info=True)
            # Clean up partial download if it exists
            if os.path.exists(local_file_path):
                try:
                    os.remove(local_file_path)
                except Exception:
                    pass
            return False, None, None, error_msg
        
    except Exception as e:
        error_msg = f"Unexpected error in fetch_latest_inventory_file: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, None, None, error_msg

