#!/usr/bin/env python3
"""
Cron Runner Script for Inventory Reporting Pipeline

This script is designed to be executed by cron for automated weekly reporting.
It triggers the complete inventory reporting pipeline with S3 file fetching.

CRON CONFIGURATION:
-------------------
# Run every Tuesday at 11:00 AM IST (05:30 AM UTC)
# IST = UTC + 5:30, so Tuesday 11:00 AM IST = Tuesday 5:30 AM UTC
30 5 * * 2 /usr/bin/python3 /path/to/project/scripts/run_inventory_report.py >> /path/to/project/logs/cron.log 2>&1

CRON EXPRESSION BREAKDOWN:
- 30: Minute (30th minute)
- 5: Hour (5 AM UTC = 11:00 AM IST, accounting for IST = UTC + 5:30)
- *: Day of month (any)
- *: Month (any)
- 2: Day of week (Tuesday, where 0=Sunday, 1=Monday, 2=Tuesday)

TIMEZONE NOTES:
- Server timezone: UTC (assumed)
- Target execution: Tuesday 11:00 AM IST
- IST = UTC + 5:30
- Therefore: 11:00 AM IST = 5:30 AM UTC

ENVIRONMENT VARIABLES:
----------------------
Cron does NOT automatically load .env files. You must ensure environment variables
are available to cron. Options:

1. Set in crontab:
   AWS_ACCESS_KEY_ID=...
   AWS_SECRET_ACCESS_KEY=...
   INVENTORY_S3_BUCKET=...
   # ... etc

2. Use wrapper shell script (see scripts/run_inventory_report.sh)

3. Load via system-wide environment configuration

REQUIRED ENVIRONMENT VARIABLES:
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_REGION (default: us-east-1)
- INVENTORY_S3_BUCKET
- INVENTORY_S3_PREFIX (optional)
- SMTP_SERVER
- SMTP_USER
- SMTP_PASSWORD
- SMTP_PORT (optional, default: 587)

LOGGING:
--------
- Cron output: logs/cron.log (stdout/stderr from this script)
- Application logs: logs/report_generator.log (from reporting modules)
"""

import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import orchestrator
from reporting.orchestrator import run_inventory_reporting_pipeline

def main():
    """
    Main entry point for cron execution.
    
    This function:
    1. Calls the inventory reporting pipeline with S3 fetch
    2. Prints minimal status output
    3. Exits with appropriate exit code
    """
    try:
        print("=" * 70)
        print("CRON: Starting Inventory Reporting Pipeline")
        print("=" * 70)
        print()
        
        # Run pipeline with S3 fetch
        # excel_file_path=None → forces S3 fetch
        # dry_run_email=False → actually send emails
        # report_date=None → allow S3 LastModified logic to apply
        success, result = run_inventory_reporting_pipeline(
            excel_file_path=None,  # Force S3 fetch
            report_date=None,      # Use S3 LastModified
            dry_run_email=False    # Actually send emails
        )
        
        print()
        print("=" * 70)
        
        if success:
            print("CRON: Pipeline completed successfully")
            print(f"PDF Report: {result}")
            print("=" * 70)
            print()
            sys.exit(0)
        else:
            print("CRON: Pipeline failed")
            print(f"Error: {result}")
            print("=" * 70)
            print()
            sys.exit(1)
            
    except Exception as e:
        print()
        print("=" * 70)
        print("CRON: Unexpected error in pipeline execution")
        print(f"Error: {str(e)}")
        print("=" * 70)
        print()
        sys.exit(1)


if __name__ == "__main__":
    main()

