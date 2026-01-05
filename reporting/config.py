"""
Configuration file for automated inventory reporting.

All configurable values must be defined here - no hardcoded values in logic files.
Update these values as needed without modifying the implementation code.

IMPORTANT: Sensitive values (email recipients, ERP links) are read from environment variables.
Set these in your .env file or system environment before running the pipeline.
"""

import os
import logging

# Set up logger for configuration warnings
_logger = logging.getLogger(__name__)

# ============================================================================
# Email Configuration
# ============================================================================

# Email recipients are read from environment variable EMAIL_RECIPIENTS
# Expected format in .env: EMAIL_RECIPIENTS=abc@company.com,xyz@company.com
# Values are split by comma, stripped of whitespace, and empty values are ignored
_email_recipients_str = os.getenv("EMAIL_RECIPIENTS", "")

if _email_recipients_str:
    # Split by comma, strip whitespace, filter out empty values
    EMAIL_RECIPIENTS = [
        email.strip() 
        for email in _email_recipients_str.split(",") 
        if email.strip()
    ]
else:
    EMAIL_RECIPIENTS = []

# Log warning if no recipients configured (but don't log actual email addresses)
if not EMAIL_RECIPIENTS:
    _logger.warning(
        "EMAIL_RECIPIENTS environment variable is not set or is empty. "
        "Pipeline will run but no emails will be sent. "
        "Set EMAIL_RECIPIENTS in .env file (comma-separated list of email addresses)."
    )
else:
    _logger.info(f"Loaded {len(EMAIL_RECIPIENTS)} email recipient(s) from environment variable")

# Email subject template
# {date} will be replaced with the file upload date (format: DD-MMM-YYYY)
EMAIL_SUBJECT_TEMPLATE = "Weekly Inventory Report - {date}"

# ============================================================================
# ERP System Link
# ============================================================================

# ERP system link is read from environment variable ERP_SYSTEM_LINK
# Expected format in .env: ERP_SYSTEM_LINK=https://erp.company.com/
# Defaults to empty string if not set (pipeline will still run)
ERP_SYSTEM_LINK = os.getenv("ERP_SYSTEM_LINK", "")

if not ERP_SYSTEM_LINK:
    _logger.warning(
        "ERP_SYSTEM_LINK environment variable is not set. "
        "ERP link in email body will be empty. "
        "Set ERP_SYSTEM_LINK in .env file if needed."
    )
else:
    _logger.info("ERP_SYSTEM_LINK loaded from environment variable")

# ============================================================================
# Report Specifications
# ============================================================================

# Fixed list of 6 specifications to include in PDF report
# These specifications remain the same every week
PDF_SPECIFICATIONS = [
    "CSSMP106B",
    "ASSMPP11",
    "ASSMPP22",
    "ASSMPP9",
    "ASSMPP5",
    "ASSMPP91"
]

# ============================================================================
# Priority Items Configuration
# ============================================================================

# Minimum Free For Sale MT threshold for priority items
# Only specifications with Free For Sale >= this value will be considered
PRIORITY_THRESHOLD_MT = 30

# Number of top specifications to include in email priority items table
PRIORITY_TOP_N = 15

# ============================================================================
# Report Titles and Headings
# ============================================================================

# Title displayed on PDF report cover page
REPORT_TITLE = "INVENTORY REPORT"

# Heading displayed in email body
EMAIL_HEADING = "Inventory Report - Priority Items for Sales Focus"

# ============================================================================
# Scheduling Configuration
# ============================================================================

# Day of week for report generation (lowercase 3-letter abbreviation)
# Options: 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'
SCHEDULE_DAY = "tue"  # Tuesday

# Hour (24-hour format) for report generation
SCHEDULE_HOUR = 11  # 11 AM

# Minute for report generation
SCHEDULE_MINUTE = 0

# Timezone for scheduling
# Use IANA timezone database names (e.g., 'Asia/Kolkata', 'America/New_York')
SCHEDULE_TIMEZONE = "Asia/Kolkata"

# ============================================================================
# Email Sending Configuration
# ============================================================================

# Delay in seconds between sending emails to different recipients
# This helps avoid SMTP rate limits when sending to ~20 recipients
# Recommended: 1-2 seconds
EMAIL_DELAY_SECONDS = 1.5

# ============================================================================
# File Paths and Directories
# ============================================================================

# Directory for storing generated reports (PDFs and temporary images)
# Relative to project root
REPORTS_DIR = "reports"

# Directory for log files
# Relative to project root
LOGS_DIR = "logs"

# Log file name
LOG_FILENAME = "report_generator.log"

# ============================================================================
# SMTP Configuration
# ============================================================================

# SMTP settings are read from environment variables for security:
# - SMTP_SERVER: SMTP server address (e.g., 'smtp.gmail.com')
# - SMTP_PORT: SMTP port (default: 587 for TLS)
# - SMTP_USER: SMTP username/email
# - SMTP_PASSWORD: SMTP password or app-specific password

# Default SMTP port (used if SMTP_PORT environment variable is not set)
DEFAULT_SMTP_PORT = 587

# ============================================================================
# Date Format Configuration
# ============================================================================

# Date format for display in PDF and email
# Format: DD-MMM-YYYY (e.g., "15-Jan-2024")
DATE_FORMAT_DISPLAY = "%d-%b-%Y"

# Date format for PDF filename
# Format: YYYYMMDD (e.g., "2026_01_03")
DATE_FORMAT_FILENAME = "%Y_%m_%d"

# ============================================================================
# PDF Configuration
# ============================================================================

# PDF page size (options: 'A4', 'letter', etc.)
# Landscape orientation is used for heatmap images
PDF_PAGE_SIZE = "A4"

# PDF filename prefix
PDF_FILENAME_PREFIX = "inventory_report_"

# ============================================================================
# Heatmap Image Configuration
# ============================================================================

# Temporary heatmap image filename prefix
HEATMAP_IMAGE_PREFIX = "temp_heatmap_"

# Heatmap image file extension
HEATMAP_IMAGE_EXTENSION = ".png"

# ============================================================================
# Notes
# ============================================================================

# IMPORTANT:
# - All email addresses, links, and sensitive values should be updated before deployment
# - SMTP credentials should be stored in environment variables, not in this file
# - Do not commit sensitive information to version control
# - Test all configuration values before production deployment

