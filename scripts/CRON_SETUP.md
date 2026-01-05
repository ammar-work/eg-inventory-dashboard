# Cron Setup Guide for Inventory Reporting Pipeline

This guide explains how to set up automated weekly execution of the inventory reporting pipeline using cron.

## Overview

The pipeline runs automatically every **Tuesday at 11:00 AM IST** (5:30 AM UTC), fetching the latest inventory file from S3, generating a PDF report, and sending it via email.

## Timezone Configuration

### Important Timezone Notes

- **Server Timezone**: UTC (assumed)
- **Target Execution**: Tuesday 11:00 AM IST
- **IST = UTC + 5:30**
- **Therefore**: Tuesday 11:00 AM IST = Tuesday 5:30 AM UTC

### Cron Expression

```
30 5 * * 2
```

**Breakdown:**
- `30` - Minute (30th minute)
- `5` - Hour (5 AM UTC)
- `*` - Day of month (any)
- `*` - Month (any)
- `2` - Day of week (Tuesday, where 0=Sunday, 1=Monday, 2=Tuesday)

## Setup Instructions

### Step 1: Make Scripts Executable

```bash
chmod +x scripts/run_inventory_report.py
chmod +x scripts/run_inventory_report.sh
```

### Step 2: Update Paths in Cron Configuration

Replace `/path/to/project` with your actual project root path.

### Step 3: Choose Execution Method

#### Option A: Direct Python Execution (Recommended for system-wide Python)

```bash
# Edit crontab
crontab -e

# Add this line:
30 5 * * 2 /usr/bin/python3 /path/to/project/scripts/run_inventory_report.py >> /path/to/project/logs/cron.log 2>&1
```

**Note**: Ensure all environment variables are set in crontab or system-wide.

#### Option B: Shell Wrapper Script (Recommended for .env support)

```bash
# Edit crontab
crontab -e

# Add this line:
30 5 * * 2 /path/to/project/scripts/run_inventory_report.sh >> /path/to/project/logs/cron.log 2>&1
```

**Note**: The shell wrapper automatically loads `.env` file and activates virtual environment.

### Step 4: Set Environment Variables

Cron does **NOT** automatically load `.env` files. You must ensure environment variables are available.

#### Method 1: Set in Crontab

```bash
# Edit crontab
crontab -e

# Add environment variables at the top:
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
INVENTORY_S3_BUCKET=your-bucket-name
INVENTORY_S3_PREFIX=inventory/
SMTP_SERVER=smtp.gmail.com
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
SMTP_PORT=587

# Then add the cron job:
30 5 * * 2 /path/to/project/scripts/run_inventory_report.py >> /path/to/project/logs/cron.log 2>&1
```

#### Method 2: Use Shell Wrapper (Recommended)

The `scripts/run_inventory_report.sh` wrapper automatically loads `.env` file. Just ensure `.env` exists in project root.

#### Method 3: System-Wide Environment

Set environment variables in `/etc/environment` or system service configuration.

### Step 5: Verify Python Path

Ensure the Python path in cron matches your system:

```bash
# Check Python path
which python3

# Update cron job if needed
# Example: /usr/bin/python3 or /usr/local/bin/python3
```

### Step 6: Test Cron Job

Test the cron job manually:

```bash
# Test Python script directly
python3 scripts/run_inventory_report.py

# Or test shell wrapper
bash scripts/run_inventory_report.sh
```

## Logging

### Cron Logs

Cron execution logs (stdout/stderr from runner script):
- **Location**: `logs/cron.log`
- **Content**: Execution status, PDF path, errors

### Application Logs

Detailed pipeline execution logs:
- **Location**: `logs/report_generator.log`
- **Content**: Step-by-step execution, data processing, S3 operations

### Log Rotation

Consider setting up log rotation to prevent log files from growing too large:

```bash
# Example logrotate configuration
/path/to/project/logs/*.log {
    weekly
    rotate 4
    compress
    missingok
    notifempty
}
```

## Troubleshooting

### Cron Job Not Running

1. **Check cron service**: `systemctl status cron` (Linux) or `service cron status`
2. **Check cron logs**: `grep CRON /var/log/syslog` (Linux)
3. **Verify cron job**: `crontab -l`
4. **Check file permissions**: Scripts must be executable

### Environment Variables Not Loaded

1. **Use shell wrapper**: `scripts/run_inventory_report.sh` loads `.env` automatically
2. **Set in crontab**: Add environment variables at top of crontab
3. **Check .env file**: Ensure `.env` exists and has correct values

### Python Path Issues

1. **Use full path**: Use `/usr/bin/python3` instead of `python3`
2. **Check virtualenv**: If using virtualenv, activate it in shell wrapper
3. **Test manually**: Run script manually to verify Python path

### S3 Access Errors

1. **Check credentials**: Verify AWS credentials are set correctly
2. **Check permissions**: Ensure S3 bucket is accessible
3. **Check logs**: Review `logs/report_generator.log` for detailed errors

### Email Sending Fails

1. **Check SMTP credentials**: Verify SMTP environment variables
2. **Check dry_run_email**: Ensure `dry_run_email=False` in cron script
3. **Check logs**: Review `logs/report_generator.log` for SMTP errors

## Safety & Idempotency

- **Independent runs**: Each cron execution is independent
- **No overwrites**: PDFs are timestamped, no overwrites
- **S3 read-only**: Only read operations on S3, no mutations
- **Failure handling**: Failures are logged but don't break future runs

## Monitoring

### Check Last Execution

```bash
# Check cron log for last execution
tail -n 50 logs/cron.log

# Check application log for details
tail -n 100 logs/report_generator.log
```

### Verify PDF Generation

```bash
# List generated PDFs
ls -lh reports/inventory_report_*.pdf

# Check latest PDF
ls -t reports/inventory_report_*.pdf | head -1
```

## Example Crontab Entry

```bash
# Environment variables
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_REGION=us-east-1
INVENTORY_S3_BUCKET=my-inventory-bucket
INVENTORY_S3_PREFIX=inventory/
SMTP_SERVER=smtp.gmail.com
SMTP_USER=reports@company.com
SMTP_PASSWORD=app-specific-password
SMTP_PORT=587

# Cron job: Run every Tuesday at 11:00 AM IST (5:30 AM UTC)
30 5 * * 2 /path/to/project/scripts/run_inventory_report.sh >> /path/to/project/logs/cron.log 2>&1
```

## Notes

- **Timezone**: Ensure server timezone matches expected UTC
- **Paths**: Use absolute paths in cron (not relative)
- **Permissions**: Ensure cron user has read/write access to project directory
- **Virtualenv**: If using virtualenv, use shell wrapper script
- **Logs**: Monitor logs regularly for errors

