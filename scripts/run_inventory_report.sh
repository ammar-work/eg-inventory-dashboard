#!/bin/bash
#
# Shell wrapper script for cron-based inventory reporting
#
# This script:
# 1. Loads environment variables from .env file
# 2. Activates virtual environment (if exists)
# 3. Executes the Python cron runner script
#
# USAGE:
#   chmod +x scripts/run_inventory_report.sh
#   # In crontab:
#   30 5 * * 2 /path/to/project/scripts/run_inventory_report.sh >> /path/to/project/logs/cron.log 2>&1
#
# ENVIRONMENT:
#   - PROJECT_ROOT: Path to project root (auto-detected if not set)
#   - VENV_PATH: Path to virtual environment (auto-detected if not set)
#

set -e  # Exit on error

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"

# Change to project root
cd "$PROJECT_ROOT"

# Load environment variables from .env file (if exists)
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo "Loading environment variables from .env file..."
    set -a  # Automatically export all variables
    source "$PROJECT_ROOT/.env"
    set +a
else
    echo "WARNING: .env file not found. Ensure environment variables are set via crontab or system config."
fi

# Resolve Python interpreter explicitly
# Priority: venv Python (Windows) -> venv Python (Linux) -> system Python
VENV_PATH="${VENV_PATH:-$PROJECT_ROOT/.venv}"
PYTHON_EXEC=""

if [ -f "$VENV_PATH/Scripts/python.exe" ]; then
    # Windows virtual environment (Git Bash)
    PYTHON_EXEC="$VENV_PATH/Scripts/python.exe"
    echo "Found Python interpreter (Windows venv): $PYTHON_EXEC"
elif [ -f "$VENV_PATH/bin/python" ]; then
    # Linux/macOS virtual environment
    PYTHON_EXEC="$VENV_PATH/bin/python"
    echo "Found Python interpreter (Linux/macOS venv): $PYTHON_EXEC"
else
    # Fallback to system Python
    if command -v python3 >/dev/null 2>&1; then
        PYTHON_EXEC=$(command -v python3)
        echo "WARNING: Virtual environment Python not found. Using system Python: $PYTHON_EXEC"
    elif command -v python >/dev/null 2>&1; then
        PYTHON_EXEC=$(command -v python)
        echo "WARNING: Virtual environment Python not found. Using system Python: $PYTHON_EXEC"
    else
        echo "ERROR: No Python interpreter found. Please install Python or set up virtual environment."
        exit 1
    fi
fi

# Log which Python interpreter will be used
echo "Using Python interpreter: $PYTHON_EXEC"

# Activate virtual environment (if exists) for any environment variable setup
# Note: We're using explicit Python path above, but activation may set other env vars
if [ -f "$VENV_PATH/Scripts/activate" ]; then
    # Windows virtual environment (Git Bash)
    echo "Activating virtual environment (Windows): $VENV_PATH/Scripts/activate"
    if ! source "$VENV_PATH/Scripts/activate" 2>/dev/null; then
        echo "WARNING: Failed to activate virtual environment (non-critical)."
    fi
elif [ -f "$VENV_PATH/bin/activate" ]; then
    # Linux/macOS virtual environment
    echo "Activating virtual environment (Linux/macOS): $VENV_PATH/bin/activate"
    if ! source "$VENV_PATH/bin/activate" 2>/dev/null; then
        echo "WARNING: Failed to activate virtual environment (non-critical)."
    fi
fi

# Execute Python cron runner script using resolved interpreter
echo "Executing inventory reporting pipeline..."
"$PYTHON_EXEC" "$SCRIPT_DIR/run_inventory_report.py"

# Exit with Python script's exit code
exit $?

