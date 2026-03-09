#!/bin/bash

LOG_DIR="$1"

if [ -z "$LOG_DIR" ]; then
    echo "Usage: ./submitit_analyze.sh <path_to_submitit_logs>"
    exit 1
fi

if [ ! -d "$LOG_DIR" ]; then
    echo "Error: Directory $LOG_DIR does not exist."
    exit 1
fi

# Function to calculate percentage
get_perc() {
    local count="$1"
    local total="$2"
    if [ "$total" -eq 0 ]; then
        echo "0.0"
    else
        # SC2027/SC2086 fix: Using -v to pass bash variables safely into awk
        awk -v c="$count" -v t="$total" 'BEGIN { printf "%.1f", (c / t) * 100 }'
    fi
}

echo "----------------------------------------------------------------"
echo " ANALYZING SUBMITIT LOGS IN: $LOG_DIR"
echo "----------------------------------------------------------------"

# Count total unique tasks (based on .out files)
# SC2012 fix: Use 'find' instead of 'ls' to safely count files
TOTAL_LOGS=$(find "$LOG_DIR" -maxdepth 1 -name "*.out" 2>/dev/null | wc -l)

if [ "$TOTAL_LOGS" -eq 0 ]; then
    echo "No log files found in $LOG_DIR"
    exit 0
fi

# 1. Success Count
SUCCESS=$(grep -l -iE "completed successfully|success" "$LOG_DIR"/*.out 2>/dev/null | wc -l)
# SC2086 fixes below: Double quote all variables
SUCCESS_P=$(get_perc "$SUCCESS" "$TOTAL_LOGS")

# 2. Permission Issues
PERM_ERR=$(grep -l -iE "Permission denied|AccessDenied|EACCES" "$LOG_DIR"/*.{out,err} 2>/dev/null | sort -u | wc -l)
PERM_P=$(get_perc "$PERM_ERR" "$TOTAL_LOGS")

# 3. Memory / Killed Issues
KILLED_ERR=$(grep -l -iE "Out of memory|Killed|OOM killer|slurm_step_terminate" "$LOG_DIR"/*.{out,err} 2>/dev/null | sort -u | wc -l)
KILLED_P=$(get_perc "$KILLED_ERR" "$TOTAL_LOGS")

# 4. General Python Tracebacks
PYTHON_ERR=$(grep -l "Traceback (most recent call last):" "$LOG_DIR"/*.err 2>/dev/null | sort -u | wc -l)
PYTHON_P=$(get_perc "$PYTHON_ERR" "$TOTAL_LOGS")

# 5. Socket / Slurm Communication Errors
SOCKET_ERR=$(grep -l -iE "socket|missing socket|confirm allocation" "$LOG_DIR"/*.{out,err} 2>/dev/null | sort -u | wc -l)
SOCKET_P=$(get_perc "$SOCKET_ERR" "$TOTAL_LOGS")

# 6. Apptainer specific errors
CONTAINER_ERR=$(grep -l -iE "apptainer: error|FATAL:|image not found" "$LOG_DIR"/*.{out,err} 2>/dev/null | sort -u | wc -l)
CONTAINER_P=$(get_perc "$CONTAINER_ERR" "$TOTAL_LOGS")

# Calculate General Failures
FAIL_TOTAL=$((TOTAL_LOGS - SUCCESS))
FAIL_P=$(get_perc "$FAIL_TOTAL" "$TOTAL_LOGS")

echo "Total Tasks Scanned: $TOTAL_LOGS"
echo "----------------------------------------------------------------"
printf "%-25s : %5d (%6s%%)\n" "SUCCESSFUL TASKS" "$SUCCESS" "$SUCCESS_P"
printf "%-25s : %5d (%6s%%)\n" "TOTAL FAILURES"   "$FAIL_TOTAL" "$FAIL_P"
echo "----------------------------------------------------------------"
echo "BREAKDOWN OF FAILURES (Percentage of Total Tasks):"
printf "  %-23s : %5d (%6s%%)\n" "Permission Issues"   "$PERM_ERR" "$PERM_P"
printf "  %-23s : %5d (%6s%%)\n" "Killed / Out of Mem" "$KILLED_ERR" "$KILLED_P"
printf "  %-23s : %5d (%6s%%)\n" "Python Exceptions"   "$PYTHON_ERR" "$PYTHON_P"
printf "  %-23s : %5d (%6s%%)\n" "Socket/Slurm Errors" "$SOCKET_ERR" "$SOCKET_P"
printf "  %-23s : %5d (%6s%%)\n" "Apptainer Errors"    "$CONTAINER_ERR" "$CONTAINER_P"
echo "----------------------------------------------------------------"

if [ "$FAIL_TOTAL" -gt 0 ]; then
    echo "HINT: To see a list of failing log files:"
    echo "grep -L \"success\" $LOG_DIR/*.out | head -n 5"
fi
