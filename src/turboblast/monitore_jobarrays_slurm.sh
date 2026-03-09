#!/bin/bash
# Slurm Array Summary with Memory and Start Time
# Usage: ./slurm_summary_v5.sh [JobID]

TARGET_ID=$1

# Hide cursor
echo -ne "\033[?25l"
trap 'echo -ne "\033[?25h"; exit' INTERRUPT SIGTERM EXIT

while true; do
    printf "\033[H\033[J"
    if [ -n "$TARGET_ID" ]; then
        echo "SLURM REPORT FOR JOB: $TARGET_ID [$(date +%H:%M:%S)]"
    else
        echo "SLURM ACTIVE ARRAYS SUMMARY [$(date +%H:%M:%S)]"
    fi
    echo "------------------------------------------------------------------------------------------------------------"
    # Column Header
    printf "%-10s | %-12s | %-8s | %-11s | %-3s | %-3s | %-3s | %-7s | %-4s | %-5s | %-4s\n" \
           "ARRAY_ID" "NAME" "MEM" "STARTED" "RUN" "PEN" "CG" "SUCCESS" "FAIL" "TOTAL" "DONE%"
    echo "-----------|--------------|----------|-------------|-----|-----|-----|---------|-------|-------|-------"

    # 1. Determine IDs
    if [ -n "$TARGET_ID" ]; then
        IDS=$TARGET_ID
    else
        IDS=$(squeue --me -h -o "%F" | sort -u)
    fi

    if [ -z "$IDS" ]; then
        if [ -n "$TARGET_ID" ]; then echo "Job $TARGET_ID not found."; else echo "No active jobs."; fi
    else
        # 2. Get Data for this ID using sacct
        for id in $IDS; do
            # We fetch ReqMem (Memory booked) and Start (Start time of the first task)
            # format=JobIDRaw,State,JobName,ReqMem,Start
            RAW_DATA=$(sacct -j "$id" -X -n --format=JobIDRaw,State,JobName,ReqMem,Start)

            if [ -z "$RAW_DATA" ]; then
                printf "%-10s | %-12s | %-8s | %-11s | %-3s | %-3s | %-3s | %-7s | %-4s | %-5s | %-4s\n" \
                       "$id" "NOT_FOUND" "-" "-" "0" "0" "0" "0" "0" "0" "0%"
                continue
            fi

            # Extract basic info from the first line
            FIRST_LINE=$(echo "$RAW_DATA" | head -n 1)
            NAME=$(echo "$FIRST_LINE" | awk '{print $3}' | cut -c1-12)
            MEM=$(echo "$FIRST_LINE" | awk '{print $4}')

            # Format Start Time to be shorter (HH:MM:SS or Month-Day HH:MM)
            # Slurm usually returns YYYY-MM-DDTHH:MM:SS
            START_RAW=$(echo "$FIRST_LINE" | awk '{print $5}')
            if [[ "$START_RAW" == "Unknown" || "$START_RAW" == "None" ]]; then
                START_DISP="Pending"
            else
                # Extracting just the date/time (Month-Day HH:MM)
                START_DISP=$(echo "$START_RAW" | cut -c 6-16 | sed 's/T/ /')
            fi

            # Count States
            R=$(echo "$RAW_DATA" | grep -c "RUNNING")
            P=$(echo "$RAW_DATA" | grep -c "PENDING")
            C=$(echo "$RAW_DATA" | grep -c "COMPLETING")
            OK=$(echo "$RAW_DATA" | grep -c "COMPLETED")
            FAIL=$(echo "$RAW_DATA" | grep -c -E "FAILED|TIMEOUT|CANCELLED|NODE_FAIL")

            # Calculations
            TOTAL=$(echo "$RAW_DATA" | wc -l)
            FINISHED=$((OK + FAIL))
            PERC=$([ "$TOTAL" -gt 0 ] && echo "$(( 100 * FINISHED / TOTAL ))" || echo "0")

            printf "%-10s | %-12s | %-8s | %-11s | %-3d | %-3d | %-3d | %-7d | %-4d | %-5d | %3d%%\n" \
                "$id" "$NAME" "$MEM" "$START_DISP" "$R" "$P" "$C" "$OK" "$FAIL" "$TOTAL" "$PERC"
        done
    fi

    echo "------------------------------------------------------------------------------------------------------------"
    sleep 2
done
