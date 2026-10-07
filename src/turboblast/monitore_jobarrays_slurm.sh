#!/bin/bash
# Slurm Array Summary with Memory and Start Time
# Usage: ./slurm_summary_v5.sh [JobID]

TARGET_ID=$1

# Hide cursor
echo -ne "\033[?25l"
trap 'echo -ne "\033[?25h"; exit' INTERRUPT SIGTERM EXIT

# Declared array size (batch x batch_size) reported by the scheduler.
# sacct only lists the tasks that already have an accounting record, so it
# under-counts the total; scontrol keeps the full declared size while the job
# is still scheduled. Empty for non-array jobs or once the job is purged.
array_total() {
    scontrol show job "$1" 2>/dev/null |
        grep -oE 'ArrayTaskCount=[0-9]+' | cut -d= -f2 | head -n 1
}

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

            # The total must be the number of jobs that will be submitted
            # (batch x batch_size), i.e. the declared array size, not the
            # number of sacct lines (tasks already recorded). For the state
            # counts, drop the array "parent" line (base id, no _<task>) so
            # each task is counted exactly once.
            DECLARED_TOTAL=$(array_total "$id")
            if [ -n "$DECLARED_TOTAL" ]; then
                TASK_DATA=$(echo "$RAW_DATA" | awk -v base="$id" '$1 != base {print}')
                TOTAL=$DECLARED_TOTAL
            else
                # Non-array job, or job purged from the scheduler: fall back
                # to the number of lines returned by sacct.
                TASK_DATA=$RAW_DATA
                TOTAL=$(echo "$RAW_DATA" | wc -l)
            fi

            # Count States
            R=$(echo "$TASK_DATA" | grep -c "RUNNING")
            P=$(echo "$TASK_DATA" | grep -c "PENDING")
            C=$(echo "$TASK_DATA" | grep -c "COMPLETING")
            OK=$(echo "$TASK_DATA" | grep -c "COMPLETED")
            FAIL=$(echo "$TASK_DATA" | grep -c -E "FAILED|TIMEOUT|CANCELLED|NODE_FAIL")

            # Calculations
            FINISHED=$((OK + FAIL))
            PERC=$([ "$TOTAL" -gt 0 ] && echo "$(( 100 * FINISHED / TOTAL ))" || echo "0")

            printf "%-10s | %-12s | %-8s | %-11s | %-3d | %-3d | %-3d | %-7d | %-4d | %-5d | %3d%%\n" \
                "$id" "$NAME" "$MEM" "$START_DISP" "$R" "$P" "$C" "$OK" "$FAIL" "$TOTAL" "$PERC"
        done
    fi

    echo "------------------------------------------------------------------------------------------------------------"
    sleep 2
done
