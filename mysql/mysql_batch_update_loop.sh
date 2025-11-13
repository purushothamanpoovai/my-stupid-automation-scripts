#!/usr/bin/env bash
set -euo pipefail

# This script executes a multi-line MySQL UPDATE query in batches inside a loop.
# It uses SELECT ROW_COUNT() to detect how many rows were updated in each iteration.
# The script stops automatically when no more rows remain to update.
# A countdown delay is added between iterations for controlled processing.
# All SQL is kept literal using a safe here-doc block with proper Bash handling.

set +e
# === CONFIGURATION ===
DB_NAME="your database "

read -r -d '' QUERY <<'SQLQUERY'
BEGIN;

UPDATE ...  WHERE .... LIMIT <LIMIT> ;

SELECT ROW_COUNT() AS affected;

COMMIT;

SQLQUERY

set -e

TOTAL_COUNT_QUERY="SELECT COUNT(*) FROM  ... "

echo "$QUERY"
read -p "Are you sure to execute this query on $DB_NAME ? [y/n]: "; [[ "$REPLY" != "y" ]] && exit 1

echo "Calculating Total count $TOTAL_COUNT_QUERY"
echo

TOTAL_PENDING_COUNT=$(mysql $DB_NAME -e "$TOTAL_COUNT_QUERY")  || exit 1

[[ -z "TOTAL_PENDING_COUNT" ]] && [[ "$TOTAL_PENDING_COUNT" -le 0 ]] && echo "No records found." && exit 1

LOOP=0
while true; do
    ((++LOOP))

    echo -e "\n\033[0;34m=== Iteration #$LOOP ===\033[0m"
    echo -e "\033[1;33mExecuting query...\033[0m"
    echo "------------------------------"

    # Run SQL and capture all output
    OUTPUT=$(mysql "$DB_NAME" -e "$QUERY" 2>&1)

    # Extract affected row count from SELECT ROW_COUNT()
    ROWS=$(echo "$OUTPUT" | awk '/affected/{getline; print $1}')
    ROWS=${ROWS:-0}

    # Safety: ensure numeric
    if ! [[ "$ROWS" =~ ^[0-9]+$ ]]; then
        echo -e "\033[0;31mInvalid affected-row value detected. Exiting.\033[0m"
        exit 1
    fi

    # Show affected rows
    echo -e "\033[0;32mRows affected: $ROWS\033[0m"

    # Exit if nothing was updated
    if (( ROWS == 0 )); then
        echo -e "\033[0;31mNo records affected — exiting loop.\033[0m"
        break
    fi

    # Update remaining count
    ((TOTAL_PENDING_COUNT -= ROWS))
    ((TOTAL_PENDING_COUNT = TOTAL_PENDING_COUNT < 0 ? 0 : TOTAL_PENDING_COUNT))

    echo -e "\033[1;33mRemaining pending: $TOTAL_PENDING_COUNT\033[0m"

    # Stop if everything is done
    if (( TOTAL_PENDING_COUNT == 0 )); then
        echo -e "\033[0;32mAll records updated successfully.\033[0m"
        break
    fi

    # Wait 2 seconds with countdown
    for i in {2..1}; do
        echo -ne "\033[0;34mWaiting... ${i}s\033[0m\r"
        sleep 1
    done
    echo -ne "\033[K"  # clear line
done
