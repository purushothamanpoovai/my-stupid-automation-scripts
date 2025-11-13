#!/usr/bin/env bash
set -uo pipefail

# Generic MySQL/MariaDB batch executor for DML operations.
# - User defines only the SQL body (UPDATE/DELETE/INSERT SELECT ... LIMIT N).
# - Script auto-wraps with BEGIN, COMMIT, and SELECT ROW_COUNT().
# - Runs in iterations until no more rows are affected.
# - Supports optional per-iteration confirmation (--ask-confirmation).
# - Shows the full SQL block before first confirmation.
#
# Recommended SQL body:
#   UPDATE ... WHERE ... LIMIT N;
#   DELETE FROM ... WHERE ... LIMIT N;
#   INSERT INTO dst SELECT ... FROM src WHERE ... LIMIT N;
#
# Allowed INSERT usage:
#   ✔ INSERT ... SELECT ... LIMIT N        (batch-safe)
#   ✔ INSERT IGNORE ... SELECT ... LIMIT N
#   ✔ REPLACE ... SELECT ... LIMIT N
#
# Avoid (unsafe for batches):
#   ✖ INSERT VALUES (…),(…)                (loops forever; ROW_COUNT > 0 always)
#   ✖ SQL without LIMIT or changing condition
#   ✖ SQL containing BEGIN/COMMIT/ROW_COUNT (script adds these)
#
# Edit inside script:
#   - DB_NAME
#   - SLEEP_SECONDS
#   - TOTAL_COUNT_QUERY (optional)
#   - USER_SQL block
# ---



# Configurable values (edit inside script)
DB_NAME="portal"
SLEEP_SECONDS=2
ASK_CONFIRM_EACH=false   # enabled via --ask-confirmation

# Total count query (set "" to disable remaining count feature or use "SELECT N" for directly providing the count)
TOTAL_COUNT_QUERY=""  #SELECT 10000;  #"SELECT COUNT(*) FROM your_table WHERE your_condition_here;"

# SQL body (only UPDATE/DELETE/etc content)

IFS= read -r -d '' USER_SQL <<'SQLBODY'

UPDATE live_table SET processed=1 
...
WHERE ... LIMIT 1000;

SQLBODY


# ------------------------------------------------------------------------------------------------------------------

READ_STATUS=$?;
if [[ "$READ_STATUS" -ne 1 ]]; then
    echo "Error loading SQL body. Exiting."
    exit 1
fi

RED="\033[0;31m";GREEN="\033[0;32m";YELLOW="\033[1;33m";BLUE="\033[0;34m";NC="\033[0m";

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ask-confirmation) ASK_CONFIRM_EACH=true ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
    shift
done

set -e

# Build final wrapped SQL

FINAL_SQL="
BEGIN;

$USER_SQL

SELECT ROW_COUNT() AS affected;

COMMIT;
"

# Initial confirmation

echo -e "${RED}The following SQL will be executed on database: $DB_NAME${NC}"
echo "------------------------------------------------------------"
echo -e "${YELLOW} $FINAL_SQL ${NC}"
echo "------------------------------------------------------------"
echo

read -p "Are you sure you want to execute this batch operation? [y/n]: " ans
[[ "$ans" != "y" ]] && echo "Aborted." && exit 0

# Compute total count (optional)

TOTAL_PENDING_COUNT=""
if [[ -n "$TOTAL_COUNT_QUERY" ]]; then
    echo -e "${BLUE}Calculating total pending records...${NC}"
    TOTAL_PENDING_COUNT=$(mysql "$DB_NAME" -N -B -e "$TOTAL_COUNT_QUERY" 2>/dev/null || true)

    if ! [[ "$TOTAL_PENDING_COUNT" =~ ^[0-9]+$ ]]; then
        echo -e "${RED}Invalid TOTAL_COUNT_QUERY result. Fix it or disable it.${NC}"
        exit 1
    fi

    echo -e "${GREEN}Initial pending count: $TOTAL_PENDING_COUNT${NC}"
fi

# batch loop

iteration=0

while true; do
    (( ++iteration ))
    echo -e "\n${BLUE}=== Iteration #$iteration ===${NC}"

    if [ "$ASK_CONFIRM_EACH" = true ]; then
        echo
        read -p "Execute next batch now? [y/n]: " ans2
        [[ "$ans2" != "y" ]] && echo "Stopping as requested." && exit 0
    fi

    OUTPUT=$(mysql "$DB_NAME" -e "$FINAL_SQL" 2>&1)

    ROWS=$(echo "$OUTPUT" | awk '/affected/{getline; print $1}')
    ROWS="${ROWS:-0}"

    if ! [[ "$ROWS" =~ ^[0-9]+$ ]]; then
        echo -e "${RED}Error: Unable to parse affected rows.${NC}"
        echo "$OUTPUT"
        exit 1
    fi

    echo -e "${GREEN}Rows affected: $ROWS${NC}"

    if (( ROWS == 0 )); then
        echo -e "${RED}No more rows affected — stopping.${NC}"
        break
    fi

    if [[ -n "$TOTAL_COUNT_QUERY" ]]; then
        (( TOTAL_PENDING_COUNT -= ROWS ))
        (( TOTAL_PENDING_COUNT = TOTAL_PENDING_COUNT < 0 ? 0 : TOTAL_PENDING_COUNT ))
        echo -e "${YELLOW}Remaining pending: $TOTAL_PENDING_COUNT${NC}"

        if (( TOTAL_PENDING_COUNT == 0 )); then
            echo -e "${GREEN}All records processed.${NC}"
            break
        fi
    fi

    for ((i=SLEEP_SECONDS; i>0; i--)); do
        echo -ne "${BLUE}Waiting... ${i}s${NC}\r"
        sleep 1
    done
    echo -ne "\033[K"
done

echo -e "${GREEN}Batch operation completed.${NC}"
exit 0


