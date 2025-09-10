# dolt/integration-tests/bats/helper/sql-diff.bash

: "${SQL_DIFF_DEBUG:=}" # set to any value to enable debug output
_dbg() { [ -n "$SQL_DIFF_DEBUG" ] && printf '%s\n' "$*" >&2; }
_dbg_block() { [ -n "$SQL_DIFF_DEBUG" ] && { printf '%s\n' "$1" >&2; printf '%s\n' "$2" >&2; }; }

# first table header row from CLI diff (data section), as newline list
_cli_header_cols() {
    awk '
        /^\s*\|\s*[-+<>]\s*\|/ && last_header != "" { print last_header; exit }
        /^\s*\|/ { last_header = $0 }
    ' <<<"$1" \
      | tr '|' '\n' \
      | sed -e 's/^[[:space:]]*//;s/[[:space:]]*$//' \
      | grep -v -E '^(<|>|)$' \
      | grep -v '^$'
}

# first table header row from SQL diff, strip to_/from_, drop metadata, as newline list
_sql_data_header_cols() {
    echo "$1" \
      | awk '/^\|/ {print; exit}' \
      | tr '|' '\n' \
      | sed -e 's/^[[:space:] ]*//;s/[[:space:] ]*$//' \
      | grep -E '^(to_|from_)' \
      | sed -E 's/^(to_|from_)//' \
      | grep -Ev '^(commit|commit_date|diff_type)$' \
      | grep -v '^$'
}

# count CLI changes by unique PK (includes +, -, <, >)
_cli_change_count() {
    awk -F'|' '
        # start counting once we see a data row marker
        /^\s*\|\s*[-+<>]\s*\|/ { in_table=1 }
        in_table && $2 ~ /^[[:space:]]*[-+<>][[:space:]]*$/ {
            pk=$3
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", pk)
            if (pk != "") seen[pk]=1
        }
        END { c=0; for (k in seen) c++; print c+0 }
    ' <<<"$1"
}

# count SQL data rows (lines starting with '|' minus header)
_sql_row_count() {
    echo "$1" | awk '/^\|/ {c++} END{print (c>0?c-1:0)}'
}

# compare two newline lists as sets (sorted)
_compare_sets_or_err() {
    local name="$1" cli_cols="$2" sql_cols="$3" cli_out="$4" sql_out="$5"

    local cli_sorted sql_sorted
    cli_sorted=$(echo "$cli_cols" | sort -u)
    sql_sorted=$(echo "$sql_cols" | sort -u)

    _dbg_block "$name CLI columns:" "$cli_sorted"
    _dbg_block "$name SQL data columns:" "$sql_sorted"

    if [ "$cli_sorted" != "$sql_sorted" ]; then
        echo "$name column set mismatch"
        echo "--- $name CLI columns ---"; echo "$cli_sorted"
        echo "--- $name SQL data columns ---"; echo "$sql_sorted"
        echo "--- $name CLI output ---"; echo "$cli_out"
        echo "--- $name SQL output ---"; echo "$sql_out"
        return 1
    fi
    return 0
}

# compare change/row counts; on mismatch, print both outputs
_compare_counts_or_err() {
    local name="$1" cli_out="$2" sql_out="$3" cli_count="$4" sql_count="$5"

    _dbg "$name counts: CLI=$cli_count SQL=$sql_count"

    if [ "$cli_count" != "$sql_count" ]; then
        echo "$name change count mismatch: CLI=$cli_count, SQL=$sql_count"
        echo "--- $name CLI output ---"; echo "$cli_out"
        echo "--- $name SQL output ---"; echo "$sql_out"
        return 1
    fi
    return 0
}

# ---- main entrypoint ----

# Compare CLI diff with SQL dolt_diff
# Usage: compare_dolt_diff [all dolt diff args...]
compare_dolt_diff() {
    local args=("$@")  # all arguments

    # --- normal diff ---
    local cli_output sql_output cli_status sql_status
    cli_output=$(dolt diff "${args[@]}" 2>&1)
    cli_status=$?

    # Build SQL argument list safely
    local sql_args=""
    for arg in "${args[@]}"; do
        if [ -z "$sql_args" ]; then
            sql_args="'$arg'"
        else
            sql_args+=", '$arg'"
        fi
    done
    sql_output=$(dolt sql -q "SELECT * FROM dolt_diff($sql_args)" 2>&1)
    sql_status=$?

    # normally prints in bats using `run`, so no debug blocks here
    echo "$cli_output"
    echo "$sql_output"

    if [ $cli_status -ne 0 ]; then
        _dbg "$cli_output"
        return 1
    fi
    if [ $sql_status -ne 0 ]; then
        _dbg "$sql_output"
        return 1
    fi

    # Compare counts
    local cli_changes sql_rows
    cli_changes=$(_cli_change_count "$cli_output")
    sql_rows=$(_sql_row_count "$sql_output")
    _compare_counts_or_err "Diff" "$cli_output" "$sql_output" "$cli_changes" "$sql_rows" || return 1

    # Compare columns
    local cli_cols sql_cols
    cli_cols=$(_cli_header_cols "$cli_output")
    sql_cols=$(_sql_data_header_cols "$sql_output")
    _compare_sets_or_err "Diff" "$cli_cols" "$sql_cols" "$cli_output" "$sql_output" || return 1

    return 0
}
