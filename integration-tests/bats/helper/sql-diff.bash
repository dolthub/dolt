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

# Compare CLI diff with SQL dolt_diff (both normal and skinny)
# Usage: compare_dolt_diff from_commit to_commit table_name
compare_dolt_diff() {
    local from_commit="$1"
    local to_commit="$2"
    local table_name="$3"

    # --- normal mode ---
    local cli_normal_output sql_normal_output cli_status sql_status
    cli_normal_output=$(dolt diff "$from_commit" "$to_commit" "$table_name" 2>&1); cli_status=$?
    sql_normal_output=$(dolt sql -q "SELECT * FROM dolt_diff('$from_commit', '$to_commit', '$table_name')" 2>&1); sql_status=$?

    echo "$cli_normal_output"
    echo "$sql_normal_output"

    if [ $cli_status -ne 0 ]; then echo "CLI diff failed"; echo "$cli_normal_output"; return 1; fi
    if [ $sql_status -ne 0 ]; then echo "SQL dolt_diff failed"; echo "$sql_normal_output"; return 1; fi

    local normal_cli_changes normal_sql_rows
    normal_cli_changes=$(_cli_change_count "$cli_normal_output")
    normal_sql_rows=$(_sql_row_count "$sql_normal_output")
    _compare_counts_or_err "Normal diff" "$cli_normal_output" "$sql_normal_output" "$normal_cli_changes" "$normal_sql_rows" || return 1

    local normal_cli_cols normal_sql_cols
    normal_cli_cols=$(_cli_header_cols "$cli_normal_output")
    normal_sql_cols=$(_sql_data_header_cols "$sql_normal_output")
    _compare_sets_or_err "Normal diff" "$normal_cli_cols" "$normal_sql_cols" "$cli_normal_output" "$sql_normal_output" || return 1

    # --- skinny mode ---
    local cli_skinny_output sql_skinny_output
    cli_skinny_output=$(dolt diff --skinny "$from_commit" "$to_commit" "$table_name" 2>&1); cli_status=$?
    sql_skinny_output=$(dolt sql -q "SELECT * FROM dolt_diff('--skinny', '$from_commit', '$to_commit', '$table_name')" 2>&1); sql_status=$?

    echo "$cli_skinny_output"
    echo "$sql_skinny_output"

    if [ $cli_status -ne 0 ]; then echo "CLI skinny diff failed"; echo "$cli_skinny_output"; return 1; fi
    if [ $sql_status -ne 0 ]; then echo "SQL skinny dolt_diff failed"; echo "$sql_skinny_output"; return 1; fi

    local skinny_cli_changes skinny_sql_rows
    skinny_cli_changes=$(_cli_change_count "$cli_skinny_output")
    skinny_sql_rows=$(_sql_row_count "$sql_skinny_output")
    _compare_counts_or_err "Skinny diff" "$cli_skinny_output" "$sql_skinny_output" "$skinny_cli_changes" "$skinny_sql_rows" || return 1

    local skinny_cli_cols skinny_sql_cols
    skinny_cli_cols=$(_cli_header_cols "$cli_skinny_output")
    skinny_sql_cols=$(_sql_data_header_cols "$sql_skinny_output")
    _compare_sets_or_err "Skinny diff" "$skinny_cli_cols" "$skinny_sql_cols" "$cli_skinny_output" "$sql_skinny_output" || return 1

    return 0
}