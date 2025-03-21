#!/usr/bin/env bats
load "$BATS_TEST_DIRNAME/helper/common.bash"
load "$BATS_TEST_DIRNAME/helper/query-server-common.bash"

CONFIG_FILE_NAME=config.yaml

DATABASE_DIRS=(
    .
    mydir
    nest1/nest2/nest3
)

setup() {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
        skip "This test tests remote connections directly, SQL_ENGINE is not needed."
    fi
    setup_common
}

teardown() {
    stop_sql_server
    teardown_common
}

@test "sql-server-config-file-generation: config file is generated if one doesn't exist" {
    for data_dir in "${DATABASE_DIRS[@]}"; do
        if [[ "$data_dir" != "." ]]; then
            mkdir -p "$data_dir"
        fi

        start_sql_server_with_args --data-dir "$data_dir" --host 0.0.0.0

        [[ -f "$data_dir/$CONFIG_FILE_NAME" ]] || false

        rm "$data_dir/$CONFIG_FILE_NAME"
        stop_sql_server
    done
}

@test "sql-server-config-file-generation: config file isn't generated if one already exists" {
    for data_dir in "${DATABASE_DIRS[@]}"; do
        if [[ "$data_dir" != "." ]]; then
            mkdir -p "$data_dir"
        fi

        echo "Don't overwrite me!" >"$data_dir/$CONFIG_FILE_NAME"

        start_sql_server_with_args --data-dir "$data_dir" --host 0.0.0.0

        run cat "$data_dir/$CONFIG_FILE_NAME"
        [ $status -eq 0 ]
        [[ "$output" =~ "Don't overwrite me!" ]] || false

        rm "$data_dir/$CONFIG_FILE_NAME"
        stop_sql_server
    done
}

@test "sql-server-config-file-generation: config file isn't generated if a config is specified in args" {
    for data_dir in "${DATABASE_DIRS[@]}"; do
        if [[ "$data_dir" != "." ]]; then
            mkdir -p "$data_dir"
        fi

        NOT_CONFIG_FILE_NAME="not-$CONFIG_FILE_NAME"
        PORT=$(definePORT)

        cat >"$data_dir/$NOT_CONFIG_FILE_NAME" <<EOF
user:
  name: dolt

listener:
  host: 0.0.0.0
  port: $PORT

data_dir: $data_dir

# Don't overwrite me!
EOF

        start_sql_server_with_args_no_port --config "$data_dir/$NOT_CONFIG_FILE_NAME"

        [[ ! -f "$data_dir/$CONFIG_FILE_NAME" ]] || false

        run cat "$data_dir/$NOT_CONFIG_FILE_NAME"
        [ $status -eq 0 ]
        [[ "$output" =~ "Don't overwrite me!" ]] || false

        rm "$data_dir/$NOT_CONFIG_FILE_NAME"
        stop_sql_server
    done
}

@test "sql-server-config-file-generation: generated config file has fields set to arg-provided values" {
    start_sql_server_with_args \
        --readonly \
        --no-auto-commit \
        --max-connections 77 \
        --timeout 7777777 \
        --allow-cleartext-passwords true \
        --back-log 767 \
        --max-connections-timeout 13s \
        --host 0.0.0.0

    run cat "$CONFIG_FILE_NAME"
    [ $status -eq 0 ]
    [[ "$output" =~ "read_only: true" ]] || false
    [[ "$output" =~ "autocommit: false" ]] || false
    [[ "$output" =~ "max_connections: 77" ]] || false
    [[ "$output" =~ "read_timeout_millis: 7777777" ]] || false
    [[ "$output" =~ "write_timeout_millis: 7777777" ]] || false
    [[ "$output" =~ "allow_cleartext_passwords: true" ]] || false
    [[ "$output" =~ "back_log: 767" ]] || false

    echo "--------------------------"
    echo "$output"
    echo "--------------------------"

    [[ "$output" =~ "max_connections_timeout: 676" ]] || false
}

@test "sql-server-config-file-generation: generated config file uses default values as placeholders for unset fields" {
    start_sql_server_with_args \
        --max-connections 77 \
        --timeout 7777777 \
        --host 0.0.0.0

    run cat "$CONFIG_FILE_NAME"
    [ $status -eq 0 ]

    # not default (set by args)
    [[ "$output" =~ "max_connections: 77" ]] || false
    [[ "$output" =~ "read_timeout_millis: 7777777" ]] || false
    [[ "$output" =~ "write_timeout_millis: 7777777" ]] || false

    # default (not set by args)
    [[ "$output" =~ "# read_only: false" ]] || false
    [[ "$output" =~ "# autocommit: true" ]] || false
    [[ "$output" =~ "# allow_cleartext_passwords: false" ]] || false
}

@test "sql-server-config-file-generation: generated config file has placeholders for unset fields with no default values" {
    start_sql_server

    run cat "$CONFIG_FILE_NAME"
    [ $status -eq 0 ]
    [[ "$output" =~ "# tls_key:" ]] || false
    [[ "$output" =~ "# tls_cert:" ]] || false
}
