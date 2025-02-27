#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt config --global --unset metrics.disabled
}

teardown() {
    teardown_common
    rm -rf "$BATS_TMPDIR/config-$$/.dolt/eventsData/"
}

# Test that event dir locks correctly during concurrent flushes
@test "send-metrics: test event flush locking" {
    # copy test event files to appropriate dir
    cp -a $BATS_TEST_DIRNAME/helper/testEvents/* $BATS_TMPDIR/config-$$/.dolt/eventsData/

    # kick off two child processes, one should lock the other out of the events dir
    dolt send-metrics --output-format stdout >file1.txt &
    pid1=$!
    dolt send-metrics --output-format stdout >file2.txt &
    pid2=$!

    # wait for processes to finish
    wait $pid1 || true
    exit_code1=$?
    wait $pid2 || true
    echo $exit_code1
    exit_code2=$?
    echo $exit_code2

    # get the line count of each output file
    event_count1=`wc -l file1.txt`
    event_count2=`wc -l file2.txt`

    if [ $exit_code1 -eq 0 ]; then
      if [ $exit_code2 -eq 0 ]; then
        # we expect for only one output file to contain 4 lines, corresponding to the 4 event files successfully processed
        # check that the line counts of the output files match what is expected
        if [[ "$event_count1" = *"4 file1.txt" ]] && [[ "$event_count2" = *"0 file2.txt" ]]; then
            echo success
            return 0
        fi
        if [[ "$event_count1" = *"0 file1.txt" ]] && [[ "$event_count2" = *"4 file2.txt" ]]; then
            echo success
            return 0
        fi
        echo "evc1 -- > $event_count1"
        echo "evc2 -- >$event_count2"
        echo miss success block
        return 1
      elif [ $exit_code2 -ne 2 ]; then
        echo exit code 2 not equal 2
        return 1
      fi
    elif [ $exit_code2 -ne 0 ]; then
        echo exit code 2 not equal 0
        return 1
    else
      if [ $exit_code1 -ne 2 ]; then
        echo exit code 1 not equal 2
        return 1
      fi
    fi
    echo this block should not fire
    return 1
}

@test "send-metrics: test event logging" {
    DOLT_DISABLE_EVENT_FLUSH=true dolt sql -q "create table t1 (a int primary key, b int)"
    DOLT_DISABLE_EVENT_FLUSH=true dolt sql -q "insert into t1 values (1, 2)"
    DOLT_DISABLE_EVENT_FLUSH=true dolt ls
    DOLT_DISABLE_EVENT_FLUSH=true dolt status

    # output all the metrics data to stdout for examination
    run dolt send-metrics --output-format stdout
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    
    # output is random-ordered, so we have to search line in it
    sql_count=$(echo "$output" | grep -o "type:SQL" | wc -l)
    ls_count=$(echo "$output" | grep -o "type:LS" | wc -l)
    status_count=$(echo "$output" | grep -o "type:STATUS" | wc -l)
    [ "$sql_count" -eq 2 ]
    [ "$ls_count" -eq 1 ]
    [ "$status_count" -eq 1 ]
    
    # send metrics should be empty after this, since it deletes all old metrics
    run dolt send-metrics --output-format stdout
    [ "$status" -eq 0 ]
    [ "$output" == "" ]
}

# TODO: we need a local metrics server here that we can spin up to verify the send actually works
# end-to-end
@test "send-metrics: grpc smoke test" {
    DOLT_DISABLE_EVENT_FLUSH=true dolt sql -q "create table t1 (a int primary key, b int)"
    DOLT_DISABLE_EVENT_FLUSH=true dolt sql -q "insert into t1 values (1, 2)"
    DOLT_DISABLE_EVENT_FLUSH=true dolt ls
    DOLT_DISABLE_EVENT_FLUSH=true dolt status

    # output all the metrics data to stdout for examination
    dolt config --global --add metrics.host "fake.server"
    run dolt send-metrics
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error flushing events" ]] || false
    [[ "$output" =~ "fake.server" ]] || false
}

@test "send-metrics: sql-server heartbeat" {
    # output all the metrics data to stdout for examination
    DOLT_EVENTS_EMITTER=logger DOLT_SQL_SERVER_HEARTBEAT_INTERVAL=1s dolt sql-server -l debug > heartbeats.out 2>&1 &
    server_pid=$!
    sleep 5
    kill $server_pid

    cat heartbeats.out

    wc=`grep SQL_SERVER_HEARTBEAT heartbeats.out | wc -l`
    [ $wc -gt 0 ]

    # make sure we don't emit until the timer goes off
    DOLT_EVENTS_EMITTER=logger DOLT_SQL_SERVER_HEARTBEAT_INTERVAL=10s dolt sql-server -l debug > heartbeats.out 2>&1 &
    server_pid=$!
    sleep 5
    kill $server_pid

    cat heartbeats.out

    wc=`grep SQL_SERVER_HEARTBEAT heartbeats.out | wc -l`
    [ $wc -eq 0 ]
}

# TODO: we need a local metrics server here that we can spin up to verify the send actually works
# end-to-end
@test "send-metrics: sql-server grpc heartbeat smoketest" {
    dolt config --global --add metrics.host "fake.server"
    DOLT_SQL_SERVER_HEARTBEAT_INTERVAL=1s dolt sql-server -l debug > heartbeats.out 2>&1 &
    server_pid=$!
    sleep 5
    kill $server_pid

    wc=`grep 'failed to send heartbeat event' heartbeats.out | wc -l`
    [ $wc -gt 0 ]

    wc=`grep 'fake.server' heartbeats.out | wc -l`
    [ $wc -gt 0 ]
}
