#!/usr/bin/env bats

setup() {
    load $BATS_TEST_DIRNAME/helper/common.bash
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
    dolt config --global --unset metrics.disabled
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
    rm -rf "$BATS_TMPDIR/config-$$/.dolt/eventsData/"
}

@test "test event flush locking" {
    # copy test event files to appropriate dir
    cp -a $BATS_TEST_DIRNAME/helper/testEvents/ $BATS_TMPDIR/config-$$/.dolt/eventsData/

    # kick off two child processes, one should lock the other out of the events dir
    dolt send-metrics -output >file1.txt &
    dolt send-metrics -output >file2.txt &

    # wait for processes to finish
    wait

    # get the line count of each output file
    event_count1=`wc -l file1.txt`
    event_count2=`wc -l file2.txt`
  
    if [[ "$event_count1" = *"4 file1.txt" ]] && [[ "$event_count2" = *"0 file2.txt" ]]; then
      return 0
    fi
    if [[ "$event_count1" = *"0 file1.txt" ]] && [[ "$event_count2" = *"4 file2.txt" ]]; then
      return 0
    fi

    return 1
}

# # Test that event dir locks correctly during concurrent flushes
# @test "test event flush locking" {
#     # copy test event files to appropriate dir
#     cp -a $BATS_TEST_DIRNAME/helper/testEvents/ $BATS_TMPDIR/config-$$/.dolt/eventsData/

#     # kick off two child processes, one should lock the other out of the events dir
#     dolt send-metrics -output >file1.txt &
#     pid1=$!
#     dolt send-metrics -output >file2.txt &
#     pid2=$!

#     # wait for processes to finish
#     wait $pid1 || true
#     exit_code1=$?
#     wait $pid2 || true
#     echo $exit_code1
#     exit_code2=$?
#     echo $exit_code2

#     # get the line count of each output file
#     event_count1=`wc -l file1.txt`
#     event_count2=`wc -l file2.txt`
  
#     if [ $exit_code1 -eq 0 ]; then
#       if [ $exit_code2 -eq 0 ]; then
#         # we expect for only one output file to contain 4 lines, corresponding to the 4 event files successfully processed
#         # check that the line counts of the output files match what is expected
#         if [[ "$event_count1" = *"4 file1.txt" ]] && [[ "$event_count2" = *"0 file2.txt" ]]; then
#             return 0
#         fi
#         if [[ "$event_count1" = *"0 file1.txt" ]] && [[ "$event_count2" = *"4 file2.txt" ]]; then
#             return 0
#         fi
#         return 1
#       elif [ $exit_code2 -ne 2 ]; then
#         return 1
#       fi
#     elif [ $exit_code2 -ne 0 ]; then
#         return 1
#     else
#       if [ $exit_code1 -ne 2 ]; then
#         return 1
#       fi
#     fi
#     return 1
# }
