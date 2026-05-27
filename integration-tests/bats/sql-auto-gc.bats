#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
  setup_common

  # Disable the auto-GC load-average throttling. By default, auto-GC backs off
  # for up to 30 minutes when the system load average is high (see
  # loadAvgGCScheduler in go/libraries/doltcore/sqle/auto_gc.go). On a busy CI
  # host that backoff can prevent the expected GC from firing within the test,
  # making this test flaky. "NONE" selects a scheduler that always runs GC
  # immediately.
  export DOLT_GC_SCHEDULER=NONE
}

teardown() {
  assert_feature_version
  teardown_common
}

@test "sql-auto-gc: import through 'dolt sql' runs auto gc" {
  if [ "$SQL_ENGINE" = "remote-engine" ]; then
   skip "dolt sql does not control auto GC behavior when in remote mode"
  fi

  before=$(du -sk .dolt | awk '{print $1}')
  awk -f $BATS_TEST_DIRNAME/gen_vals.awk | dolt sql
  after=$(du -sk .dolt | awk '{print $1}')
  [[ after-before -lt 524288 ]] || (echo ".dolt should be less than 512MB after the import" && false)
  tablefiles=$(ls -1 .dolt/noms/ | egrep '[0-9a-v]{32}' | egrep -v 'v{32}')
  [[ $(echo "$tablefiles" | wc -l) -eq 1 ]] || (echo ".dolt/noms should have one table file" && false)
}

@test "sql-auto-gc: import through 'dolt sql' can disable auto gc" {
  if [ "$SQL_ENGINE" = "remote-engine" ]; then
   skip "dolt sql does not control auto GC behavior when in remote mode"
  fi

  before=$(du -sk .dolt | awk '{print $1}')
  awk -f $BATS_TEST_DIRNAME/gen_vals.awk | dolt sql --disable-auto-gc
  after=$(du -sk .dolt | awk '{print $1}')
  [[ after-before -gt 524288 ]] || (echo ".dolt should be more than 512MB after the import" && false)
  if ls -1 .dolt/noms/ | egrep '[0-9a-v]{32}' | egrep -v 'v{32}'; then
    echo ".dolt/noms should have two non-journal table files";
    false
  fi
}
