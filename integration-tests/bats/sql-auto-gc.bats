#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
  setup_common
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
