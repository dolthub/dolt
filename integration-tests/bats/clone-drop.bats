#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
  setup_no_dolt_init
}

teardown() {
  stop_sql_server 1
  assert_feature_version
  teardown_common
}

@test "clone-drop: clone a database and then drop it" {
  mkdir repo
  cd repo
  dolt init
  dolt remote add pushed 'file://../pushed'
  dolt push pushed main:main
  dolt sql -q 'call dolt_clone("file://../pushed", "cloned"); drop database cloned;'
}

@test "clone-drop: sql-server: clone a database and then drop it" {
  mkdir repo
  cd repo
  dolt init
  dolt remote add pushed 'file://../pushed'
  dolt push pushed main:main
  start_sql_server
  dolt sql -q 'call dolt_clone("file://../pushed", "cloned"); drop database cloned;'
}

@test "clone-drop: in-progress marker lifecycle on clone" {
  # See https://github.com/dolthub/dolt/issues/11206
  mkdir repo
  cd repo
  dolt init
  dolt remote add pushed 'file://../pushed'
  dolt push pushed main:main

  dolt sql -q 'call dolt_clone("file://../pushed", "cloned");'
  [ ! -f cloned/.dolt_safe_to_ignore ]

  dolt clone file://../pushed cli_cloned
  [ ! -f cli_cloned/.dolt_safe_to_ignore ]

  # A directory left behind by an interrupted clone carries the in-progress marker and squats on the
  # name. A retried clone must reclaim it -- quarantine the leftover into the dropped-database holding
  # area and proceed -- rather than wedging on it. See https://github.com/dolthub/dolt/issues/11533
  mkdir stuck
  touch stuck/.dolt_safe_to_ignore
  run dolt sql -q 'call dolt_clone("file://../pushed", "stuck");'
  [ "$status" -eq 0 ]

  # stuck is now a real clone with the in-progress marker cleared...
  [ ! -f stuck/.dolt_safe_to_ignore ]
  run dolt sql -q 'use stuck;'
  [ "$status" -eq 0 ]

  # ...and the reclaimed leftover was quarantined (recoverable via dolt_undrop), not deleted.
  [ -d .dolt_dropped_databases/stuck ]
}

# An interrupted create/clone can leave a directory that discovery ignores in three shapes: an
# in-progress marker, partial .dolt storage without a repo-state file, or a bare directory with no
# .dolt storage at all. A retried clone must reclaim any of them (quarantine to the dropped-database
# holding area) and succeed. See https://github.com/dolthub/dolt/issues/11533
@test "clone-drop: clone reclaims an incomplete leftover directory" {
  mkdir repo
  cd repo
  dolt init
  dolt remote add pushed 'file://../pushed'
  dolt push pushed main:main

  make_incomplete_dir() {
    case "$2" in
      marker)  mkdir -p "$1" && touch "$1/.dolt_safe_to_ignore" ;;
      partial) mkdir -p "$1/.dolt/noms" ;;
      bare)    mkdir -p "$1" ;;
    esac
  }

  for shape in marker partial bare; do
    echo "clone reclaim shape: $shape"
    name="reclaimed_$shape"
    make_incomplete_dir "$name" "$shape"

    run dolt sql -q "call dolt_clone('file://../pushed', '$name');"
    [ "$status" -eq 0 ]

    # the target is now a real clone with any in-progress marker cleared...
    [ ! -f "$name/.dolt_safe_to_ignore" ]
    run dolt sql -q "use $name;"
    [ "$status" -eq 0 ]

    # ...and the reclaimed leftover was quarantined (recoverable via dolt_undrop), not deleted.
    [ -d ".dolt_dropped_databases/$name" ]
  done
}
