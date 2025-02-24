
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

skip_if_no_aws_tests() {
    if [ -z "$DOLT_BATS_AWS_TABLE" ] || [ -z "$DOLT_BATS_AWS_BUCKET" ]; then
      skip "skipping aws tests; set DOLT_BATS_AWS_TABLE and DOLT_BATS_AWS_BUCKET to run"
    fi
}

# bats test_tags=no_lambda
@test "archive-aws: can backup and restore/clone archive" {
  skip_if_no_aws_tests
  rm -rf .dolt

  random_repo=`openssl rand -hex 16`

  mkdir -p original/.dolt
  cp -R $BATS_TEST_DIRNAME/archive-test-repo/* original/.dolt
  cd original

  url='aws://['"$DOLT_BATS_AWS_TABLE"':'"$DOLT_BATS_AWS_BUCKET"']/'"$random_repo"

  dolt backup add backup1 "$url"
  dolt backup sync backup1

  cd ../
  dolt backup restore "$url" restoreddb
  cd restoreddb

  # Verify we can read data
  run dolt sql -q 'select sum(i) from tbl;'
  [[ "$status" -eq 0 ]] || false
  [[ "$output" =~ "138075" ]] || false # i = 1 - 525, sum is 138075

  cd ../
  ## We can clone this thing too, even though it has archive files. Pushing archive
  ## files currently converts them to table files, so using the backup files
  ## is currently the only way we can get archive files from AWS backed databases
  ## when cloning.
  dolt clone "$url" cloneddb
  # Verify we can read data
  run dolt sql -q 'select sum(i) from tbl;'
  [[ "$status" -eq 0 ]] || false
  [[ "$output" =~ "138075" ]] || false # i = 1 - 525, sum is 138075
}

# bats test_tags=no_lambda
@test "archive-aws: can backup and restore archive with workspace changes" {
  skip_if_no_aws_tests
  rm -rf .dolt

  random_repo=`openssl rand -hex 16`

  mkdir -p original/.dolt
  cp -R $BATS_TEST_DIRNAME/archive-test-repo/* original/.dolt
  cd original

  # dirty the database. Should be visible after we restore.
  dolt sql -q "delete from tbl where i = 42;"

  url='aws://['"$DOLT_BATS_AWS_TABLE"':'"$DOLT_BATS_AWS_BUCKET"']/'"$random_repo"

  dolt backup add backup1 "$url"
  dolt backup sync backup1

  cd ../
  dolt backup restore "$url" restoreddb
  cd restoreddb

  # Verify we can read data
  run dolt sql -q 'select sum(i) from tbl;'
  [[ "$status" -eq 0 ]] || false
  [[ "$output" =~ "138033" ]] || false # i = 1 - 525, sum is 138075, then substract 42
}

# bats test_tags=no_lambda
@test "archive-aws: can push and and clone archive" {
  skip_if_no_aws_tests
  rm -rf .dolt

  random_repo=`openssl rand -hex 16`

  mkdir -p original/.dolt
  cp -R $BATS_TEST_DIRNAME/archive-test-repo/* original/.dolt
  cd original

  url='aws://['"$DOLT_BATS_AWS_TABLE"':'"$DOLT_BATS_AWS_BUCKET"']/'"$random_repo"

  dolt remote add rmt1 "$url"
  dolt push rmt1 main:main

  cd ../
  dolt clone "$url" cloneddb
  cd cloneddb

  # Verify we can read data
  run dolt sql -q 'select sum(i) from tbl;'
  [[ "$status" -eq 0 ]] || false
  [[ "$output" =~ "138075" ]] || false # i = 1 - 525, sum is 138075
}
