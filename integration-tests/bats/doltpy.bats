#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE foo (
  a BIGINT NOT NULL,
  b BIGINT,
  PRIMARY KEY (a)
);
INSERT INTO foo VALUES (0,0), (1,1);
CALL DOLT_ADD('.');
call dolt_commit('-am', 'Initialize table');
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

# NOTE: if these break on a release that's OK, just tell Max/Oscar

# already covered by other tests:
# "select * from dolt_branches"
# "SELECT * FROM `{table}` ASOF '{commit}'"

# verifying the output comparison to dolt log would be nice
@test "doltpy: log query returns expected header names" {
    run dolt sql -r csv <<SQL
SELECT
    dc.commit_hash as commit_hash,
    dca.parent_hash as parent_hash,
    committer as committer,
    email as email,
    date as date,
    message as message
FROM
    dolt_log AS dc
    LEFT OUTER JOIN dolt_commit_ancestors AS dca
        ON dc.commit_hash = dca.commit_hash
    WHERE dc.commit_hash = HASHOF('HEAD')
    ORDER BY date DESC
    LIMIT 1;
SQL
    [[ "$output" =~ "commit_hash,parent_hash,committer,email,date,message" ]] || false
}

@test "doltpy: hashof returns expected header names" {
    run dolt sql -r csv -q "select HASHOF('HEAD') as hash"
    [[ $output =~ "hash" ]]
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "doltpy: active branch query returns one row and expected header names" {
    run dolt sql -r csv -q "select * from dolt_branches where name = (select active_branch())"
    [[ $output =~ "name,hash,latest_committer,latest_committer_email,latest_commit_date,latest_commit_message" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}
