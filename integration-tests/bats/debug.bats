#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    skiponwindows "tests are flaky on Windows"
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test tests remote connections directly, SQL_ENGINE is not needed."
    fi

    setup_common

    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/repo1

    cd $TMPDIRS/repo1
    dolt init

    dolt sql <<SQL
create table ab (a int primary key, b varchar(100), key (b,a));
insert into ab
select * from (
  with recursive cte(a,b) as (
    select 0,'text_val'
    union
    select a+1, 'text_val' from cte where a < 1000
  )
  select * from cte
) dt;
SQL

}

teardown() {
    teardown_common
    stop_sql_server 1
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}


@test "debug produces expected files" {
    run dolt debug -t 1 -o out -q "select count(*) from ab where b = 'text_val'"
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "cpu" ]] || false
    [[ "${lines[1]}" =~ "mem" ]] || false
    [[ "${lines[2]}" =~ "trace" ]] || false

    ls
    tar -xvzf out.tar.gz

    run ls out
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "analysis.txt" ]] || false
    [[ "${lines[1]}" =~ "cpu.pprof" ]] || false
    [[ "${lines[2]}" =~ "exec.txt" ]] || false
    [[ "${lines[3]}" =~ "input.sql" ]] || false
    [[ "${lines[4]}" =~ "mem.pprof" ]] || false
    [[ "${lines[5]}" =~ "plan.txt" ]] || false
    [[ "${lines[6]}" =~ "trace.out" ]] || false

    run cat out/plan.txt
    [ "$status" -eq 0 ]
    [[ "$output" =~ "IndexedTableAccess" ]] || false
}
