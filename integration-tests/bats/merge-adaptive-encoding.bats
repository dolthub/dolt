#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

# These tests exercise merging branches where a long-value column (TEXT / JSON / BLOB ...)
# has changed between "adaptive" and "non-adaptive" encoding on one side of the merge.
#
# Dolt encodes long-value columns one of two ways:
#   - adaptive encoding (the default): small values are stored inline in the row, large
#     values are stored out-of-band by address.
#   - non-adaptive ("addr") encoding: the value is always stored out-of-band by address.
#
# The default is adaptive. To create a column with non-adaptive encoding we set the
# DOLT_USE_ADAPTIVE_ENCODING=false environment variable for the process that creates the
# column. That variable is read once at process startup, so we control it per `dolt`
# invocation (each CLI call is its own process).
#
# To "convert" a table's encoding on one branch while keeping the *same logical columns*
# (so the merge lines the two sides up by tag, not as unrelated columns) we recreate the
# table from its original schema under the desired encoding setting:
#   1. drop the table,
#   2. re-create it from the identical CREATE TABLE statement, with the encoding env var
#      set as desired (preserves column order and any check constraints),
#   3. copy the committed rows back with `insert into t select * from t as of 'HEAD'`.
# Dolt generates column tags deterministically from the schema (a documented invariant),
# so re-creating an identical schema reproduces the original tags automatically -- the two
# branches still line up column-for-column by tag. We deliberately do NOT call
# dolt_update_column_tag to "fix up" tags afterward: that procedure rebuilds the schema as
# a side effect, which changes how the merge classifies the two schemas and ends up masking
# the very encoding-mismatch behavior these tests are meant to exercise.

setup() {
    setup_common
    # Default every dolt invocation in these tests to NON-adaptive encoding, so that any
    # table/column we create starts out non-adaptive. recreate_table_with_encoding flips
    # the encoding to adaptive with a per-command override.
    export DOLT_USE_ADAPTIVE_ENCODING=false
}

teardown() {
    assert_feature_version
    teardown_common
}

# recreate_table_with_encoding <table> <true|false> <full-create-table-ddl>
# Recreates <table> under the given adaptive-encoding setting, preserving the original
# column order, check constraints, tags, and data. Leaves the result in the working set
# (does not commit). The table's current rows must be present at HEAD.
recreate_table_with_encoding() {
    local tbl=$1 adaptive=$2 ddl=$3
    dolt sql -q "drop table $tbl"
    # The re-created table picks up adaptive (or non-adaptive) encoding because this single
    # process overrides the test-wide DOLT_USE_ADAPTIVE_ENCODING default.
    DOLT_USE_ADAPTIVE_ENCODING=$adaptive dolt sql -q "$ddl"
    dolt sql -q "insert into $tbl select * from $tbl as of 'HEAD'"
}

# assert_col_tag <table> <column> <expected-tag>
assert_col_tag() {
    local tbl=$1 col=$2 want=$3 got
    got=$(dolt schema tags -r csv | awk -F, -v t="$tbl" -v c="$col" '$1==t && $2==c {print $3}')
    [ "$got" = "$want" ] || ( echo "tag mismatch for $tbl.$col: want $want got $got" && return 1 )
}

# ---------------------------------------------------------------------------
# Functional matrix: small tables, TEXT and JSON, case 1 (dest converts) and
# case 2 (source converts), with and without a check constraint.
#
# Layout (independent changes on each side):
#   base:  pk 1,2,4 present
#   source 'other': updates val of pk=1, inserts pk=3
#   dest   'main':  updates val of pk=2, updates the long-value column of pk=4
# Exactly one side additionally converts the long column's encoding.
# ---------------------------------------------------------------------------

@test "merge-adaptive-encoding: TEXT, dest converts to adaptive (case 1)" {
    ddl="create table t (pk int primary key, c text, val int)"
    dolt sql -q "$ddl"
    dolt sql -q "insert into t (pk,c,val) values (1,'aaa',10),(2,'bbb',20),(4,'ddd',40)"
    dolt add . && dolt commit -m base
    base_tag=$(dolt schema tags -r csv | awk -F, '$1=="t" && $2=="c"{print $3}')

    # source: no conversion, independent changes
    dolt checkout -b other
    dolt sql -q "update t set val = 11 where pk = 1"
    dolt sql -q "insert into t (pk,c,val) values (3,'ccc',30)"
    dolt commit -am "source changes"

    # dest: convert to adaptive, independent changes
    dolt checkout main
    recreate_table_with_encoding t true "$ddl"
    dolt sql -q "update t set val = 21 where pk = 2"
    dolt sql -q "update t set c = 'DDD' where pk = 4"
    dolt commit -am "dest converts + changes"
    assert_col_tag t c "$base_tag"

    run dolt merge other -m merge
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "conflict" ]] || false

    run dolt sql -r csv -q "select pk, c, val from t order by pk"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,aaa,11" ]] || false
    [[ "$output" =~ "2,bbb,21" ]] || false
    [[ "$output" =~ "3,ccc,30" ]] || false
    [[ "$output" =~ "4,DDD,40" ]] || false
}

@test "merge-adaptive-encoding: TEXT, source converts to adaptive (case 2)" {
    ddl="create table t (pk int primary key, c text, val int)"
    dolt sql -q "$ddl"
    dolt sql -q "insert into t (pk,c,val) values (1,'aaa',10),(2,'bbb',20),(4,'ddd',40)"
    dolt add . && dolt commit -m base
    base_tag=$(dolt schema tags -r csv | awk -F, '$1=="t" && $2=="c"{print $3}')

    # source: convert to adaptive, independent changes
    dolt checkout -b other
    recreate_table_with_encoding t true "$ddl"
    dolt sql -q "update t set val = 11 where pk = 1"
    dolt sql -q "insert into t (pk,c,val) values (3,'ccc',30)"
    dolt commit -am "source converts + changes"
    assert_col_tag t c "$base_tag"

    # dest: no conversion, independent changes
    dolt checkout main
    dolt sql -q "update t set val = 21 where pk = 2"
    dolt sql -q "update t set c = 'DDD' where pk = 4"
    dolt commit -am "dest changes"

    run dolt merge other -m merge
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "conflict" ]] || false

    run dolt sql -r csv -q "select pk, c, val from t order by pk"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,aaa,11" ]] || false
    [[ "$output" =~ "2,bbb,21" ]] || false
    [[ "$output" =~ "3,ccc,30" ]] || false
    [[ "$output" =~ "4,DDD,40" ]] || false
}

@test "merge-adaptive-encoding: JSON, dest converts to adaptive (case 1)" {
    ddl="create table t (pk int primary key, c json, val int)"
    dolt sql -q "$ddl"
    dolt sql -q "insert into t (pk,c,val) values (1,JSON_OBJECT('v',1),10),(2,JSON_OBJECT('v',2),20),(4,JSON_OBJECT('v',4),40)"
    dolt add . && dolt commit -m base
    base_tag=$(dolt schema tags -r csv | awk -F, '$1=="t" && $2=="c"{print $3}')

    dolt checkout -b other
    dolt sql -q "update t set val = 11 where pk = 1"
    dolt sql -q "insert into t (pk,c,val) values (3,JSON_OBJECT('v',3),30)"
    dolt commit -am "source changes"

    dolt checkout main
    recreate_table_with_encoding t true "$ddl"
    dolt sql -q "update t set val = 21 where pk = 2"
    dolt sql -q "update t set c = JSON_OBJECT('v',44) where pk = 4"
    dolt commit -am "dest converts + changes"
    assert_col_tag t c "$base_tag"

    run dolt merge other -m merge
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "conflict" ]] || false

    run dolt sql -r csv -q "select pk, json_extract(c,'\$.v') v, val from t order by pk"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1,11" ]] || false
    [[ "$output" =~ "2,2,21" ]] || false
    [[ "$output" =~ "3,3,30" ]] || false
    [[ "$output" =~ "4,44,40" ]] || false
}

@test "merge-adaptive-encoding: JSON, source converts to adaptive (case 2)" {
    ddl="create table t (pk int primary key, c json, val int)"
    dolt sql -q "$ddl"
    dolt sql -q "insert into t (pk,c,val) values (1,JSON_OBJECT('v',1),10),(2,JSON_OBJECT('v',2),20),(4,JSON_OBJECT('v',4),40)"
    dolt add . && dolt commit -m base
    base_tag=$(dolt schema tags -r csv | awk -F, '$1=="t" && $2=="c"{print $3}')

    dolt checkout -b other
    recreate_table_with_encoding t true "$ddl"
    dolt sql -q "update t set val = 11 where pk = 1"
    dolt sql -q "insert into t (pk,c,val) values (3,JSON_OBJECT('v',3),30)"
    dolt commit -am "source converts + changes"
    assert_col_tag t c "$base_tag"

    dolt checkout main
    dolt sql -q "update t set val = 21 where pk = 2"
    dolt sql -q "update t set c = JSON_OBJECT('v',44) where pk = 4"
    dolt commit -am "dest changes"

    run dolt merge other -m merge
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "conflict" ]] || false

    run dolt sql -r csv -q "select pk, json_extract(c,'\$.v') v, val from t order by pk"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1,11" ]] || false
    [[ "$output" =~ "2,2,21" ]] || false
    [[ "$output" =~ "3,3,30" ]] || false
    [[ "$output" =~ "4,44,40" ]] || false
}

# KNOWN FIX TARGET (expected to FAIL until the bug is fixed):
# When the merge DESTINATION holds the column adaptively encoded and the SOURCE holds it
# non-adaptively, a row ADDED on the source side is validated against the check constraint in
# the destination's (adaptive) context: the merge builds the new row and reads its long
# column -- whose bytes are the source's non-adaptive encoding -- with the adaptive value
# descriptor. On main this panics the process ("invalid hash length: 19"); on this branch the
# panic is recovered into an error, but the merge still does not succeed. (The trigger is the
# inserted pk=3 row; an update-only merge of the same mismatch does not hit it.) These tests
# assert the *desired* behavior (a successful, correct, constraint-validated merge), so they
# are red until that is implemented.
@test "merge-adaptive-encoding: TEXT with check constraint, dest converts (case 1)" {
    ddl="create table t (pk int primary key, c text, val int, check (val >= 0))"
    dolt sql -q "$ddl"
    dolt sql -q "insert into t (pk,c,val) values (1,'aaa',10),(2,'bbb',20),(4,'ddd',40)"
    dolt add . && dolt commit -m base
    base_tag=$(dolt schema tags -r csv | awk -F, '$1=="t" && $2=="c"{print $3}')

    dolt checkout -b other
    dolt sql -q "update t set val = 11 where pk = 1"
    dolt sql -q "insert into t (pk,c,val) values (3,'ccc',30)"
    dolt commit -am "source changes"

    dolt checkout main
    recreate_table_with_encoding t true "$ddl"
    dolt sql -q "update t set val = 21 where pk = 2"
    dolt sql -q "update t set c = 'DDD' where pk = 4"
    dolt commit -am "dest converts + changes"
    assert_col_tag t c "$base_tag"

    run dolt merge other -m merge
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "conflict" ]] || false

    run dolt sql -r csv -q "select pk, c, val from t order by pk"
    [[ "$output" =~ "1,aaa,11" ]] || false
    [[ "$output" =~ "2,bbb,21" ]] || false
    [[ "$output" =~ "3,ccc,30" ]] || false
    [[ "$output" =~ "4,DDD,40" ]] || false

    # the check constraint survives the merge and is not violated
    run dolt sql -q "show create table t"
    [[ "$output" =~ "CHECK" ]] || false
    run dolt sql -r csv -q "select count(*) as n from dolt_constraint_violations"
    [ "${lines[1]}" = "0" ]
}

@test "merge-adaptive-encoding: TEXT with check constraint, source converts (case 2)" {
    ddl="create table t (pk int primary key, c text, val int, check (val >= 0))"
    dolt sql -q "$ddl"
    dolt sql -q "insert into t (pk,c,val) values (1,'aaa',10),(2,'bbb',20),(4,'ddd',40)"
    dolt add . && dolt commit -m base
    base_tag=$(dolt schema tags -r csv | awk -F, '$1=="t" && $2=="c"{print $3}')

    dolt checkout -b other
    recreate_table_with_encoding t true "$ddl"
    dolt sql -q "update t set val = 11 where pk = 1"
    dolt sql -q "insert into t (pk,c,val) values (3,'ccc',30)"
    dolt commit -am "source converts + changes"
    assert_col_tag t c "$base_tag"

    dolt checkout main
    dolt sql -q "update t set val = 21 where pk = 2"
    dolt sql -q "update t set c = 'DDD' where pk = 4"
    dolt commit -am "dest changes"

    run dolt merge other -m merge
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "conflict" ]] || false

    run dolt sql -r csv -q "select pk, c, val from t order by pk"
    [[ "$output" =~ "1,aaa,11" ]] || false
    [[ "$output" =~ "2,bbb,21" ]] || false
    [[ "$output" =~ "3,ccc,30" ]] || false
    [[ "$output" =~ "4,DDD,40" ]] || false

    run dolt sql -q "show create table t"
    [[ "$output" =~ "CHECK" ]] || false
    run dolt sql -r csv -q "select count(*) as n from dolt_constraint_violations"
    [ "${lines[1]}" = "0" ]
}

# KNOWN FIX TARGET (expected to FAIL until the bug is fixed): see the note on the TEXT
# "dest converts (case 1)" check-constraint test above. Same root cause for JSON.
@test "merge-adaptive-encoding: JSON with check constraint, dest converts (case 1)" {
    ddl="create table t (pk int primary key, c json, val int, check (val >= 0))"
    dolt sql -q "$ddl"
    dolt sql -q "insert into t (pk,c,val) values (1,JSON_OBJECT('v',1),10),(2,JSON_OBJECT('v',2),20),(4,JSON_OBJECT('v',4),40)"
    dolt add . && dolt commit -m base
    base_tag=$(dolt schema tags -r csv | awk -F, '$1=="t" && $2=="c"{print $3}')

    dolt checkout -b other
    dolt sql -q "update t set val = 11 where pk = 1"
    dolt sql -q "insert into t (pk,c,val) values (3,JSON_OBJECT('v',3),30)"
    dolt commit -am "source changes"

    dolt checkout main
    recreate_table_with_encoding t true "$ddl"
    dolt sql -q "update t set val = 21 where pk = 2"
    dolt sql -q "update t set c = JSON_OBJECT('v',44) where pk = 4"
    dolt commit -am "dest converts + changes"
    assert_col_tag t c "$base_tag"

    run dolt merge other -m merge
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "conflict" ]] || false

    run dolt sql -r csv -q "select pk, json_extract(c,'\$.v') v, val from t order by pk"
    [[ "$output" =~ "1,1,11" ]] || false
    [[ "$output" =~ "2,2,21" ]] || false
    [[ "$output" =~ "3,3,30" ]] || false
    [[ "$output" =~ "4,44,40" ]] || false

    run dolt sql -r csv -q "select count(*) as n from dolt_constraint_violations"
    [ "${lines[1]}" = "0" ]
}

@test "merge-adaptive-encoding: JSON with check constraint, source converts (case 2)" {
    ddl="create table t (pk int primary key, c json, val int, check (val >= 0))"
    dolt sql -q "$ddl"
    dolt sql -q "insert into t (pk,c,val) values (1,JSON_OBJECT('v',1),10),(2,JSON_OBJECT('v',2),20),(4,JSON_OBJECT('v',4),40)"
    dolt add . && dolt commit -m base
    base_tag=$(dolt schema tags -r csv | awk -F, '$1=="t" && $2=="c"{print $3}')

    dolt checkout -b other
    recreate_table_with_encoding t true "$ddl"
    dolt sql -q "update t set val = 11 where pk = 1"
    dolt sql -q "insert into t (pk,c,val) values (3,JSON_OBJECT('v',3),30)"
    dolt commit -am "source converts + changes"
    assert_col_tag t c "$base_tag"

    dolt checkout main
    dolt sql -q "update t set val = 21 where pk = 2"
    dolt sql -q "update t set c = JSON_OBJECT('v',44) where pk = 4"
    dolt commit -am "dest changes"

    run dolt merge other -m merge
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "conflict" ]] || false

    run dolt sql -r csv -q "select pk, json_extract(c,'\$.v') v, val from t order by pk"
    [[ "$output" =~ "1,1,11" ]] || false
    [[ "$output" =~ "2,2,21" ]] || false
    [[ "$output" =~ "3,3,30" ]] || false
    [[ "$output" =~ "4,44,40" ]] || false

    run dolt sql -r csv -q "select count(*) as n from dolt_constraint_violations"
    [ "${lines[1]}" = "0" ]
}

# ---------------------------------------------------------------------------
# Structural matrix: a base table whose primary index spans many prolly-tree leaf chunks,
# with a layout that gives each side genuinely *exclusive* new subtrees plus a shared range
# that both sides edit and partly delete:
#
#   SHARED  [2048, 4096)  present in the base. The two sides edit disjoint rows in it:
#       source owns even pks -> modifies pk%4==0, deletes pk%4==2
#       dest   owns odd  pks -> modifies pk%4==1, deletes pk%4==3
#   SRC_INS [0, 2048)     inserted only by the source  (source's exclusive new subtrees)
#   DST_INS [4096, 6144)  inserted only by the dest    (dest's exclusive new subtrees)
#
# Because each inserted region is brand new and untouched by the other side, the merge can
# represent it with whole-subtree (chunk-level) patches and graft it directly into the other
# branch's tree. If the two branches encode the long column differently and the merge grafts
# a subtree without rewriting it into the destination encoding, the result is a tree whose
# stored chunks don't match the schema's declared encoding -- a corruption that a count(*) or
# a val-only read will not notice. The assertions therefore force the engine to actually read
# and materialize the long column (matching on its content), so an encoding graft bug cannot
# hide behind column pruning.
#
# Values are large enough to be stored out-of-band; ~2k rows per region at ~100 rows/leaf
# gives tens of leaf chunks per region, i.e. real whole-subtree grafts.
# ---------------------------------------------------------------------------

# struct2_val <prefix> <pk-expr> <coltype>  -> SQL for a large, out-of-band, content-tagged
# value. The <prefix> records which edit produced the row so assertions can read it back.
struct2_val() {
    if [ "$3" = "json" ]; then
        echo "json_object('tag','$1','pk',$2,'pad',repeat('x',1190))"
    else
        echo "rpad(concat('$1-', $2), 1200, '.')"
    fi
}

# struct2_present <prefix> <coltype>  -> a WHERE predicate matching rows tagged <prefix>.
# Evaluating it touches the long column for every row, forcing materialization.
struct2_present() {
    if [ "$2" = "json" ]; then echo "json_extract(c,'\$.tag') = '$1'"; else echo "c like '$1-%'"; fi
}

# struct2_build <coltype> <case 1|2> <check-clause>
# Builds the base and both branches in the layout above, leaving HEAD on main ready to
# 'dolt merge other'. The converting side (dest for case 1, source for case 2) recreates the
# table as adaptive; the other side stays non-adaptive.
struct2_build() {
    local typ=$1 cs=$2 chk=$3 ddl
    ddl="create table t (pk int primary key, c $typ, val int $chk)"
    dolt sql -q "$ddl"
    dolt sql -q "insert into t (pk,c,val) with recursive s(n) as (select 2048 union all select n+1 from s where n < 4095) select n, $(struct2_val BASE n $typ), n from s"
    dolt add .
    dolt commit -m "base (shared range, non-adaptive)"

    dolt checkout -b other
    [ "$cs" = "2" ] && recreate_table_with_encoding t true "$ddl"
    dolt sql -q "update t set c = $(struct2_val SMOD pk $typ), val = val + 100000 where pk >= 2048 and pk < 4096 and pk % 4 = 0"
    dolt sql -q "delete from t where pk >= 2048 and pk < 4096 and pk % 4 = 2"
    dolt sql -q "insert into t (pk,c,val) with recursive s(n) as (select 0 union all select n+1 from s where n < 2047) select n, $(struct2_val SRC n $typ), n from s"
    dolt commit -am "source: shared edits + exclusive inserts [0,2048)"

    dolt checkout main
    [ "$cs" = "1" ] && recreate_table_with_encoding t true "$ddl"
    dolt sql -q "update t set c = $(struct2_val DMOD pk $typ), val = val + 200000 where pk >= 2048 and pk < 4096 and pk % 4 = 1"
    dolt sql -q "delete from t where pk >= 2048 and pk < 4096 and pk % 4 = 3"
    dolt sql -q "insert into t (pk,c,val) with recursive s(n) as (select 4096 union all select n+1 from s where n < 6143) select n, $(struct2_val DST n $typ), n from s"
    dolt commit -am "dest: shared edits + exclusive inserts [4096,6144)"
}

# struct2_assert <coltype>: forces a full read/materialization of the long column and checks
# every region merged correctly. Surviving rows: 2048 (src inserts) + 2048 (dest inserts)
# + 512 (src-modified shared) + 512 (dest-modified shared) = 5120; the 1024 shared rows
# marked for deletion are gone and no unmodified BASE row remains.
struct2_assert() {
    local typ=$1
    run dolt sql -r csv -q "select count(*) as n from t"
    [ "${lines[1]}" = "5120" ]
    # Each of these evaluates the long column for every row -> forces materialization.
    run dolt sql -r csv -q "select count(*) as n from t where $(struct2_present SRC $typ)"
    [ "${lines[1]}" = "2048" ]
    run dolt sql -r csv -q "select count(*) as n from t where $(struct2_present DST $typ)"
    [ "${lines[1]}" = "2048" ]
    run dolt sql -r csv -q "select count(*) as n from t where $(struct2_present SMOD $typ)"
    [ "${lines[1]}" = "512" ]
    run dolt sql -r csv -q "select count(*) as n from t where $(struct2_present DMOD $typ)"
    [ "${lines[1]}" = "512" ]
    run dolt sql -r csv -q "select count(*) as n from t where $(struct2_present BASE $typ)"
    [ "${lines[1]}" = "0" ]
    # shared rows marked for deletion on either side are gone
    run dolt sql -r csv -q "select count(*) as n from t where pk >= 2048 and pk < 4096 and (pk % 4 = 2 or pk % 4 = 3)"
    [ "${lines[1]}" = "0" ]
}

@test "merge-adaptive-encoding: structural TEXT, dest converts to adaptive (case 1)" {
    struct2_build text 1 ""
    run dolt merge other -m merge
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "conflict" ]] || false
    struct2_assert text
}

# KNOWN FIX TARGET (expected to FAIL until the bug is fixed): the merge SOURCE holds the
# column adaptively encoded and inserts whole new leaf subtrees; merging into the
# non-adaptive destination grafts those adaptive subtrees in without rewriting them. The
# merge reports success, but the resulting table is corrupt: reading the long column fails
# ("invalid hash length"). The forced-materialization assertions below are what expose it.
@test "merge-adaptive-encoding: structural TEXT, source converts to adaptive (case 2)" {
    struct2_build text 2 ""
    run dolt merge other -m merge
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "conflict" ]] || false
    struct2_assert text
}

@test "merge-adaptive-encoding: structural JSON, dest converts to adaptive (case 1)" {
    struct2_build json 1 ""
    run dolt merge other -m merge
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "conflict" ]] || false
    struct2_assert json
}

# KNOWN FIX TARGET (expected to FAIL until the bug is fixed): see the TEXT case-2 note above.
@test "merge-adaptive-encoding: structural JSON, source converts to adaptive (case 2)" {
    struct2_build json 2 ""
    run dolt merge other -m merge
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "conflict" ]] || false
    struct2_assert json
}

# KNOWN FIX TARGET (expected to FAIL until the bug is fixed): case-1 layout with a check
# constraint. The source (non-adaptive) inserts new rows that are validated against the
# check in the destination's adaptive context, reading the long column with a mismatched
# descriptor and panicking during the merge.
@test "merge-adaptive-encoding: structural TEXT with check constraint, dest converts (case 1)" {
    struct2_build text 1 ", check (val >= 0)"
    run dolt merge other -m merge
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "conflict" ]] || false
    struct2_assert text
    run dolt sql -r csv -q "select count(*) as n from dolt_constraint_violations"
    [ "${lines[1]}" = "0" ]
}

# KNOWN FIX TARGET (expected to FAIL until the bug is fixed): case-2 layout with a check
# constraint -- the grafted-subtree corruption of case 2 plus constraint validation.
@test "merge-adaptive-encoding: structural TEXT with check constraint, source converts (case 2)" {
    struct2_build text 2 ", check (val >= 0)"
    run dolt merge other -m merge
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "conflict" ]] || false
    struct2_assert text
    run dolt sql -r csv -q "select count(*) as n from dolt_constraint_violations"
    [ "${lines[1]}" = "0" ]
}
