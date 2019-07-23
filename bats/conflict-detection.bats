#!/usr/bin/env bats

setup() {
    load $BATS_TEST_DIRNAME/helper/common.bash
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

@test "two branches modify different cell different row. merge. no conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt table put-row test pk:1 c1:1 c2:1 c3:1 c4:1 c5:1
    dolt add test
    dolt commit -m "table created"
    dolt branch change-cell
    dolt table put-row test pk:0 c1:11 c2:0 c3:0 c4:0 c5:0
    dolt add test
    dolt commit -m "changed pk=0 c1 to 11"
    dolt checkout change-cell
    dolt table put-row test pk:1 c1:1 c2:1 c3:1 c4:1 c5:11
    dolt add test
    dolt commit -m "changed pk=1 c5 to 11"
    dolt checkout master
    run dolt merge change-cell
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ "$output" =~ "1 rows modified" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches modify different cell same row. merge. no conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt add test
    dolt commit -m "table created"
    dolt branch change-cell
    dolt table put-row test pk:0 c1:11 c2:0 c3:0 c4:0 c5:0
    dolt add test
    dolt commit -m "changed pk=0 c1 to 11"
    dolt checkout change-cell
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:11
    dolt add test
    dolt commit -m "changed pk=0 c5 to 11"
    dolt checkout master
    run dolt merge change-cell
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ "$output" =~ "1 rows modified" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches modify same cell. merge. conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt add test
    dolt commit -m "table created"
    dolt branch change-cell
    dolt table put-row test pk:0 c1:1 c2:1 c3:1 c4:1 c5:1
    dolt add test
    dolt commit -m "changed pk=0 all cells to 1"
    dolt checkout change-cell
    dolt table put-row test pk:0 c1:11 c2:11 c3:11 c4:11 c5:11
    dolt add test
    dolt commit -m "changed pk=0 all cells to 11"
    dolt checkout master
    run dolt merge change-cell
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
}

@test "two branches add a different row. merge. no conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch add-row
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt add test
    dolt commit -m "added pk=0 row"
    dolt checkout add-row
    dolt table put-row test pk:1 c1:1 c2:1 c3:1 c4:1 c5:1
    dolt add test
    dolt commit -m "added pk=1 row"
    dolt checkout master
    run dolt merge add-row
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ "$output" =~ "1 rows added" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches add same row. merge. no conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch add-row
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt add test
    dolt commit -m "added pk=0 row"
    dolt checkout add-row
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt add test
    dolt commit -m "added pk=0 row"
    dolt checkout master
    run dolt merge add-row
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "one branch add table, other modifies table. merge. no conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch add-table
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt add test
    dolt commit -m "added row"
    dolt checkout add-table
    dolt table create -s=`batshelper 1pk5col-ints.schema` test2
    dolt add test2
    dolt commit -m "added new table test2"
    dolt checkout master
    run dolt merge add-table
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    skip "should have a merge summary section that says 1 table changed" 
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches add same column. merge. no conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch add-column
    dolt schema --add-column test c0 int
    dolt add test
    dolt commit -m "added column c0"
    dolt checkout add-column
    dolt schema --add-column test c0 int
    dolt add test
    dolt commit -m "added same column c0"
    dolt checkout master
    run dolt merge add-column
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches add different column. merge. no conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch add-column
    dolt schema --add-column test c0 int
    dolt add test
    dolt commit -m "added column c0"
    dolt checkout add-column
    dolt schema --add-column test c6 int
    dolt add test
    dolt commit -m "added column c6"
    dolt checkout master
    run dolt merge add-column
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches add same column, different types. merge. conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch add-column
    dolt schema --add-column test c0 string
    dolt add test
    dolt commit -m "added column c0 as string"
    dolt checkout add-column
    dolt schema --add-column test c0 int
    dolt add test
    dolt commit -m "added column c0 as int"
    dolt checkout master
    run dolt merge add-column
    [ $status -eq 0 ]
    skip "This created two c0 columns with different types and tag numbers. Bug I think."
    [[ "$output" =~ "CONFLICT" ]] || false
}

@test "two branches delete same column. merge. no conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch delete-column
    dolt schema --drop-column test c5
    dolt add test
    dolt commit -m "deleted c45 column"
    dolt checkout delete-column
    dolt schema --drop-column test c5
    dolt add test
    dolt commit -m "deleted c5 again"
    dolt checkout master
    run dolt merge delete-column
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches delete different column. merge. no conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch delete-column
    dolt schema --drop-column test c5
    dolt add test
    dolt commit -m "deleted column c5"
    dolt checkout delete-column
    dolt schema --drop-column test c4
    dolt add test
    dolt commit -m "deleted column c4"
    dolt checkout master
    run dolt merge delete-column
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches rename same column to same name. merge. no conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch rename-column
    dolt schema --rename-column test c5 c0
    dolt add test
    dolt commit -m "renamed c5 to c0"
    dolt checkout rename-column
    dolt schema --rename-column test c5 c0
    dolt add test
    dolt commit -m "renamed c5 to c0 again"
    dolt checkout master
    run dolt merge rename-column
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches rename same column to different name. merge. conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch rename-column
    dolt schema --rename-column test c5 c0
    dolt add test
    dolt commit -m "renamed c5 to c0"
    dolt checkout rename-column
    dolt schema --rename-column test c5 c6
    dolt add test
    dolt commit -m "renamed c5 to c6"
    dolt checkout master
    run dolt merge rename-column
    [ $status -eq 1 ]
    [[ "$output" =~ "Bad merge" ]] || false
    skip "This currently is a failed merge. I think it should be a conflict that you can resolve by modifying the schema. Basically choose a column name for the tag. The data is the same."
    [ $status -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
}

@test "two branches rename different column to same name. merge. conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch rename-column
    dolt schema --rename-column test c5 c0
    dolt add test
    dolt commit -m "renamed c5 to c0"
    dolt checkout rename-column
    dolt schema --rename-column test c4 c0
    dolt add test
    dolt commit -m "renamed c5 to c6"
    dolt checkout master
    run dolt merge rename-column
    [ $status -eq 1 ]
    [[ "$output" =~ "Bad merge" ]] || false
    skip "Same as test above. This case needs some thought. My initial instinct was that this generates a tag conflict. Pick one tag and then you have a data conflict because the schemas are the same on both branches."
    [ $status -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
}

# Altering types and properties of the schema are not really supported by the 
# command line. Have to upload schema files for these next few tests.
@test "two branches change type of same column to same type. merge. no conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch change-types
    dolt table create -f -s=`batshelper 1pk5col-ints-change-type-1.schema` test
    dolt add test
    dolt commit -m "changed c1 to type bool"
    dolt checkout change-types
    dolt table create -f -s=`batshelper 1pk5col-ints-change-type-1.schema` test
    dolt add test
    dolt commit -m "changed c1 to type bool again"
    dolt checkout master
    run dolt merge change-types
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches change type of same column to different type. merge. conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch change-types
    dolt table create -f -s=`batshelper 1pk5col-ints-change-type-1.schema` test
    dolt add test
    dolt commit -m "changed c1 to type bool"
    dolt checkout change-types
    dolt table create -f -s=`batshelper 1pk5col-ints-change-type-2.schema` test
    dolt add test
    dolt commit -m "changed c1 to type float"
    dolt checkout master
    run dolt merge change-types
    [ $status -eq 1 ]
    [[ "$output" =~ "Bad merge" ]] || false
    skip "I think changing a type to two different types should throw a conflict" 
    [ $status -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
}

@test "two branches make same column primary key. merge. no conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch add-pk
    dolt table create -f -s=`batshelper 1pk5col-ints-change-pk-1.schema` test
    dolt add test
    dolt commit -m "made c1 a pk"
    dolt checkout add-pk
    dolt table create -f -s=`batshelper 1pk5col-ints-change-pk-1.schema` test
    dolt add test
    dolt commit -m "made c1 a pk again"
    dolt checkout master
    run dolt merge add-pk
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches add same primary key column. merge. no conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch add-pk
    dolt table create -f -s=`batshelper 1pk5col-ints-add-pk1.schema` test
    dolt add test
    dolt commit -m "added pk pk1"
    dolt checkout add-pk
    dolt table create -f -s=`batshelper 1pk5col-ints-add-pk1.schema` test
    dolt add test
    dolt commit -m "added pk pk1 again"
    dolt checkout master
    run dolt merge add-pk
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches make different columns primary key. merge. conflict" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "table created"
    dolt branch add-pk
    dolt table create -f -s=`batshelper 1pk5col-ints-add-pk1.schema` test
    dolt add test
    dolt commit -m "added pk pk1"
    dolt checkout add-pk
    dolt table create -f -s=`batshelper 1pk5col-ints-add-pk2.schema` test
    dolt add test
    dolt commit -m "added pk pk2"
    dolt checkout master
    run dolt merge add-pk
    [ $status -eq 0 ]
    skip "This merges fine right now. Should throw conflict."
    [[ "$output" =~ "CONFLICT" ]] || false
}

@test "two branches both create different tables. merge. no conflict" {
    dolt branch table1
    dolt branch table2
    dolt checkout table1
    dolt table create -s=`batshelper 1pk5col-ints.schema` table1
    dolt add table1
    dolt commit -m "first table"
    dolt checkout table2
    dolt table create -s=`batshelper 2pk5col-ints.schema` table2
    dolt add table2
    dolt commit -m "second table"
    dolt checkout master
    run dolt merge table1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]] || false
    run dolt merge table2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}