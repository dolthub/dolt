#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

two_pk_header="pk1,pk2,c1,c2"
one_pk_header="pk1,c1,c2"

two_pk="$two_pk_header
1,9,128,32
2,8,129,31
3,7,130,30
4,6,131,29
5,5,132,28"
one_pk="$one_pk_header
1,128,32
2,129,31
3,130,30
4,131,29
5,132,28"

two_pk_all_updated="$two_pk_header
1,9,128,256
2,8,129,256
3,7,130,256
4,6,131,256
5,5,132,256"
one_pk_all_updated="$one_pk_header
1,128,256
2,129,256
3,130,256
4,131,256
5,132,256"

two_pk_one_row_deleted="$two_pk_header
1,9,128,32
3,7,130,30
4,6,131,29
5,5,132,28"
one_pk_one_row_deleted="$one_pk_header
1,128,32
3,130,30
4,131,29
5,132,28"

two_pk_two_row_deleted="$two_pk_header
1,9,128,32
4,6,131,29
5,5,132,28"
one_pk_two_row_deleted="$one_pk_header
1,128,32
4,131,29
5,132,28"

two_pk_one_row_updated="$two_pk_header
1,9,128,32
2,8,129,256
3,7,130,30
4,6,131,29
5,5,132,28"
one_pk_one_row_updated="$one_pk_header
1,128,32
2,129,256
3,130,30
4,131,29
5,132,28"

two_pk_two_row_updated="$two_pk_header
1,9,128,32
2,8,129,256
3,7,130,256
4,6,131,29
5,5,132,28"
one_pk_two_row_updated="$one_pk_header
1,128,32
2,129,256
3,130,256
4,131,29
5,132,28"

min_pk1=1
max_pk1=5
min_pk2=5
max_pk2=9
min_c1=128
max_c1=132
min_c2=28
max_c2=32

create_tables() {
    cat <<EOF > two_pk.csv
$two_pk
EOF
    cat <<EOF > one_pk.csv
$one_pk
EOF
    dolt table import -c -pk pk1,pk2 --file-type=csv two_pk two_pk.csv
    dolt table import -c -pk pk1 --file-type=csv one_pk one_pk.csv
}

setup() {
    setup_common
    create_tables
}

teardown() {
    assert_feature_version
    teardown_common
}

test_mutation() {
    dml="$1"
    table="$2"
    expected="$3"
    uses_pk="$4"
    dolt sql -q "$dml"
    run dolt sql -q "select * from $table" -r csv
    [ "$status" -eq "0" ]
    [ "$output" == "$expected" ] || (echo $output && exit 1)
    dolt reset --hard
    dolt sql --batch -q "$dml ; $dml"
    run dolt sql -q "select * from $table" -r csv
    [ "$status" -eq "0" ]
    [ "$output" == "$expected" ] || (echo $output && exit 1)
    run dolt sql -q "explain $dml"
    [ "$status" -eq "0" ]
    if ! [ -z "$uses_pk" ]; then
        [[ "$output" =~ "IndexedTableAccess" ]] || exit 1
    else
        if [[ "$output" =~ "IndexedTableAccess" ]]; then exit 1; fi
    fi
}

@test "index-on-writes: delete all two_pk" {
    test_mutation "delete from two_pk" "two_pk" "$two_pk_header"
}

@test "index-on-writes: delete all one_pk" {
    test_mutation "delete from one_pk" "one_pk" "$one_pk_header"
}

@test "index-on-writes: delete all two_pk, <>, pk" {
    test_mutation "delete from two_pk where pk1 <> 1024 and pk2 <> 1024" "two_pk" "$two_pk_header"
}

@test "index-on-writes: delete all one_pk, <>, pk" {
    test_mutation "delete from one_pk where pk1 <> 1024" "one_pk" "$one_pk_header" "yes"
}

@test "index-on-writes: delete all two_pk, >, pk" {
    test_mutation "delete from two_pk where pk1 > $((min_pk1-1)) and pk2 > $((min_pk2-1))" "two_pk" "$two_pk_header" "yes"
}

@test "index-on-writes: delete all one_pk, >, pk" {
    test_mutation "delete from one_pk where pk1 > $((min_pk1-1))" "one_pk" "$one_pk_header" "yes"
}

@test "index-on-writes: delete all two_pk, >=, pk" {
    test_mutation "delete from two_pk where pk1 >= $min_pk1 and pk2 >= $min_pk2" "two_pk" "$two_pk_header" "yes"
}

@test "index-on-writes: delete all one_pk, >=, pk" {
    test_mutation "delete from one_pk where pk1 >= $min_pk1" "one_pk" "$one_pk_header" "yes"
}

@test "index-on-writes: delete all two_pk, <, pk" {
    test_mutation "delete from two_pk where pk1 < $((max_pk1+1)) and pk2 < $((max_pk2+1))" "two_pk" "$two_pk_header" "yes"
}

@test "index-on-writes: delete all one_pk, <, pk" {
    test_mutation "delete from one_pk where pk1 < $((max_pk1+1))" "one_pk" "$one_pk_header" "yes"
}

@test "index-on-writes: delete all two_pk, <=, pk" {
    test_mutation "delete from two_pk where pk1 <= $max_pk1 and pk2 <= $max_pk2" "two_pk" "$two_pk_header" "yes"
}

@test "index-on-writes: delete all one_pk, <=, pk" {
    test_mutation "delete from one_pk where pk1 <= $max_pk1" "one_pk" "$one_pk_header" "yes"
}

@test "index-on-writes: delete all two_pk, <>, non-pk" {
    test_mutation "delete from two_pk where c1 <> 1024" "two_pk" "$two_pk_header"
}

@test "index-on-writes: delete all one_pk, <>, non-pk" {
    test_mutation "delete from one_pk where c1 <> 1024" "one_pk" "$one_pk_header"
}

@test "index-on-writes: delete all two_pk, >, non-pk" {
    test_mutation "delete from two_pk where c1 > $((min_c1-1))" "two_pk" "$two_pk_header"
}

@test "index-on-writes: delete all one_pk, >, non-pk" {
    test_mutation "delete from one_pk where c1 > $((min_c1-1))" "one_pk" "$one_pk_header"
}

@test "index-on-writes: delete all two_pk, >=, non-pk" {
    test_mutation "delete from two_pk where c1 >= $min_c1" "two_pk" "$two_pk_header"
}

@test "index-on-writes: delete all one_pk, >=, non-pk" {
    test_mutation "delete from one_pk where c1 >= $min_c1" "one_pk" "$one_pk_header"
}

@test "index-on-writes: delete all two_pk, <, non-pk" {
    test_mutation "delete from two_pk where c1 < $((max_c1+1))" "two_pk" "$two_pk_header"
}

@test "index-on-writes: delete all one_pk, <, non-pk" {
    test_mutation "delete from one_pk where c1 < $((max_c1+1))" "one_pk" "$one_pk_header"
}

@test "index-on-writes: delete all two_pk, <=, non-pk" {
    test_mutation "delete from two_pk where c2 <= $max_c2" "two_pk" "$two_pk_header"
}

@test "index-on-writes: delete all one_pk, <=, non-pk" {
    test_mutation "delete from one_pk where c2 <= $max_c2" "one_pk" "$one_pk_header"
}

@test "index-on-writes: delete all two_pk, <>, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 <> 1024 and pk2 <> 1024 and c1 <> 1024" "two_pk" "$two_pk_header"
}

@test "index-on-writes: delete all one_pk, <>, pk + non-pk" {
    test_mutation "delete from one_pk where pk1 <> 1024 and c1 <> 1024" "one_pk" "$one_pk_header" "yes"
}

@test "index-on-writes: delete all two_pk, >, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 > $((min_pk1-1)) and pk2 > $((min_pk2-1)) and c1 > $((min_c1-1))" "two_pk" "$two_pk_header" "yes"
}

@test "index-on-writes: delete all one_pk, >, pk + non-pk" {
    test_mutation "delete from one_pk where pk1 > $((min_pk1-1)) and c1 > $((min_c1-1))" "one_pk" "$one_pk_header" "yes"
}

@test "index-on-writes: delete all two_pk, >=, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 >= $min_pk1 and pk2 >= $min_pk2 and c1 >= $min_c1" "two_pk" "$two_pk_header" "yes"
}

@test "index-on-writes: delete all one_pk, >=, pk + non-pk" {
    test_mutation "delete from one_pk where pk1 >= $min_pk1 and c1 >= $min_c1" "one_pk" "$one_pk_header" "yes"
}

@test "index-on-writes: delete all two_pk, <, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 < $((max_pk1+1)) and pk2 < $((max_pk2+1)) and c1 < $((max_c1+1))" "two_pk" "$two_pk_header" "yes"
}

@test "index-on-writes: delete all one_pk, <, pk + non-pk" {
    test_mutation "delete from one_pk where pk1 < $((max_pk1+1)) and c1 < $((max_c1+1))" "one_pk" "$one_pk_header" "yes"
}

@test "index-on-writes: delete all two_pk, <=, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 <= $max_pk1 and pk2 <= $max_pk2 and c2 <= $max_c2" "two_pk" "$two_pk_header" "yes"
}

@test "index-on-writes: delete all one_pk, <=, pk + non-pk" {
    test_mutation "delete from one_pk where pk1 <= $max_pk1 and c2 <= $max_c2" "one_pk" "$one_pk_header" "yes"
}

@test "index-on-writes: update all two_pk" {
    test_mutation "update two_pk set c2 = 256" "two_pk" "$two_pk_all_updated"
}

@test "index-on-writes: update all one_pk" {
    test_mutation "update one_pk set c2 = 256" "one_pk" "$one_pk_all_updated"
}

@test "index-on-writes: update all two_pk, <>, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 <> 1024 and pk2 <> 1024" "two_pk" "$two_pk_all_updated"
}

@test "index-on-writes: update all one_pk, <>, pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 <> 1024" "one_pk" "$one_pk_all_updated" "yes"
}

@test "index-on-writes: update all two_pk, >, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 > $((min_pk1-1)) and pk2 > $((min_pk2-1))" "two_pk" "$two_pk_all_updated" "yes"
}

@test "index-on-writes: update all one_pk, >, pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 > $((min_pk1-1))" "one_pk" "$one_pk_all_updated" "yes"
}

@test "index-on-writes: update all two_pk, >=, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 >= $min_pk1 and pk2 >= $min_pk2" "two_pk" "$two_pk_all_updated" "yes"
}

@test "index-on-writes: update all one_pk, >=, pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 >= $min_pk1" "one_pk" "$one_pk_all_updated" "yes"
}

@test "index-on-writes: update all two_pk, <, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 < $((max_pk1+1)) and pk2 < $((max_pk2+1))" "two_pk" "$two_pk_all_updated" "yes"
}

@test "index-on-writes: update all one_pk, <, pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 < $((max_pk1+1))" "one_pk" "$one_pk_all_updated" "yes"
}

@test "index-on-writes: update all two_pk, <=, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 <= $max_pk1 and pk2 <= $max_pk2" "two_pk" "$two_pk_all_updated" "yes"
}

@test "index-on-writes: update all one_pk, <=, pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 <= $max_pk1" "one_pk" "$one_pk_all_updated" "yes"
}

@test "index-on-writes: update all two_pk, <>, non-pk" {
    test_mutation "update two_pk set c2 = 256 where c1 <> 1024" "two_pk" "$two_pk_all_updated"
}

@test "index-on-writes: update all one_pk, <>, non-pk" {
    test_mutation "update one_pk set c2 = 256 where c1 <> 1024" "one_pk" "$one_pk_all_updated"
}

@test "index-on-writes: update all two_pk, >, non-pk" {
    test_mutation "update two_pk set c2 = 256 where c1 > $((min_c1-1))" "two_pk" "$two_pk_all_updated"
}

@test "index-on-writes: update all one_pk, >, non-pk" {
    test_mutation "update one_pk set c2 = 256 where c1 > $((min_c1-1))" "one_pk" "$one_pk_all_updated"
}

@test "index-on-writes: update all two_pk, >=, non-pk" {
    test_mutation "update two_pk set c2 = 256 where c1 >= $min_c1" "two_pk" "$two_pk_all_updated"
}

@test "index-on-writes: update all one_pk, >=, non-pk" {
    test_mutation "update one_pk set c2 = 256 where c1 >= $min_c1" "one_pk" "$one_pk_all_updated"
}

@test "index-on-writes: update all two_pk, <, non-pk" {
    test_mutation "update two_pk set c2 = 256 where c1 < $((max_c1+1))" "two_pk" "$two_pk_all_updated"
}

@test "index-on-writes: update all one_pk, <, non-pk" {
    test_mutation "update one_pk set c2 = 256 where c1 < $((max_c1+1))" "one_pk" "$one_pk_all_updated"
}

@test "index-on-writes: update all two_pk, <=, non-pk" {
    test_mutation "update two_pk set c2 = 256 where c2 <= $max_c2" "two_pk" "$two_pk_all_updated"
}

@test "index-on-writes: update all one_pk, <=, non-pk" {
    test_mutation "update one_pk set c2 = 256 where c2 <= $max_c2" "one_pk" "$one_pk_all_updated"
}

@test "index-on-writes: update all two_pk, <>, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 <> 1024 and pk2 <> 1024 and c1 <> 1024" "two_pk" "$two_pk_all_updated"
}

@test "index-on-writes: update all one_pk, <>, pk + non-pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 <> 1024 and c1 <> 1024" "one_pk" "$one_pk_all_updated" "yes"
}

@test "index-on-writes: update all two_pk, >, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 > $((min_pk1-1)) and pk2 > $((min_pk2-1)) and c1 > $((min_c1-1))" "two_pk" "$two_pk_all_updated" "yes"
}

@test "index-on-writes: update all one_pk, >, pk + non-pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 > $((min_pk1-1)) and c1 > $((min_c1-1))" "one_pk" "$one_pk_all_updated" "yes"
}

@test "index-on-writes: update all two_pk, >=, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 >= $min_pk1 and pk2 >= $min_pk2 and c1 >= $min_c1" "two_pk" "$two_pk_all_updated" "yes"
}

@test "index-on-writes: update all one_pk, >=, pk + non-pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 >= $min_pk1 and c1 >= $min_c1" "one_pk" "$one_pk_all_updated" "yes"
}

@test "index-on-writes: update all two_pk, <, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 < $((max_pk1+1)) and pk2 < $((max_pk2+1)) and c1 < $((max_c1+1))" "two_pk" "$two_pk_all_updated" "yes"
}

@test "index-on-writes: update all one_pk, <, pk + non-pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 < $((max_pk1+1)) and c1 < $((max_c1+1))" "one_pk" "$one_pk_all_updated" "yes"
}

@test "index-on-writes: update all two_pk, <=, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 <= $max_pk1 and pk2 <= $max_pk2 and c2 <= $max_c2" "two_pk" "$two_pk_all_updated" "yes"
}

@test "index-on-writes: update all one_pk, <=, pk + non-pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 <= $max_pk1 and c2 <= $max_c2" "one_pk" "$one_pk_all_updated" "yes"
}

@test "index-on-writes: delete none two_pk, =, pk" {
    test_mutation "delete from two_pk where pk1 = 1024 and pk2 = 1024" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: delete none one_pk, =, pk" {
    test_mutation "delete from one_pk where pk1 = 1024" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: delete none two_pk, <=, pk" {
    test_mutation "delete from two_pk where pk1 <= $((min_pk1-1)) and pk2 <= $((min_pk2-1))" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: delete none one_pk, <=, pk" {
    test_mutation "delete from one_pk where pk1 <= $((min_pk1-1))" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: delete none two_pk, <, pk" {
    test_mutation "delete from two_pk where pk1 < $min_pk1 and pk2 < $min_pk2" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: delete none one_pk, <, pk" {
    test_mutation "delete from one_pk where pk1 < $min_pk1" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: delete none two_pk, >=, pk" {
    test_mutation "delete from two_pk where pk1 >= $((max_pk1+1)) and pk2 >= $((max_pk2+1))" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: delete none one_pk, >=, pk" {
    test_mutation "delete from one_pk where pk1 >= $((max_pk1+1))" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: delete none two_pk, >, pk" {
    test_mutation "delete from two_pk where pk1 > $max_pk1 and pk2 > $max_pk2" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: delete none one_pk, >, pk" {
    test_mutation "delete from one_pk where pk1 > $max_pk1" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: delete none two_pk, =, non-pk" {
    test_mutation "delete from two_pk where c1 = 1024" "two_pk" "$two_pk"
}

@test "index-on-writes: delete none one_pk, =, non-pk" {
    test_mutation "delete from one_pk where c1 = 1024" "one_pk" "$one_pk"
}

@test "index-on-writes: delete none two_pk, <=, non-pk" {
    test_mutation "delete from two_pk where c1 <= $((min_c1-1))" "two_pk" "$two_pk"
}

@test "index-on-writes: delete none one_pk, <=, non-pk" {
    test_mutation "delete from one_pk where c1 <= $((min_c1-1))" "one_pk" "$one_pk"
}

@test "index-on-writes: delete none two_pk, <, non-pk" {
    test_mutation "delete from two_pk where c1 < $min_c1" "two_pk" "$two_pk"
}

@test "index-on-writes: delete none one_pk, <, non-pk" {
    test_mutation "delete from one_pk where c1 < $min_c1" "one_pk" "$one_pk"
}

@test "index-on-writes: delete none two_pk, >=, non-pk" {
    test_mutation "delete from two_pk where c1 >= $((max_c1+1))" "two_pk" "$two_pk"
}

@test "index-on-writes: delete none one_pk, >=, non-pk" {
    test_mutation "delete from one_pk where c1 >= $((max_c1+1))" "one_pk" "$one_pk"
}

@test "index-on-writes: delete none two_pk, >, non-pk" {
    test_mutation "delete from two_pk where c2 > $max_c2" "two_pk" "$two_pk"
}

@test "index-on-writes: delete none one_pk, >, non-pk" {
    test_mutation "delete from one_pk where c2 > $max_c2" "one_pk" "$one_pk"
}

@test "index-on-writes: delete none two_pk, =, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 = 1024 and pk2 = 1024 and c1 = 1024" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: delete none one_pk, =, pk + non-pk" {
    test_mutation "delete from one_pk where pk1 = 1024 and c1 = 1024" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: delete none two_pk, <=, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 <= $((min_pk1-1)) and pk2 <= $((min_pk2-1)) and c1 <= $((min_c1-1))" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: delete none one_pk, <=, pk + non-pk" {
    test_mutation "delete from one_pk where pk1 <= $((min_pk1-1)) and c1 <= $((min_c1-1))" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: delete none two_pk, <, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 < $min_pk1 and pk2 < $min_pk2 and c1 < $min_c1" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: delete none one_pk, <, pk + non-pk" {
    test_mutation "delete from one_pk where pk1 < $min_pk1 and c1 < $min_c1" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: delete none two_pk, >=, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 >= $((max_pk1+1)) and pk2 >= $((max_pk2+1)) and c1 >= $((max_c1+1))" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: delete none one_pk, >=, pk + non-pk" {
    test_mutation "delete from one_pk where pk1 >= $((max_pk1+1)) and c1 >= $((max_c1+1))" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: delete none two_pk, >, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 > $max_pk1 and pk2 > $max_pk2 and c2 > $max_c2" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: delete none one_pk, >, pk + non-pk" {
    test_mutation "delete from one_pk where pk1 > $max_pk1 and c2 > $max_c2" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: update none two_pk, =, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 = 1024 and pk2 = 1024" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: update none one_pk, =, pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 = 1024" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: update none two_pk, <=, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 <= $((min_pk1-1)) and pk2 <= $((min_pk2-1))" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: update none one_pk, <=, pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 <= $((min_pk1-1))" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: update none two_pk, <, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 < $min_pk1 and pk2 < $min_pk2" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: update none one_pk, <, pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 < $min_pk1" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: update none two_pk, >=, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 >= $((max_pk1+1)) and pk2 >= $((max_pk2+1))" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: update none one_pk, >=, pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 >= $((max_pk1+1))" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: update none two_pk, >, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 > $max_pk1 and pk2 > $max_pk2" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: update none one_pk, >, pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 > $max_pk1" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: update none two_pk, =, non-pk" {
    test_mutation "update two_pk set c2 = 256 where c1 = 1024" "two_pk" "$two_pk"
}

@test "index-on-writes: update none one_pk, =, non-pk" {
    test_mutation "update one_pk set c2 = 256 where c1 = 1024" "one_pk" "$one_pk"
}

@test "index-on-writes: update none two_pk, <=, non-pk" {
    test_mutation "update two_pk set c2 = 256 where c1 <= $((min_c1-1))" "two_pk" "$two_pk"
}

@test "index-on-writes: update none one_pk, <=, non-pk" {
    test_mutation "update one_pk set c2 = 256 where c1 <= $((min_c1-1))" "one_pk" "$one_pk"
}

@test "index-on-writes: update none two_pk, <, non-pk" {
    test_mutation "update two_pk set c2 = 256 where c1 < $min_c1" "two_pk" "$two_pk"
}

@test "index-on-writes: update none one_pk, <, non-pk" {
    test_mutation "update one_pk set c2 = 256 where c1 < $min_c1" "one_pk" "$one_pk"
}

@test "index-on-writes: update none two_pk, >=, non-pk" {
    test_mutation "update two_pk set c2 = 256 where c1 >= $((max_c1+1))" "two_pk" "$two_pk"
}

@test "index-on-writes: update none one_pk, >=, non-pk" {
    test_mutation "update one_pk set c2 = 256 where c1 >= $((max_c1+1))" "one_pk" "$one_pk"
}

@test "index-on-writes: update none two_pk, >, non-pk" {
    test_mutation "update two_pk set c2 = 256 where c2 > $max_c2" "two_pk" "$two_pk"
}

@test "index-on-writes: update none one_pk, >, non-pk" {
    test_mutation "update one_pk set c2 = 256 where c2 > $max_c2" "one_pk" "$one_pk"
}

@test "index-on-writes: update none two_pk, =, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 = 1024 and pk2 = 1024 and c1 = 1024" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: update none one_pk, =, pk + non-pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 = 1024 and c1 = 1024" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: update none two_pk, <=, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 <= $((min_pk1-1)) and pk2 <= $((min_pk2-1)) and c1 <= $((min_c1-1))" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: update none one_pk, <=, pk + non-pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 <= $((min_pk1-1)) and c1 <= $((min_c1-1))" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: update none two_pk, <, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 < $min_pk1 and pk2 < $min_pk2 and c1 < $min_c1" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: update none one_pk, <, pk + non-pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 < $min_pk1 and c1 < $min_c1" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: update none two_pk, >=, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 >= $((max_pk1+1)) and pk2 >= $((max_pk2+1)) and c1 >= $((max_c1+1))" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: update none one_pk, >=, pk + non-pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 >= $((max_pk1+1)) and c1 >= $((max_c1+1))" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: update none two_pk, >, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 > $max_pk1 and pk2 > $max_pk2 and c2 > $max_c2" "two_pk" "$two_pk" "yes"
}

@test "index-on-writes: update none one_pk, >, pk + non-pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 > $max_pk1 and c2 > $max_c2" "one_pk" "$one_pk" "yes"
}

@test "index-on-writes: delete partial two_pk, =, pk" {
    test_mutation "delete from two_pk where pk1 = 2 and pk2 = 8" "two_pk" "$two_pk_one_row_deleted" "yes"
}

@test "index-on-writes: delete partial one_pk, =, pk" {
    test_mutation "delete from one_pk where pk1 = 2" "one_pk" "$one_pk_one_row_deleted" "yes"
}

@test "index-on-writes: delete partial two_pk, =, non-pk" {
    test_mutation "delete from two_pk where c1 = 129" "two_pk" "$two_pk_one_row_deleted"
}

@test "index-on-writes: delete partial one_pk, =, non-pk" {
    test_mutation "delete from one_pk where c1 = 129" "one_pk" "$one_pk_one_row_deleted"
}

@test "index-on-writes: delete partial two_pk, =, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 = 2 and pk2 = 8 and c1 = 129" "two_pk" "$two_pk_one_row_deleted" "yes"
}

@test "index-on-writes: delete partial one_pk, =, pk + non-pk" {
    test_mutation "delete from one_pk where pk1 = 2 and c1 = 129" "one_pk" "$one_pk_one_row_deleted" "yes"
}

@test "index-on-writes: delete partial two_pk, >, pk" {
    test_mutation "delete from two_pk where pk1 > 1 and pk2 > 6" "two_pk" "$two_pk_two_row_deleted" "yes"
}

@test "index-on-writes: delete partial two_pk, >=, pk" {
    test_mutation "delete from two_pk where pk1 >= 2 and pk2 >= 7" "two_pk" "$two_pk_two_row_deleted" "yes"
}

@test "index-on-writes: delete partial two_pk, <, pk" {
    test_mutation "delete from two_pk where pk1 < 4 and pk2 < 9" "two_pk" "$two_pk_two_row_deleted" "yes"
}

@test "index-on-writes: delete partial two_pk, <=, pk" {
    test_mutation "delete from two_pk where pk1 <= 3 and pk2 <= 8" "two_pk" "$two_pk_two_row_deleted" "yes"
}

@test "index-on-writes: delete partial two_pk, >, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 > 1 and pk2 > 6 and c1 = 129" "two_pk" "$two_pk_one_row_deleted" "yes"
}

@test "index-on-writes: delete partial two_pk, >=, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 >= 2 and pk2 >= 7 and c1 = 129" "two_pk" "$two_pk_one_row_deleted" "yes"
}

@test "index-on-writes: delete partial two_pk, <, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 < 4 and pk2 < 9 and c1 = 129" "two_pk" "$two_pk_one_row_deleted" "yes"
}

@test "index-on-writes: delete partial two_pk, <=, pk + non-pk" {
    test_mutation "delete from two_pk where pk1 <= 3 and pk2 <= 8 and c1 = 129" "two_pk" "$two_pk_one_row_deleted" "yes"
}

@test "index-on-writes: update partial two_pk, =, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 = 2 and pk2 = 8" "two_pk" "$two_pk_one_row_updated" "yes"
}

@test "index-on-writes: update partial one_pk, =, pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 = 2" "one_pk" "$one_pk_one_row_updated" "yes"
}

@test "index-on-writes: update partial two_pk, =, non-pk" {
    test_mutation "update two_pk set c2 = 256 where c1 = 129" "two_pk" "$two_pk_one_row_updated"
}

@test "index-on-writes: update partial one_pk, =, non-pk" {
    test_mutation "update one_pk set c2 = 256 where c1 = 129" "one_pk" "$one_pk_one_row_updated"
}

@test "index-on-writes: update partial two_pk, =, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 = 2 and pk2 = 8 and c1 = 129" "two_pk" "$two_pk_one_row_updated" "yes"
}

@test "index-on-writes: update partial one_pk, =, pk + non-pk" {
    test_mutation "update one_pk set c2 = 256 where pk1 = 2 and c1 = 129" "one_pk" "$one_pk_one_row_updated" "yes"
}

@test "index-on-writes: update partial two_pk, >, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 > 1 and pk2 > 6" "two_pk" "$two_pk_two_row_updated" "yes"
}

@test "index-on-writes: update partial two_pk, >=, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 >= 2 and pk2 >= 7" "two_pk" "$two_pk_two_row_updated" "yes"
}

@test "index-on-writes: update partial two_pk, <, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 < 4 and pk2 < 9" "two_pk" "$two_pk_two_row_updated" "yes"
}

@test "index-on-writes: update partial two_pk, <=, pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 <= 3 and pk2 <= 8" "two_pk" "$two_pk_two_row_updated" "yes"
}

@test "index-on-writes: update partial two_pk, >, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 > 1 and pk2 > 6 and c1 = 129" "two_pk" "$two_pk_one_row_updated" "yes"
}

@test "index-on-writes: update partial two_pk, >=, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 >= 2 and pk2 >= 7 and c1 = 129" "two_pk" "$two_pk_one_row_updated" "yes"
}

@test "index-on-writes: update partial two_pk, <, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 < 4 and pk2 < 9 and c1 = 129" "two_pk" "$two_pk_one_row_updated" "yes"
}

@test "index-on-writes: update partial two_pk, <=, pk + non-pk" {
    test_mutation "update two_pk set c2 = 256 where pk1 <= 3 and pk2 <= 8 and c1 = 129" "two_pk" "$two_pk_one_row_updated" "yes"
}
