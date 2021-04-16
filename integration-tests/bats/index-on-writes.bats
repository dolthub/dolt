#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/index-on-writes-common.bash

setup() {
    setup_common
    create_tables
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "index-on-writes: delete all two_pk" {
    test_mutation "delete from two_pk" "two_pk" "$two_pk_header"
}

@test "index-on-writes: delete all one_pk" {
    test_mutation "delete from one_pk" "one_pk" "$one_pk_header"
}

@test "index-on-writes: delete all two_pk, <>, pk" {
    test_mutation "delete from two_pk where pk1 <> 1024 and pk2 <> 1024" "two_pk" "$two_pk_header"  "yes"
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
    test_mutation "delete from two_pk where pk1 <> 1024 and pk2 <> 1024 and c1 <> 1024" "two_pk" "$two_pk_header" "yes"
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
    test_mutation "update two_pk set c2 = 256 where pk1 <> 1024 and pk2 <> 1024" "two_pk" "$two_pk_all_updated" "yes"
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
    test_mutation "update two_pk set c2 = 256 where pk1 <> 1024 and pk2 <> 1024 and c1 <> 1024" "two_pk" "$two_pk_all_updated" "yes"
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
