# criss_cross makes a repository in the directory criss-cross, changes to it, and makes branches branchA and branchB
# with two merge bases, baseA and baseB. It sets $BASE_A and $BASE_B to their hashes.
#
#                -- baseA -- mergeA <-- branchA
#              /        \  /
#  initial <-- main      \/
#              \         /\
#                -- baseB -- mergeB <-- branchB
#
# baseA adds row 10 and baseB adds row 20 to table t. mergeA merges baseB, and mergeB merges baseA. baseB is committed
# after baseA, so it is the newest merge base.
criss_cross() {
    mkdir criss-cross
    cd criss-cross
    dolt init
    dolt sql -q "CREATE TABLE t (pk INT PRIMARY KEY);"
    dolt add -A
    dolt commit -m "initial"

    dolt checkout -b branchA
    dolt sql -q "INSERT INTO t VALUES (10);"
    dolt commit -am "baseA"
    BASE_A=$(head_hash)

    dolt checkout -b branchB main
    dolt sql -q "INSERT INTO t VALUES (20);"
    # Make baseB's commit date later than baseA's.
    sleep 1
    dolt commit -am "baseB"
    BASE_B=$(head_hash)

    dolt merge "$BASE_A" -m "mergeB"
    dolt checkout branchA
    dolt merge "$BASE_B" -m "mergeA"
}

head_hash() {
    dolt sql -r csv -q "SELECT hashof('HEAD');" | tail -n 1
}
