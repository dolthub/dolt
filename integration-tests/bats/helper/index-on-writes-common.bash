
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

test_mutation() {
    dml="$1"
    table="$2"
    expected="$3"
    uses_pk="$4"
    dolt sql -q "$dml"
    run dolt sql -q "select * from $table ORDER BY pk1" -r csv
    [ "$status" -eq "0" ]
    [ "$output" == "$expected" ] || (echo $output && exit 1)
    dolt reset --hard
    dolt sql --batch -q "$dml ; $dml"
    run dolt sql -q "select * from $table ORDER BY pk1" -r csv
    [ "$status" -eq "0" ]
    [ "$output" == "$expected" ] || (echo $output && exit 1)
    run dolt sql -q "explain plan $dml"
    [ "$status" -eq "0" ]
    if ! [ -z "$uses_pk" ]; then
        [[ "$output" =~ "IndexedTableAccess" ]] || exit 1
    else
        if [[ "$output" =~ "IndexedTableAccess" ]]; then exit 1; fi
    fi
}
