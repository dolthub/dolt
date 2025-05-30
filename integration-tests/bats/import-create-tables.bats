#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    cat <<DELIM > 1pk5col-ints.csv
pk,c1,c2,c3,c4,c5
0,1,2,3,4,5
1,1,2,3,4,5
DELIM

    cat <<DELIM > empty-strings-null-values.csv
pk,headerOne,headerTwo
a,"""""",1
b,"",2
c,,3
d,row four,""
e,row five,
f,row six,6
g, ,
DELIM

    cat <<JSON > name-map.json
{
    "one":"pk",
    "two":"c1",
    "three":"c2",
    "four":"c3"
}
JSON

    cat <<DELIM > name-map-data.csv
one,two,three,four
0,1,2,3
DELIM

    cat <<SQL > name-map-sch.sql
CREATE TABLE test (
    pk int not null,
    c1 float,
    c2 float,
    c3 float,
    primary key(pk)
);
SQL

    cat <<DELIM > people.csv
pk,first,last,age,street,city,state,zip,dollar,color,date
1,Oscar,Rodgers,38,Zapib View,Vervutce,OH,03020,$1200.09,RED,11/12/1928
2,Estella,Cannon,33,Kubta Manor,Tocunuz,OH,04943,$1296.25,YELLOW,03/05/2016
3,Dora,Stanley,27,Bidohe Boulevard,Siguhazep,CA,53768,$9744.06,WHITE,07/31/1993
4,Brian,Newman,41,Koef Court,Abemivu,OH,44534,$3808.15,YELLOW,03/29/2064
DELIM

}

teardown() {
    assert_feature_version
    teardown_common
}

@test "import-create-tables: create a table with json import" {
    run dolt table import -c -s `batshelper employees-sch.sql` employees `batshelper employees-tbl.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "employees" ]] || false
    run dolt sql -q "select * from employees"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tim" ]] || false
    [ "${#lines[@]}" -eq 7 ]
}

@test "import-create-tables: create a table with json import, utf8 with bom" {
    run dolt table import -c -s `batshelper employees-sch.sql` employees `batshelper employees-tbl.utf8bom.json`
    echo "$output"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "employees" ]] || false
    run dolt sql -q "select * from employees"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tim" ]] || false
    [ "${#lines[@]}" -eq 7 ]
}

@test "import-create-tables: create a table with json import, utf16le with bom" {
    run dolt table import -c -s `batshelper employees-sch.sql` employees `batshelper employees-tbl.utf16lebom.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "employees" ]] || false
    run dolt sql -q "select * from employees"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tim" ]] || false
    [ "${#lines[@]}" -eq 7 ]
}

@test "import-create-tables: create a table with json import, utf16be with bom" {
    run dolt table import -c -s `batshelper employees-sch.sql` employees `batshelper employees-tbl.utf16bebom.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "employees" ]] || false
    run dolt sql -q "select * from employees"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tim" ]] || false
    [ "${#lines[@]}" -eq 7 ]
}

@test "import-create-tables: create a table with json import. no schema." {
    run dolt table import -c employees `batshelper employees-tbl.json`
    [ "$status" -ne 0 ]
    [ "$output" = "Please specify schema file for .json tables." ]
}

@test "import-create-tables: create a table with json data import. bad json data." {
    run dolt table import -c -s `batshelper employees-sch.sql` employees `batshelper employees-tbl-bad.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cause: invalid character after object key:value pair: 'b'" ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "employees" ]] || false
}

@test "import-create-tables: create a table with json import. bad schema." {
    run dolt table import -c -s `batshelper employees-sch-bad.sql` employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error creating reader for json file" ]] || false
    [[ "$output" =~ "employees-tbl.json" ]] || false
    [[ "$output" =~ "employees-sch-bad.sql" ]] || false
}

@test "import-create-tables: import data from csv and create the table" {
    run dolt table import -c --pk=pk test 1pk5col-ints.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
}

@test "import-create-tables: import data from csv and create the table different types" {
    run dolt table import -c --pk=pk test people.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    # Sanity Check
    ! [[ "$output" =~ "Warning: The import file's schema does not match the table's schema" ]] || false

    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
}

@test "import-create-tables: use -f to overwrite data in existing table" {
    cat <<DELIM > other.csv
pk,c1,c2,c3,c4,c5
8,1,2,3,4,5
9,1,2,3,4,5
DELIM
    dolt table import -c --pk=pk test 1pk5col-ints.csv
    run dolt table import -c --pk=pk test 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "test already exists. Use -f to overwrite." ]] || false
    run dolt table import -f -c --pk=pk test other.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt sql -r csv -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "pk,c1,c2,c3,c4,c5" ]
    [ "${lines[1]}" = "8,1,2,3,4,5" ]
    [ "${lines[2]}" = "9,1,2,3,4,5" ]
    [ ! "${lines[1]}" = "0,1,2,3,4,5" ]
    [ ! "${lines[2]}" = "1,1,2,3,4,5" ]
}

@test "import-create-tables: use -f to overwrite data in existing table with fk constraints" {
    cat <<DELIM > other.csv
pk,c1,c2,c3,c4,c5
8,1,2,3,4,5
9,1,2,3,4,5
DELIM
    dolt table import -c --pk=pk test 1pk5col-ints.csv
    run dolt sql -q "create table fktest(id int not null, tpk int, c2 int, primary key(id), foreign key (tpk) references test(pk))"
    [ "$status" -eq 0 ]
    run dolt sql -q "insert into fktest values (1, 0, 1)"
    [ "$status" -eq 0 ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    [[ "$output" =~ "fktest" ]] || false
    run dolt table import -c --pk=id fktest 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fktest already exists. Use -f to overwrite." ]] || false
    run dolt table import -c -f --pk=pk fktest other.csv
    [ "$status" -eq 0 ]

    run dolt schema show
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "FOREIGN KEY" ]] || false
}

@test "import-create-tables: try to create a table with a bad csv" {
    run dolt table import -c --pk=pk test `batshelper bad.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error creating reader" ]] || false
}

@test "import-create-tables: try to create a table with duplicate column names" {
    cat <<CSV > duplicate-names.csv
pk,abc,Abc
1,2,3
4,5,6
CSV
    
    run dolt table import -c --pk=pk test duplicate-names.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "name" ]] || false
    [[ "$output" =~ "invalid schema" ]] || false
}

@test "import-create-tables: try to create a table with dolt table import with a bad file name" {
    run dolt table import -c test `batshelper bad.data`
    [ "$status" -eq 1 ]
}

@test "import-create-tables: try to create a table with dolt table import with invalid name" {
    run dolt table import -c --pk=pk dolt_docs 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
    run dolt table import -c --pk=pk dolt_query_catalog 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
    run dolt table import -c --pk=pk dolt_reserved 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
}

@test "import-create-tables: try to table import with nonexistent --pk arg" {
    run dolt table import -c -pk="batmansparents" test 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error determining the output schema." ]] || false
    [[ "$output" =~ "column 'batmansparents' not found" ]] || false
}

@test "import-create-tables: try to table import with one valid and one nonexistent --pk arg" {
    run dolt table import -c -pk="pk,batmansparents" test 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error determining the output schema." ]] || false
    [[ "$output" =~ "column 'batmansparents' not found" ]] || false
}

@test "import-create-tables: create a table with two primary keys from csv import" {
    run dolt table import -c --pk=pk1,pk2 test `batshelper 2pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt sql -q 'select count(*) from test'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "4" ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
}

@test "import-create-tables: import data from psv and create the table" {
    cat <<DELIM > 1pk5col-ints.psv
pk|c1|c2|c3|c4|c5
0|1|2|3|4|5
1|1|2|3|4|5
DELIM
    run dolt table import -c --pk=pk test 1pk5col-ints.psv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
}

@test "import-create-tables: import table using --delim" {
    cat <<DELIM > 1pk5col-ints.csv
pk||c1||c2||c3||c4||c5
0||1||2||3||4||5
1||1||2||3||4||5
DELIM
    run dolt table import -c -pk=pk --delim="||" test 1pk5col-ints.csv
    [ "$status" -eq 0 ]
    run dolt sql -r csv -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "pk,c1,c2,c3,c4,c5" ]
    [ "${lines[1]}" = "0,1,2,3,4,5" ]
    [ "${lines[2]}" = "1,1,2,3,4,5" ]
}

@test "import-create-tables: create a table with a name map" {
    run dolt table import -c -pk=pk -m=name-map.json test name-map-data.csv
    [ "$status" -eq 0 ]
    run dolt sql -r csv -q 'select * from test'
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "pk,c1,c2,c3" ]
    [ "${lines[1]}" = "0,1,2,3" ]
    run dolt schema export test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "import-create-tables: use a name map with missing and extra entries" {
    cat <<JSON > partial-map.json
{
    "one":"pk",
    "ten":"c10"
}
JSON
    run dolt table import -c -pk=pk -m=partial-map.json test name-map-data.csv
    [ "$status" -eq 0 ]
    run dolt schema export test
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "c10" ]] || false
    [[ "${lines[1]}" =~ "pk" ]] || false
    [[ "${lines[2]}" =~ "two" ]] || false
    [[ "${lines[3]}" =~ "three" ]] || false
    [[ "${lines[4]}" =~ "four" ]] || false
}

@test "import-create-tables: create a table with a schema file" {
    cat <<DELIM > sch-data.csv
pk,c1,c2,c3
0,1,2,3
DELIM
    run dolt table import -c -s=name-map-sch.sql test sch-data.csv
    [ "$status" -eq 0 ]
    run dolt sql -r csv -q 'select * from test'
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "pk,c1,c2,c3" ]
    [ "${lines[1]}" = "0,1,2,3" ]
    run dolt schema export test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "\`c1\` float" ]] || false
    [[ "$output" =~ "\`c2\` float" ]] || false
    [[ "$output" =~ "\`c3\` float" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "import-create-tables: create a table with a name map and a schema file" {
    run dolt table import -c -s=name-map-sch.sql -m=name-map.json test name-map-data.csv
    [ "$status" -eq 0 ]
    run dolt sql -r csv -q 'select * from test'
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "pk,c1,c2,c3" ]
    [ "${lines[1]}" = "0,1,2,3" ]
    run dolt schema export test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "\`c1\` float" ]] || false
    [[ "$output" =~ "\`c2\` float" ]] || false
    [[ "$output" =~ "\`c3\` float" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "import-create-tables: create a table from CSV with common column name patterns" {
    run dolt table import -c --pk=UPPERCASE test `batshelper caps-column-names.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt sql -r csv -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "UPPERCASE" ]] || false
}

@test "import-create-tables: create a table from excel import with multiple sheets" {
    run dolt table import -c --pk=id employees `batshelper employees.xlsx`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "employees" ]] || false
    run dolt sql -q "select * from employees"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tim" ]] || false
    [ "${#lines[@]}" -eq 7 ]
    run dolt table import -c --pk=number basketball `batshelper employees.xlsx`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "employees" ]] || false
    [[ "$output" =~ "basketball" ]] || false
    run dolt sql -q "select * from basketball"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tim" ]] || false
    [ "${#lines[@]}" -eq 8 ]
}

@test "import-create-tables: specify incorrect sheet name on excel import" {
    run dolt table import -c --pk=id bad-sheet-name `batshelper employees.xlsx`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table name must match excel sheet name" ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "bad-sheet-name" ]] || false
}

@test "import-create-tables: import an .xlsx file that is not a valid excel spreadsheet" {
    run dolt table import -c --pk=id test `batshelper bad.xlsx`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid xlsx file" ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false
}

@test "import-create-tables: import and create table with non UTF-8 characters in it" {
    skiponwindows "windows can't find bad-characters.csv"
    run dolt table import -c --pk=pk test `batshelper bad-characters.csv`
    [ "$status" -eq 1 ]
}

@test "import-create-tables: import and update table with non UTF-8 characters in it" {
    skiponwindows "windows can't find bad-characters.csv"
    dolt sql -q "create table test (pk int primary key, c1 blob);"
    run dolt table import -u --pk=pk test `batshelper bad-characters.csv`
    [ "$status" -eq 0 ]
    dolt sql -q 'select * from test'
    dolt sql -r csv -q 'select * from test' > compare.csv
    diff compare.csv `batshelper bad-characters.csv`
}

@test "import-create-tables: dolt diff on a newly created table" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "diff --dolt a/test b/test" ]] || false
    [[ "$output" =~ "added table" ]] || false
}

@test "import-create-tables: create a table with null values from csv import" {
    run dolt table import -c -pk=pk test empty-strings-null-values.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt sql -q "select * from test ORDER BY pk"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [ "${lines[3]}" = '| a  | ""        | 1         |' ]
    [ "${lines[4]}" = '| b  |           | 2         |' ]
    [ "${lines[5]}" = "| c  | NULL      | 3         |" ]
    [ "${lines[6]}" = '| d  | row four  | NULL      |' ]
    [ "${lines[7]}" = "| e  | row five  | NULL      |" ]
    [ "${lines[8]}" = "| f  | row six   | 6         |" ]
    [ "${lines[9]}" = "| g  | NULL      | NULL      |" ]
}

@test "import-create-tables: table import with schema different from data file" {
    cat <<SQL > schema.sql
CREATE TABLE subset (
    pk INT NOT NULL,
    c1 INT,
    c3 INT,
    noData INT,
    PRIMARY KEY (pk)
);
SQL
    run dolt table import -s schema.sql -c subset 1pk5col-ints.csv
    [ "$status" -eq 0 ]

    # schema argument subsets the data and adds empty column
    run dolt sql -r csv -q "select * from subset ORDER BY pk"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "pk,c1,c3,noData" ]
    [ "${lines[1]}" = "0,1,3," ]
    [ "${lines[2]}" = "1,1,3," ]
}

@test "import-create-tables: create a table with null values from csv import with json file" {
    cat <<SQL > schema.sql
CREATE TABLE empty_strings_null_values (
    pk VARCHAR(120) NOT NULL COMMENT 'tag:0',
    headerOne VARCHAR(120) COMMENT 'tag:1',
    headerTwo VARCHAR(120) COMMENT 'tag:2',
    PRIMARY KEY (pk)
);
SQL
    run dolt table import -c -s schema.sql empty_strings_null_values empty-strings-null-values.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "empty_strings_null_values" ]] || false
    run dolt sql -q "select * from empty_strings_null_values ORDER BY pk"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [ "${lines[3]}" = '| a  | ""        | 1         |' ]
    [ "${lines[4]}" = '| b  |           | 2         |' ]
    [ "${lines[5]}" = "| c  | NULL      | 3         |" ]
    [ "${lines[6]}" = "| d  | row four  |           |" ]
    [ "${lines[7]}" = "| e  | row five  | NULL      |" ]
    [ "${lines[8]}" = "| f  | row six   | 6         |" ]
    [ "${lines[9]}" = "| g  | NULL      | NULL      |" ]
}

@test "import-create-tables: create a table with null values from json import with json file" {
    dolt sql <<SQL
CREATE TABLE test (
  pk varchar(20) NOT NULL,
  headerOne LONGTEXT,
  headerTwo BIGINT,
  PRIMARY KEY (pk)
);
SQL
    run dolt table import -u test `batshelper empty-strings-null-values.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt sql -q "select * from test ORDER BY pk"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [ "${lines[3]}" = '| a  | ""        | 1         |' ]
    [ "${lines[4]}" = '| b  |           | 2         |' ]
    [ "${lines[5]}" = "| c  | NULL      | 3         |" ]
    [ "${lines[6]}" = "| d  | row four  | NULL      |" ]
    [ "${lines[7]}" = "| e  | row five  | NULL      |" ]
    [ "${lines[8]}" = "| f  | row six   | 6         |" ]
    [ "${lines[9]}" = "| g  | NULL      | NULL      |" ]
}

@test "import-create-tables: fail to create a table with null values from json import with json file" {
    dolt sql <<SQL
CREATE TABLE test (
  pk varchar(20) NOT NULL,
  headerOne LONGTEXT NOT NULL,
  headerTwo BIGINT NOT NULL,
  PRIMARY KEY (pk)
);
SQL
    run dolt table import -u test `batshelper empty-strings-null-values.json`
    [ "$status" -eq 1 ]
}

@test "import-create-tables: fail on import table creation when defined pk has a NULL value" {
    cat <<DELIM > null-pk-1.csv
pk,v1
"a",1
,2
DELIM
    cat <<DELIM > null-pk-2.csv
pk1,pk2,v1
0,0,0
1,,1
DELIM
    run dolt table import -c --pk=pk test null-pk-1.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "pk" ]] || false
    run dolt table import -c --pk=pk1,pk2 test null-pk-2.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "pk2" ]] || false
}

@test "import-create-tables: table import -c infers types from data" {
    cat <<DELIM > types.csv
pk,str,int,bool,float, date, time, datetime
0,abc,123,false,3.14,2020-02-02,12:12:12.12,2020-02-02 12:12:12
DELIM
    run dolt table import -c --pk=pk test types.csv
    [ "$status" -eq 0 ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`str\` varchar(200)" ]] || false
    [[ "$output" =~ "\`int\` int" ]] || false
    [[ "$output" =~ "\`bool\` tinyint" ]] || false
    [[ "$output" =~ "\`float\` float" ]] || false
    [[ "$output" =~ "\`date\` date" ]] || false
    [[ "$output" =~ "\`time\` time" ]] || false
    [[ "$output" =~ "\`datetime\` datetime" ]] || false
}

@test "import-create-tables: table import -c collects garbage" {
    echo "pk" > pk.csv
    seq 0 100000 >> pk.csv

    run dolt table import -c -pk=pk test pk.csv
    [ "$status" -eq 0 ]

    # assert that we already collected garbage
    BEFORE=$(du -c .dolt/noms/ | grep total | sed 's/[^0-9]*//g')
    dolt gc
    AFTER=$(du -c .dolt/noms/ | grep total | sed 's/[^0-9]*//g')

    skip "chunk journal doesn't shrink"
    # less than 10% smaller
    [ "$BEFORE" -lt $(($AFTER * 11 / 10)) ]
}

@test "import-create-tables: table import -c --continue logs bad rows" {
    cat <<DELIM > 1pk5col-rpt-ints.csv
pk,c1,c2,c3,c4,c5
1,1,2,3,4,5
1,1,2,3,4,7
1,1,2,3,4,8
DELIM

    run dolt table import -c --continue --pk=pk test 1pk5col-rpt-ints.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "The following rows were skipped:" ]] || false
    [[ "$output" =~ "1,1,2,3,4,7" ]] || false
    [[ "$output" =~ "1,1,2,3,4,8" ]] || false
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Lines skipped: 2" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "import-create-tables: csv files has less columns than -s schema" {
    cat <<SQL > schema.sql
CREATE TABLE subset (
    pk INT NOT NULL,
    c1 INT,
    c3 INT,
    PRIMARY KEY (pk)
);
SQL
     cat <<DELIM > data.csv
pk,c3
0,2
DELIM

    run dolt table import -s schema.sql -c subset data.csv
    [ "$status" -eq 0 ]

    # schema argument subsets the data and adds empty column
    run dolt sql -r csv -q "select * from subset ORDER BY pk"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "0,,2" ]
}

@test "import-create-tables: csv files has more columns than -s schema" {
    cat <<SQL > schema.sql
CREATE TABLE subset (
    pk INT NOT NULL,
    c1 INT,
    c2 INT,
    c3 INT,
    PRIMARY KEY (pk)
);
SQL
    cat <<DELIM > data.csv
pk,c3,c1,c2,c4
0,3,1,2,4
DELIM

    run dolt table import -s schema.sql -c subset data.csv
    [ "$status" -eq 0 ]

    # schema argument subsets the data and adds empty column
    run dolt sql -r csv -q "select * from subset ORDER BY pk"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "0,1,2,3" ]
}

@test "import-create-tables: csv files has equal columns but different order than -s schema" {
    cat <<SQL > schema.sql
CREATE TABLE subset (
    pk INT NOT NULL,
    c1 INT,
    c2 INT,
    PRIMARY KEY (pk)
);
SQL
    cat <<DELIM > data.csv
pk,c2,c1
0,2,1
DELIM

    run dolt table import -s schema.sql -c subset data.csv
    [ "$status" -eq 0 ]

    # schema argument subsets the data and adds empty column
    run dolt sql -r csv -q "select * from subset ORDER BY pk"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "0,1,2" ]
}

@test "import-create-tables: csv files has fewer columns filled with default value" {
    cat <<SQL > schema.sql
CREATE TABLE subset (
    pk INT NOT NULL,
    c1 INT DEFAULT 42,
    c2 INT,
    PRIMARY KEY (pk)
);
SQL
     cat <<DELIM > data.csv
pk,c2
0,2
DELIM

    run dolt table import -s schema.sql -c subset data.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Warning: The import file's schema does not match the table's schema" ]] || false

    # schema argument subsets the data and adds empty column
    run dolt sql -r csv -q "select * from subset ORDER BY pk"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "0,42,2" ]
}

@test "import-create-tables: keyless table import" {
    cat <<SQL > schema.sql
CREATE TABLE keyless (
    c0 INT,
    c1 INT DEFAULT 42,
    c2 INT
);
SQL

    cat <<DELIM > data.csv
c0,c2
0,2
DELIM

    run dolt table import -s schema.sql -c keyless data.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -r csv -q "select * from keyless"
    [ "${lines[1]}" = "0,42,2" ]
}

@test "import-create-tables: auto-increment table" {
    cat <<SQL > schema.sql
CREATE TABLE test (
    pk int PRIMARY KEY AUTO_INCREMENT,
    v1 int
);
SQL

    cat <<DELIM > data.csv
pk,v1
1,1
2,2
3,3
4,4
DELIM

    run dolt table import -s schema.sql -c test data.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 4, Additions: 4, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -r csv -q "select * from test order by pk ASC"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [ "${lines[1]}" = 1,1 ]
    [ "${lines[2]}" = 2,2 ]
    [ "${lines[3]}" = 3,3 ]
    [ "${lines[4]}" = 4,4 ]

    dolt sql -q "insert into test values (NULL, 5)"

    run dolt sql -r csv -q "select * from test where pk = 5"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [ "${lines[1]}" = 5,5 ]
}

@test "import-create-tables: --quiet correctly prevents skipped rows from printing" {
    cat <<DELIM > 1pk5col-rpt-ints.csv
pk,c1,c2,c3,c4,c5
1,1,2,3,4,5
1,1,2,3,4,7
1,1,2,3,4,8
DELIM

    run dolt table import -c --continue --quiet --pk=pk test 1pk5col-rpt-ints.csv
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "The following rows were skipped:" ]] || false
    ! [[ "$output" =~ "1,1,2,3,4,7" ]] || false
    ! [[ "$output" =~ "1,1,2,3,4,8" ]] || false
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Lines skipped: 2" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    dolt sql -q "drop table test"
    
    # --ignore-skipped-rows is an alias for --quiet
    run dolt table import -c --continue --ignore-skipped-rows --pk=pk test 1pk5col-rpt-ints.csv
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "The following rows were skipped:" ]] || false
    ! [[ "$output" =~ "1,1,2,3,4,7" ]] || false
    ! [[ "$output" =~ "1,1,2,3,4,8" ]] || false
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Lines skipped: 2" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

}

@test "import-create-tables: created table with force option can be added and committed as modified" {
    run dolt table import -c --pk=id test `batshelper jails.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt add test
    [ "$status" -eq 0 ]
    run dolt commit -m 'added table test'
    [ "$status" -eq 0 ]
    run dolt table import -c -f --pk=state test `batshelper states.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt add test
    [ "$status" -eq 0 ]
    run dolt commit -m 'modified table test'
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "On branch main" ]
    [ "${lines[1]}" = "nothing to commit, working tree clean" ]
}

@test "import-create-tables: import null foreign key value does not violate constraint" {
    cat <<DELIM > test.csv
id, state_id, data
1,,poop
DELIM

    dolt sql <<SQL
CREATE TABLE states (
  id int NOT NULL,
  abbr char(2),
  PRIMARY KEY (id)
);
CREATE TABLE data (
  id int NOT NULL,
  state_id int,
  data varchar(500),
  PRIMARY KEY (id),
  KEY state_id (state_id),
  CONSTRAINT d4jibcjf FOREIGN KEY (state_id) REFERENCES states (id)
);
SQL

    run dolt sql -q "insert into data values (0, NULL, 'poop')"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from data"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 0  | NULL     | poop |" ]] || false

    run dolt table import -u data test.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt sql -q "select * from data"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 0  | NULL     | poop |" ]] || false
    [[ "$output" =~ "| 1  | NULL     | poop |" ]] || false
}

@test "import-create-tables: --all-text imports all columns as text" {
    cat <<DELIM >test.csv
id, state, data
1,WA,"{""a"":1,""b"":""value""}"
DELIM

    run dolt table import -c --all-text --pk=id test test.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -q "describe test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| id    | varchar(200) |" ]] || false
    [[ "$output" =~ "| state | text         |" ]] || false
    [[ "$output" =~ "| data  | text         |" ]] || false

    # pk defaults to first column if not explicitly defined
    run dolt table import -c --all-text test2 test.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -q "describe test2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| id    | varchar(200) |" ]] || false
    [[ "$output" =~ "| state | text         |" ]] || false
    [[ "$output" =~ "| data  | text         |" ]] || false
}

@test "import-create-tables: --all-text and --schema are mutually exclusive" {
    run dolt table import -c -s `batshelper employees-sch.sql` --all-text employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "parameters all-text and schema are mutually exclusive" ]] || false
}

@test "import-create-tables: import from pre-existing parquet table" {
  # The file strings.parquet uses a different name for the root column than the one generated by `dolt table export`,
  # but Dolt should still be able to import it.
  run dolt table import -c -s `batshelper parquet/strings.sql` strings `batshelper parquet/strings.parquet`
  [ "$status" -eq 0 ]

  dolt sql -r csv -q "select * from strings;"
  run dolt sql -r csv -q "select * from strings;"
  [ "$status" -eq 0 ]
  [ "${#lines[@]}" -eq 3 ]
  [[ "$output" =~ "text" ]] || false
  [[ "$output" =~ "hello foo" ]] || false
  [[ "$output" =~ "hello world" ]] || false
}

@test "import-create-tables: import sequences as JSON arrays" {
  # The file strings.parquet uses a different name for the root column than the one generated by `dolt table export`,
  # but Dolt should still be able to import it.
  run dolt table import -c -s `batshelper parquet/sequences.sql` sequences `batshelper parquet/sequences.parquet`
  [ "$status" -eq 0 ]

  dolt sql -r csv -q "select * from sequences;"
  run dolt sql -r csv -q "select * from sequences;"
  [ "$status" -eq 0 ]
  [ "${#lines[@]}" -eq 7 ]
  [[ "$output" =~ '1,empty,[]' ]] || false
  [[ "$output" =~ "2,single,[1]" ]] || false
  [[ "$output" =~ "3,null," ]] || false
  [[ "$output" =~ '4,double,"[2,3]"' ]] || false
  [[ "$output" =~ '5,contains null,"[4,null]"' ]] || false
  [[ "$output" =~ '6,empty,[]' ]] || false

}