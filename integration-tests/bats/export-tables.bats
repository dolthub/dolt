#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE test_int (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
CREATE TABLE test_string (
  pk LONGTEXT NOT NULL,
  c1 LONGTEXT,
  c2 LONGTEXT,
  c3 LONGTEXT,
  c4 LONGTEXT,
  c5 LONGTEXT,
  PRIMARY KEY (pk)
);
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "export-tables: table export sql datetime" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT PRIMARY KEY,
  v1 DATE,
  v2 TIME,
  v3 YEAR,
  v4 DATETIME
);
INSERT INTO test VALUES
    (1,'2020-04-08','11:11:11','2020','2020-04-08 11:11:11'),
    (2,'2020-04-08','12:12:12','2020','2020-04-08 12:12:12');
SQL
    dolt table export test test.sql
    run cat test.sql
    [[ "$output" =~ "INSERT INTO \`test\` (\`pk\`,\`v1\`,\`v2\`,\`v3\`,\`v4\`) VALUES (1,'2020-04-08','11:11:11','2020','2020-04-08 11:11:11');" ]] || false
    [[ "$output" =~ "INSERT INTO \`test\` (\`pk\`,\`v1\`,\`v2\`,\`v3\`,\`v4\`) VALUES (2,'2020-04-08','12:12:12','2020','2020-04-08 12:12:12');" ]] || false
    dolt table export test test.json
    run cat test.json
    [ "$output" = '{"rows": [{"pk":1,"v1":"2020-04-08","v2":"11:11:11","v3":"2020","v4":"2020-04-08 11:11:11"},{"pk":2,"v1":"2020-04-08","v2":"12:12:12","v3":"2020","v4":"2020-04-08 12:12:12"}]}' ]
}

@test "export-tables: dolt table import from stdin export to stdout" {
    skiponwindows "Need to install python before this test will work."
    echo 'pk,c1,c2,c3,c4,c5
0,1,2,3,4,5
9,8,7,6,5,4
'|dolt table import -u test_int
    dolt table export --file-type=csv test_int|python -c '
import sys
rows = []
for line in sys.stdin:
    line = line.strip()

    if line != "":
        rows.append(line.strip().split(","))

if len(rows) != 3:
    sys.exit(1)

if rows[0] != "pk,c1,c2,c3,c4,c5".split(","):
    sys.exit(1)

if rows[1] != "0,1,2,3,4,5".split(","):
    sys.exit(1)

if rows[2] != "9,8,7,6,5,4".split(","):
    sys.exit(1)
'
}

@test "export-tables: dolt table export" {
    dolt sql -q "insert into test_int values (0, 1, 2, 3, 4, 5)"
    run dolt table export test_int export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f export.csv ]
    run grep 5 export.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    run dolt table export test_int export.csv
    [ "$status" -ne 0 ]
    [[ "$output" =~ "export.csv already exists" ]] || false
    run dolt table export -f test_int export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f export.csv ]
}

@test "export-tables: dolt table SQL export" {
    dolt sql -q "insert into test_int values (0, 1, 2, 3, 4, 5)"
    run dolt table export test_int export.sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f export.sql ]
    diff --strip-trailing-cr $BATS_TEST_DIRNAME/helper/1pk5col-ints.sql export.sql

    # string columns
    dolt sql -q "create table strings (a varchar(10) primary key, b char(10))"
    dolt sql -q "insert into strings values ('abc', '123'), ('def', '456')"
    dolt commit -am "Checkpoint"

    dolt table export strings -f export.sql
    dolt sql < export.sql

    run dolt status
    [[ "$output" =~ "working tree clean" ]] || false

    # enum columns
    dolt sql -q "create table enums (a varchar(10) primary key, b enum('one','two','three'))"
    dolt sql -q "insert into enums values ('abc', 'one'), ('def', 'two')"
    dolt commit -am "Checkpoint"

    dolt table export enums -f export.sql
    dolt sql < export.sql

    run dolt status

    [[ "$output" =~ "working tree clean" ]] || false

    # set columns
    dolt sql <<SQL
create table sets (a varchar(10) primary key, b set('one','two','three'));
insert into sets values ('abc', 'one,two'), ('def', 'two,three');
SQL
    
    dolt commit -am "Checkpoint"

    dolt table export sets -f export.sql
    dolt sql < export.sql

    run dolt status

    [[ "$output" =~ "working tree clean" ]] || false

    # json columns
    dolt sql -q "create table json_vals (a varchar(10) primary key, b json)"
    dolt sql <<SQL
    insert into json_vals values ('abc', '{"key": "value"}'), ('def', '[{"a": "b"},{"conjuction": "it\'s"}]');
SQL
    dolt commit -am "Checkpoint"

    dolt table export json_vals -f export.sql
    dolt sql < export.sql

    run dolt status

    [[ "$output" =~ "working tree clean" ]] || false    
}

@test "export-tables: broken SQL escaping" {
    dolt sql <<SQL
create table sets (a varchar(10) primary key, b set('one','two','three\'s'));
insert into sets values ('abc', 'one,two'), ('def', 'two,three\'s');
SQL
    
    dolt commit -am "Checkpoint"

    dolt table export sets -f export.sql
    
    skip "Export embeds single quote in string without escaping it https://github.com/dolthub/dolt/issues/2197"
   
    dolt sql < export.sql

    run dolt status

    [[ "$output" =~ "working tree clean" ]] || false    
}

@test "export-tables: SQL with foreign keys" {
    dolt sql <<SQL
create table one (a int primary key, b int);
create table two (c int primary key, d int);
insert into one values (1,1), (2,2);
insert into two values (1,1), (2,2);
alter table one add foreign key (b) references two (c);
alter table two add foreign key (d) references one (a);
SQL

    dolt commit -am "Added tables and data"
    
    dolt table export one one.sql
    dolt table export two two.sql
    
    dolt sql -b -q "set foreign_key_checks = 0; drop table one"
    dolt sql -b -q "set foreign_key_checks = 0; drop table two"

    echo -e "set foreign_key_checks = 0;\n$(cat one.sql)" > one_mod.sql

    dolt sql < one_mod.sql
    dolt sql < two.sql

    dolt table export one one_new.sql
    dolt table export two two_new.sql

    run diff one.sql one_new.sql
    [ "$status" -eq 0 ]
    [[ "$output" -eq "" ]] || false
    run diff two.sql two_new.sql
    [ "$status" -eq 0 ]
    [[ "$output" -eq "" ]] || false
}

@test "export-tables: export a table with a string with commas to csv" {
    run dolt sql -q "insert into test_string values ('tim', 'is', 'super', 'duper', 'rad', 'a,b,c,d,e')"
    [ "$status" -eq 0 ]
    run dolt table export test_string export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] ||  false
    grep -E \"a,b,c,d,e\" export.csv
}

@test "export-tables: export a table with a string with double quotes to csv" {
    run dolt sql -q 'insert into test_string (pk,c1,c5) values ("this", "is", "a ""quotation""");'
    [ "$status" -eq 0 ]
    run dolt table export test_string export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] ||  false
    grep '"a ""quotation"""' export.csv
}

@test "export-tables: export a table with a string with new lines to csv" {
    run dolt sql -q 'insert into test_string (pk,c1,c5) values ("this", "is", "a new \n line");'
    [ "$status" -eq 0 ]
    run dolt table export test_string export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] ||  false

    # output will be slit over two lines
    grep 'this,is,,,,"a new ' export.csv
    grep ' line"' export.csv
}

@test "export-tables: table with column with not null constraint can be exported and reimported" {
    dolt sql -q "CREATE TABLE person_info(name VARCHAR(255) NOT NULL,location VARCHAR(255) NOT NULL,age BIGINT NOT NULL,PRIMARY KEY (name));"
    dolt add .
    dolt commit -m 'add person_info table'
    dolt sql -q "INSERT INTO person_info (name, location, age) VALUES ('kevern smith', 'los angeles', 21);"
    dolt sql -q "INSERT INTO person_info (name, location, age) VALUES ('barbara smith', 'los angeles', 24);"

    # insert empty value in not null column
    dolt sql -q "INSERT INTO person_info (name, location, age) VALUES ('gary busy', '', 900);"
    dolt sql -q "INSERT INTO person_info (name, location, age) VALUES ('the tampa bay buccs', 'florida', 123);"
    dolt sql -q "INSERT INTO person_info (name, location, age) VALUES ('michael fakeperson', 'fake city', 39);"

    # create csvs
    dolt sql -r csv -q 'select * from person_info' > sql-csv.csv
    dolt table export person_info export-csv.csv
    dolt checkout person_info

    run dolt table import -u person_info sql-csv.csv
    [ "$status" -eq 0 ]
    run dolt table import -u person_info export-csv.csv
    [ "$status" -eq 0 ]
}

@test "export-tables: export a table with a json string to csv" {
    dolt sql -q "create table t2 (id int primary key, j JSON)"
    dolt sql -q "insert into t2 values (0, '[\"hi\"]')"

    run dolt table export t2 export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] ||  false

    # output will be split over two lines
    grep 'id,j' export.csv
    grep '"\[""hi""\]"' export.csv
}

@test "export-tables: uint schema parsing for writer_test.go backwards compatibility" {
    dolt sql -q "create table t2 (name text, age int unsigned, title text)"
    dolt sql -q "insert into t2 values ('Bill Billerson', 32, 'Senior Dufus')"

    run dolt table export t2 export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] ||  false

    # output will be split over two lines
    grep 'name,age,title' export.csv
    grep 'Bill Billerson,32,Senior Dufus' export.csv
}

@test "export-tables: exporting a table with datetimes can be reimported" {
   dolt sql -q "create table timetable(pk int primary key, time datetime)"
   dolt sql -q "insert into timetable values (1, '2021-06-02 15:37:24 +0000 UTC');"

   run dolt table export -f timetable export.csv
   [ "$status" -eq 0 ]
   [[ "$output" =~ "Successfully exported data." ]] ||  false

   # reimport the data
   dolt table rm timetable
   run dolt table import -c --pk=pk timetable export.csv
   [ "$status" -eq 0 ]

   run dolt sql -q "SELECT * FROM timetable" -r csv
   [[ "$output" =~ "1,2021-06-02 15:37:24 +0000 UTC" ]] ||  false
}

@test "export-tables: parquet file export check with parquet tools" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."
    dolt sql -q "CREATE TABLE test_table (pk int primary key, col1 text, col2 int);"
    dolt sql -q "INSERT INTO test_table VALUES (1, 'row1', 22), (2, 'row2', 33), (3, 'row3', 22);"

    run dolt table export -f test_table result.parquet
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f result.parquet ]

    run parquet-tools cat --json result.parquet > output.json
    [ "$status" -eq 0 ]
    row1='{"pk":1,"col1":"row1","col2":22}'
    row2='{"pk":2,"col1":"row2","col2":33}'
    row3='{"pk":3,"col1":"row3","col2":22}'
    [[ "$output" =~ "$row1" ]] || false
    [[ "$output" =~ "$row2" ]] || false
    [[ "$output" =~ "$row3" ]] || false
}

@test "export-tables: parquet file export compare pandas and pyarrow reads" {
    dolt sql -q "CREATE TABLE test_table (pk int primary key, col1 text, col2 int);"
    dolt sql -q "INSERT INTO test_table VALUES (1, 'row1', 22), (2, 'row2', 33), (3, 'row3', 22);"

    run dolt table export -f test_table result.parquet
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f result.parquet ]

    echo "import pandas as pd
df = pd.read_parquet('result.parquet')
print(df)
" > pandas.py
    run python3 pandas.py > pandas.txt
    [ -f pandas.txt ]

    echo "import pyarrow.parquet as pq
table = pq.read_table('result.parquet')
print(table.to_pandas())
" > arrow.py
    run python3 arrow.py > pyarrow.txt
    [ -f pyarrow.txt ]

    run diff pandas.txt pyarrow.txt
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "export-tables: table export datetime, bool, enum types to parquet" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."
    dolt sql <<SQL
CREATE TABLE diffTypes (
  pk BIGINT PRIMARY KEY,
  v1 DATE,
  v2 TIME,
  v3 YEAR,
  v4 DATETIME,
  v5 BOOL,
  v6 ENUM('one', 'two', 'three')
);
INSERT INTO diffTypes VALUES
    (1,'2020-04-08','11:11:11','2020','2020-04-08 11:11:11',true,'one'),
    (2,'2020-04-08','12:12:12','2020','2020-04-08 12:12:12',false,'three'),
    (3,'2021-10-09','04:12:34','2019','2019-10-09 04:12:34',true,NULL);
SQL
    run dolt table export diffTypes dt.parquet
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f dt.parquet ]

    run parquet-tools cat --json dt.parquet > output.json
    [ "$status" -eq 0 ]
    row1='{"pk":1,"v1":1586304000,"v2":40271000000,"v3":2020,"v4":1586344271,"v5":1,"v6":"one"}'
    row2='{"pk":2,"v1":1586304000,"v2":43932000000,"v3":2020,"v4":1586347932,"v5":0,"v6":"three"}'
    row3='{"pk":3,"v1":1633737600,"v2":15154000000,"v3":2019,"v4":1570594354,"v5":1}'
    [[ "$output" =~ "$row1" ]] || false
    [[ "$output" =~ "$row2" ]] || false
    [[ "$output" =~ "$row3" ]] || false
}


@test "export-tables: table export more types to parquet" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."
    dolt sql <<SQL
CREATE TABLE test (
  \`pk\` BIGINT NOT NULL,
  \`int\` BIGINT,
  \`string\` LONGTEXT,
  \`boolean\` BOOLEAN,
  \`float\` DOUBLE,
  \`uint\` BIGINT UNSIGNED,
  \`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin,
  PRIMARY KEY (pk)
);
SQL
    dolt table import -u test `batshelper 1pksupportedtypes.csv`

    run dolt table export test test.parquet
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f test.parquet ]

    run parquet-tools cat --json test.parquet > output.json
    [ "$status" -eq 0 ]
    row1='{"pk":0,"int":0,"string":"asdf","boolean":1,"float":0.0,"uint":0,"uuid":"00000000-0000-0000-0000-000000000000"}'
    row2='{"pk":1,"int":-1,"string":"qwerty","boolean":0,"float":-1.0,"uint":1,"uuid":"00000000-0000-0000-0000-000000000001"}'
    row3='{"pk":2,"int":1,"string":"","boolean":1,"float":0.0,"uint":0,"uuid":"123e4567-e89b-12d3-a456-426655440000"}'
    [[ "$output" =~ "$row1" ]] || false
    [[ "$output" =~ "$row2" ]] || false
    [[ "$output" =~ "$row3" ]] || false
}

@test "export-tables: table export decimal and bit types to parquet" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."
    dolt sql -q "CREATE TABLE more (pk BIGINT NOT NULL,v DECIMAL(9,5),b BIT(10),PRIMARY KEY (pk));"
    dolt sql -q "INSERT INTO more VALUES (1, 1234.56789, 511);"
    dolt sql -q "INSERT INTO more VALUES (2, 5235.66789, 514);"

    run dolt table export more more.parquet
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f more.parquet ]

    run parquet-tools cat --json more.parquet > output.json
    [ "$status" -eq 0 ]
    row1='{"pk":1,"v":1234.57,"b":511}'
    row2='{"pk":2,"v":5235.67,"b":514}'
    [[ "$output" =~ "$row1" ]] || false
    [[ "$output" =~ "$row2" ]] || false
}

@test "export-tables: table export to sql with null values in different sql types" {
    dolt sql <<SQL
CREATE TABLE s (stringVal VARCHAR(6));
INSERT INTO s VALUES ('value'), (null);
CREATE TABLE i (intVal integer);
INSERT INTO s VALUES (2), (null);
SQL

    run dolt sql -q "SELECT * FROM s"
    string_output=$output

    run dolt table export s s.sql
    [ $status -eq 0 ]

    dolt table rm s
    run dolt sql < s.sql
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM s"
    [ "$output" = "$string_output" ]

    run dolt sql -q "SELECT * FROM i"
    int_output=$output

    run dolt table export i i.sql
    [ $status -eq 0 ]

    dolt table rm i
    run dolt sql < i.sql
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM i"
    [ "$output" = "$int_output" ]
}
