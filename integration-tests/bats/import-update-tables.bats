#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    cat <<SQL > 1pk5col-ints-sch.sql
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL

    cat <<SQL > 1pk1col-char-sch.sql
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c CHAR(5) COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL

    cat <<DELIM > 1pk5col-ints.csv
pk,c1,c2,c3,c4,c5
0,1,2,3,4,5
1,1,2,3,4,5
DELIM

    cat <<DELIM > 1pk5col-ints-updt.csv
pk,c1,c2,c3,c4,c5
0,1,2,3,4,6
DELIM

    cat <<SQL > employees-sch.sql
CREATE TABLE employees (
  \`id\` varchar(20) NOT NULL COMMENT 'tag:0',
  \`first name\` LONGTEXT COMMENT 'tag:1',
  \`last name\` LONGTEXT COMMENT 'tag:2',
  \`title\` LONGTEXT COMMENT 'tag:3',
  \`start date\` LONGTEXT COMMENT 'tag:4',
  \`end date\` LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (id)
);
SQL

  cat <<SQL > check-constraint-sch.sql
CREATE TABLE persons (
    ID int PRIMARY KEY,
    LastName varchar(255) NOT NULL,
    FirstName varchar(255),
    Age int CHECK (Age>=18)
);
SQL

    cat <<SQL > nibrs_month_sch.sql
CREATE TABLE \`test\` (
  \`STATE_ID\` smallint NOT NULL,
  \`NIBRS_MONTH_ID\` bigint NOT NULL,
  \`AGENCY_ID\` bigint NOT NULL,
  \`AGENCY_TABLE_TYPE_ID\` smallint NOT NULL,
  \`MONTH_NUM\` smallint NOT NULL,
  \`DATA_YEAR\` smallint NOT NULL,
  \`REPORTED_STATUS\` varchar(1) collate utf8mb4_0900_ai_ci,
  \`REPORT_DATE\` timestamp,
  \`UPDATE_FLAG\` char(1) collate utf8mb4_0900_ai_ci,
  \`ORIG_FORMAT\` char(1) collate utf8mb4_0900_ai_ci,
  \`DATA_HOME\` varchar(1) collate utf8mb4_0900_ai_ci,
  \`DDOCNAME\` varchar(50) collate utf8mb4_0900_ai_ci,
  \`DID\` bigint,
  \`MONTH_PUB_STATUS\` int,
  \`INC_DATA_YEAR\` int,
  PRIMARY KEY (\`STATE_ID\`,\`NIBRS_MONTH_ID\`,\`DATA_YEAR\`),
  KEY \`AGENCY_TABLE_TYPE_ID\` (\`AGENCY_TABLE_TYPE_ID\`),
  KEY \`DATA_YEAR_INDEX\` (\`DATA_YEAR\`),
  KEY \`NIBRS_MONTH_ID_INDEX\` (\`NIBRS_MONTH_ID\`),
  KEY \`STATE_ID_INDEX\` (\`STATE_ID\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SQL

    cat <<CSV > nibrs_month_csv.csv
INC_DATA_YEAR,NIBRS_MONTH_ID,AGENCY_ID,MONTH_NUM,DATA_YEAR,REPORTED_STATUS,REPORT_DATE,UPDATE_FLAG,ORIG_FORMAT,DATA_HOME,DDOCNAME,DID,MONTH_PUB_STATUS,STATE_ID,AGENCY_TABLE_TYPE_ID
2019,9128595,9305,3,2019,I,2019-07-18,Y,F,C,2019_03_MN0510000_NIBRS,49502383,0,27,2
CSV

    dolt sql <<SQL
CREATE TABLE colors (
    id INT NOT NULL,
    color VARCHAR(32) NOT NULL,

    PRIMARY KEY (id),
    INDEX color_index(color)
);
CREATE TABLE objects (
    id INT NOT NULL,
    name VARCHAR(64) NOT NULL,
    color VARCHAR(32),

    PRIMARY KEY(id),
    FOREIGN KEY (color) REFERENCES colors(color)
);
INSERT INTO colors (id,color) VALUES (1,'red'),(2,'green'),(3,'blue'),(4,'purple');
INSERT INTO objects (id,name,color) VALUES (1,'truck','red'),(2,'ball','green'),(3,'shoe','blue');
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "import-update-tables: distinguish between empty string and null for SETs" {
    dolt sql <<SQL
create table word(pk int primary key, letters set('', 'a', 'b'));
SQL
    dolt commit -Am "add a table"

    expected=$(cat <<DELIM
pk,letters
1,"a,b"
2,a
3,""
4,
DELIM
)
    echo "$expected" > word_data.csv

    run dolt table import -u word word_data.csv
    [ $status -eq 0 ]
    [[ "$output" =~ "Rows Processed: 4, Additions: 4, Modifications: 0, Had No Effect: 0" ]] || false

    run dolt sql -r csv -q "select * from word;"
    echo "OUTPUT: $output \n"
    [ $status -eq 0 ]
    [[ "$output" = "$expected" ]] || false
}
