#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))"
    dolt add -A
    dolt commit -m "Created table"
    dolt checkout -b branch1
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    dolt add -A
    dolt commit -m "Inserted 1"
    dolt sql -q "INSERT INTO test VALUES (2, 'b')"
    dolt add -A
    dolt commit -m "Inserted 2"
    dolt sql -q "INSERT INTO test VALUES (3, 'c')"
    dolt add -A
    dolt commit -m "Inserted 3"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "cherry-pick: simple cherry pick with the latest commit" {
    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,b" ]] || false
    [[ "$output" =~ "3,c" ]] || false

    dolt checkout main

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ ! "$output" =~ "1,a" ]] || false
    [[ ! "$output" =~ "2,b" ]] || false
    [[ "$output" =~ "3,c" ]] || false
}

@test "cherry-pick: multiple simple cherry-picks" {
    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,b" ]] || false
    [[ "$output" =~ "3,c" ]] || false

    dolt sql -q "UPDATE test SET v = 'x' WHERE pk = 2"
    dolt sql -q "INSERT INTO test VALUES (5, 'g'), (8, 'u');"
    dolt add -A
    dolt commit -m "Updated 2b to 2x and inserted more rows"

    dolt checkout main

    run dolt cherry-pick branch1~2
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "2,b" ]] || false
    [[ ! "$output" =~ "1,a" ]] || false
    [[ ! "$output" =~ "2,x" ]] || false
    [[ ! "$output" =~ "3,c" ]] || false

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ ! "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,x" ]] || false
    [[ ! "$output" =~ "3,c" ]] || false
    [[ "$output" =~ "5,g" ]] || false
    [[ "$output" =~ "8,u" ]] || false
}

@test "cherry-pick: too far back" {
    dolt checkout main
    run dolt cherry-pick branch1~10
    [ "$status" -eq "1" ]
    [[ "$output" =~ "ancestor" ]] || false
}

@test "cherry-pick: no changes" {
    dolt commit --allow-empty -m "empty commit"
    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "No changes were made" ]] || false
}

@test "cherry-pick: invalid hash" {
    run dolt cherry-pick aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    [ "$status" -eq "1" ]
    [[ "$output" =~ "target commit not found" ]] || false
}

@test "cherry-pick: has changes in the working set" {
    dolt checkout main
    dolt sql -q "INSERT INTO test VALUES (4, 'f')"
    run dolt cherry-pick branch1~2
    [ "$status" -eq "1" ]
    [[ "$output" =~ "changes" ]] || false
}

@test "cherry-pick: update and delete on non existent row" {
    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,b" ]] || false
    [[ "$output" =~ "3,c" ]] || false

    dolt sql -q "UPDATE test SET v = 'x' WHERE pk = 2"
    dolt sql -q "DELETE FROM test WHERE pk = 1"
    dolt add -A
    dolt commit -m "Update and delete rows"

    dolt checkout main

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ ! "$output" =~ "1,a" ]] || false
    [[ ! "$output" =~ "2,b" ]] || false
    [[ ! "$output" =~ "2,x" ]] || false
    [[ ! "$output" =~ "3,c" ]] || false
}

@test "cherry-pick: insert duplicate rows" {
    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,b" ]] || false
    [[ "$output" =~ "3,c" ]] || false

    dolt checkout main

    dolt sql -q "INSERT INTO test VALUES (3, 'c')"
    dolt add -A
    dolt commit -m "Inserted 3 on main"

    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "duplicate key error" ]] || false
}

@test "cherry-pick: insert, update, delete rows and schema changes on non existent table in working set" {
    dolt sql -q "CREATE TABLE branch1table (id int primary key, col1 int)"
    dolt sql -q "INSERT INTO branch1table VALUES (9,8),(7,6),(5,4)"
    dolt add -A
    dolt commit -m "create table with rows"

    dolt sql -q "INSERT INTO branch1table VALUES (1,2)"
    dolt add -A
    dolt commit -m "Insert a row"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "No changes" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false

    dolt checkout branch1
    dolt sql -q "UPDATE branch1table SET col1 = 0 WHERE id > 6"
    dolt add -A
    dolt commit -m "Update a rows"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table does not exist in working set" ]] || false
    [[ "$output" =~ "No changes" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false

    dolt checkout branch1
    dolt sql -q "DELETE FROM branch1table WHERE id > 8"
    dolt add -A
    dolt commit -m "Update and delete rows"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table does not exist in working set" ]] || false
    [[ "$output" =~ "No changes" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false

    dolt checkout branch1
    dolt sql -q "ALTER TABLE branch1table ADD COLUMN col2 int"
    dolt add -A
    dolt commit -m "Alter table add column"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table does not exist in working set" ]] || false
    [[ "$output" =~ "No changes" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false
}

@test "cherry-pick: commit with CREATE TABLE" {
    dolt sql -q "CREATE TABLE table_a (pk BIGINT PRIMARY KEY, v varchar(10))"
    dolt sql -q "INSERT INTO table_a VALUES (11, 'aa'), (22, 'ab'), (33, 'ac')"
    dolt sql -q "DELETE FROM test WHERE pk = 2"
    dolt add -A
    dolt commit -m "Added table_a with rows and delete pk=2 from test"

    dolt checkout main

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    dolt sql -q "SHOW TABLES" -r csv
    [[ "$output" =~ "table_a" ]] || false

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ ! "$output" =~ "1,a" ]] || false
    [[ ! "$output" =~ "2,b" ]] || false
    [[ ! "$output" =~ "3,c" ]] || false

    run dolt sql -q "SELECT * FROM table_a" -r csv
    [[ "$output" =~ "11,aa" ]] || false
    [[ "$output" =~ "22,ab" ]] || false
    [[ "$output" =~ "33,ac" ]] || false
}

@test "cherry-pick: commit with DROP TABLE" {
    dolt sql -q "DROP TABLE test"
    dolt add -A
    dolt commit -m "Drop table test"

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "test" ]] || false

    dolt checkout main

    run dolt sql -q "SHOW TABLES" -r csv
    [[ "$output" =~ "test" ]] || false

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "test" ]] || false
}

@test "cherry-pick: ALTER TABLE rename table name" {
    dolt sql -q "INSERT INTO test VALUES (4, 'd')"
    dolt sql -q "ALTER TABLE test RENAME TO new_name"
    dolt add -A
    dolt commit -m "rename table name"

    dolt checkout main

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table not found" ]] || false

    run dolt sql -q "SELECT * FROM new_name" -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "4,d" ]] || false
    [[ ! "$output" =~ "1,a" ]] || false
    [[ ! "$output" =~ "2,b" ]] || false
    [[ ! "$output" =~ "3,c" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE add column" {
    skip # TODO: handle schema changes
    dolt sql -q "INSERT INTO test VALUES (4, 'd')"
    dolt sql -q "ALTER TABLE test ADD COLUMN c int"
    dolt add -A
    dolt commit -m "alter table test add column c"

    dolt checkout main

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW CREATE TABLE test" -r csv
    [[ "$output" =~ "\`c\` int" ]] || false

    run dolt sql -q "SELECT * FROM test" -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "4,d" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE change column" {
    skip # TODO: handle schema changes
    dolt sql -q "INSERT INTO test VALUES (4, 'd')"
    dolt sql -q "ALTER TABLE test CHANGE COLUMN v c varchar(100)"
    dolt add -A
    dolt commit -m "alter table test change column v"

    dolt checkout main

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW CREATE TABLE test" -r csv
    [[ "$output" =~ "\`c\` varchar(100)" ]] || false

    run dolt sql -q "SELECT * FROM test" -r csv
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "pk,v" ]] || false
    [[ "$output" =~ "pk,c" ]] || false
    [[ "$output" =~ "4,d" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE modify column" {
    skip # TODO: handle schema changes
    dolt sql -q "UPDATE test SET v = '1' WHERE pk < 4"
    dolt sql -q "ALTER TABLE test MODIFY COLUMN v int"
    dolt add -A
    dolt commit -m "alter table test modify column v"

    dolt checkout main

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW CREATE TABLE test" -r csv
    [[ "$output" =~ "\`v\` int" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE drop column" {
    skip # TODO: handle schema changes
    dolt sql -q "ALTER TABLE test DROP COLUMN v"
    dolt add -A
    dolt commit -m "alter table test drop column v"

    dolt checkout main

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW CREATE TABLE test" -r csv
    [[ ! "$output" =~ "\`v\` varchar(10)" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE rename column" {
    skip # TODO: handle schema changes
    dolt sql -q "ALTER TABLE test RENAME COLUMN v TO c"
    dolt add -A
    dolt commit -m "alter table test rename column v"

    dolt checkout main

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW CREATE TABLE test" -r csv
    [[ "$output" =~ "\`c\` varchar(10)" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE drop and add primary key" {
    skip # TODO: handle schema changes
    dolt sql -q "ALTER TABLE test DROP PRIMARY KEY, ADD PRIMARY KEY (pk, v)"
    dolt add -A
    dolt commit -m "alter table test drop and add primary key"

    dolt checkout main

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW CREATE TABLE test" -r csv
    [[ "$output" =~ "PRIMARY KEY (\`pk\`,\`v\`)" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE foreign key with create index" {
    skip # TODO : handle index changes
    dolt sql -q "CREATE TABLE child (id INT PRIMARY KEY, cv VARCHAR(10))"
    dolt sql -q "CREATE INDEX idx_v ON test(v)"
    dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (cv) REFERENCES test(v)"
    dolt add -A
    dolt commit -m "create index and alter table add foreign key"

    dolt checkout main

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    dolt sql -q "SHOW CREATE TABLE test" -r csv
    [[ "$output" =~ "KEY \`idx_v\` (\`v\`)" ]] || false

    run dolt sql -q "SHOW CREATE TABLE child" -r csv
    [[ "$output" =~ "KEY \`cv\` (\`cv\`)" ]] || false
    [[ "$output" =~ "CONSTRAINT \`fk1\` FOREIGN KEY (\`cv\`) REFERENCES \`test\` (\`v\`)" ]] || false

    run dolt index ls test
    [ "$status" -eq "0" ]
    [[ "$output" =~ "idx_v(v)" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE foreign key" {
    skip # TODO : handle foreign key changes
    dolt checkout main
    dolt sql <<SQL
CREATE TABLE parent (id int PRIMARY KEY, v1 int, v2 int, INDEX v1 (v1), INDEX v2 (v2));
CREATE TABLE child (id int primary key, v1 int, v2 int);
SQL
    dolt add -A
    dolt commit -m "create two tables"

    dolt checkout -b branch2

    dolt sql -q "INSERT INTO parent VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5)"
    dolt add -A
    dolt commit -m "Inserted rows to parent"
    dolt sql -q "INSERT INTO child VALUES (11, 2, 3), (22, 3, 4), (33, 4, 5)"
    dolt add -A
    dolt commit -m "Inserted rows to child"
    dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_named FOREIGN KEY (v1) REFERENCES parent(v1)"
    dolt add -A
    dolt commit -m "create index and alter table add foreign key"

    dolt checkout main

    dolt cherry-pick branch2
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW CREATE TABLE child" -r csv
    [[ "$output" =~ "KEY \`v1\` (\`cv1\`)" ]] || false
    [[ "$output" =~ "CONSTRAINT \`fk_named\` FOREIGN KEY (\`v1\`) REFERENCES \`test\` (\`v1\`)" ]] || false

    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1(v1)" ]] || false
}
