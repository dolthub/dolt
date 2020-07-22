#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE parent (
    id int PRIMARY KEY,
    v1 int,
    v2 int
);
ALTER TABLE parent ADD INDEX v1 (v1);
ALTER TABLE parent ADD INDEX v2 (v2);
CREATE TABLE child (
    id int primary key,
    v1 int,
    v2 int
);
SQL
}

teardown() {
    teardown_common
}

@test "foreign-keys: ALTER TABLE Single Named FOREIGN KEY" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_named FOREIGN KEY (v1) REFERENCES parent(v1);
SQL
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_named` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)' ]] || false
}

@test "foreign-keys: CREATE TABLE Single Named FOREIGN KEY" {
    dolt sql <<SQL
CREATE TABLE sibling (
  id int PRIMARY KEY,
  v1 int,
  CONSTRAINT fk_named FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
SQL
    run dolt schema show sibling
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_named` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)' ]] || false
}

@test "foreign-keys: CREATE TABLE Name Collision" {
    run dolt sql <<SQL
CREATE TABLE child (
  id INT PRIMARY KEY,
  v1 INT,
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1),
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
SQL

    [ "$status" -eq "1" ]
    [[ "$output" =~ "already exists" ]] || false
}

@test "foreign-keys: CREATE TABLE Type Mismatch" {
    run dolt sql <<SQL
CREATE TABLE sibling (
  pk int primary key,
  v1 text
);
SQL
    run dolt sql -q "ALTER TABLE sibling ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "column type mismatch" ]] || false
}

@test "foreign-keys: CREATE TABLE Key Count Mismatch" {
    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1,v2);"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "number of columns" ]] || false

    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1,v2) REFERENCES parent(v1);"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "number of columns" ]] || false
}

@test "foreign-keys: SET DEFAULT not supported" {
    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE SET DEFAULT"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "\"SET DEFAULT\" is not supported" ]] || false

    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1) ON UPDATE SET DEFAULT"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "\"SET DEFAULT\" is not supported" ]] || false

    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1) ON UPDATE SET DEFAULT ON DELETE SET DEFAULT"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "\"SET DEFAULT\" is not supported" ]] || false
}

@test "foreign-keys: CREATE TABLE Disallow TEXT/BLOB" {
    dolt sql <<SQL
CREATE TABLE parent1 (
  id INT PRIMARY KEY,
  v1 TINYTEXT,
  v2 TEXT,
  v3 MEDIUMTEXT,
  v4 LONGTEXT
);
SQL

    run dolt sql <<SQL
CREATE TABLE child11 (
  id INT PRIMARY KEY,
  parent_v1 TINYTEXT,
  FOREIGN KEY (parent_v1)
    REFERENCES parent1(v1)
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false

    run dolt sql <<SQL
CREATE TABLE child12 (
  id INT PRIMARY KEY,
  parent_v2 TEXT,
  FOREIGN KEY (parent_v2)
    REFERENCES parent1(v2)
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false

    run dolt sql <<SQL
CREATE TABLE child13 (
  id INT PRIMARY KEY,
  parent_v3 MEDIUMTEXT,
  FOREIGN KEY (parent_v3)
    REFERENCES parent1(v3)
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false

    run dolt sql <<SQL
CREATE TABLE child14 (
  id INT PRIMARY KEY,
  parent_v4 LONGTEXT,
  FOREIGN KEY (parent_v4)
    REFERENCES parent1(v4)
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false

    skip "TEXT passed, BLOB not yet supported"
    dolt sql <<SQL
CREATE TABLE parent2 (
  id INT PRIMARY KEY,
  v1 TINYBLOB,
  v2 BLOB,
  v3 MEDIUMBLOB,
  v4 LONGBLOB
);
SQL

    run dolt sql <<SQL
CREATE TABLE child21 (
  id INT PRIMARY KEY,
  parent_v1 TINYBLOB,
  FOREIGN KEY (parent_v1)
    REFERENCES parent2(v1)
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false

    run dolt sql <<SQL
CREATE TABLE child22 (
  id INT PRIMARY KEY,
  parent_v2 BLOB,
  FOREIGN KEY (parent_v2)
    REFERENCES parent2(v2)
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false

    run dolt sql <<SQL
CREATE TABLE child23 (
  id INT PRIMARY KEY,
  parent_v3 MEDIUMBLOB,
  FOREIGN KEY (parent_v3)
    REFERENCES parent2(v3)
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false

    run dolt sql <<SQL
CREATE TABLE child24 (
  id INT PRIMARY KEY,
  parent_v4 LONGBLOB,
  FOREIGN KEY (parent_v4)
    REFERENCES parent2(v4)
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false
}

@test "foreign-keys: CREATE TABLE Non-existent Table" {
    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES father(v1)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table not found" ]] || false
}

@test "foreign-keys: CREATE TABLE Non-existent Columns" {
    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (random) REFERENCES parent(v1)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "does not have column" ]] || false

    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(random)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "does not have column" ]] || false
}

@test "foreign-keys: CREATE TABLE SET NULL on non-nullable column" {
    dolt sql -q "ALTER TABLE child MODIFY v1 int NOT NULL"

    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE SET NULL"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "SET NULL" ]] || false
    [[ "$output" =~ "v1" ]] || false
    
    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1) ON UPDATE SET NULL"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "SET NULL" ]] || false
    [[ "$output" =~ "v1" ]] || false

    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE SET NULL ON UPDATE SET NULL"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "SET NULL" ]] || false
    [[ "$output" =~ "v1" ]] || false
}

@test "foreign-keys: ALTER TABLE Foreign Key Name Collision" {
    dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1);"
    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "already exists" ]] || false
}

@test "foreign-keys: ALTER TABLE DROP FOREIGN KEY" {
    dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1)"
    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1)" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1)" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'CONSTRAINT `fk_name`' ]] || false

    run dolt sql -q "ALTER TABLE child DROP FOREIGN KEY fk_name"
    [ "$status" -eq "0" ]
    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1)" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1)" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ 'CONSTRAINT `fk_name`' ]] || false
    run dolt sql -q "ALTER TABLE child DROP FOREIGN KEY fk_name"
    [ "$status" -eq "1" ]
}

@test "foreign-keys: ADD FOREIGN KEY fails on existing table when data would cause violation" {
    dolt sql <<SQL
INSERT INTO parent VALUES (1, 1, 1), (2, 2, 2);
INSERT INTO child  VALUES (1, 1, 1), (2, 3, 2);
SQL
    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    [[ "$output" =~ "fk_name" ]] || false
}

@test "foreign-keys: RENAME TABLE" {
    dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1)"
    dolt sql -q "RENAME TABLE parent TO new_parent;"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `new_parent` (`v1`)' ]] || false
    dolt sql -q "RENAME TABLE child TO new_child;"
    run dolt schema show new_child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `new_parent` (`v1`)' ]] || false
}

@test "foreign-keys: dolt table mv" {
    dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1)"
    dolt table mv parent new_parent
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `new_parent` (`v1`)' ]] || false
    dolt table mv child new_child;
    run dolt schema show new_child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `new_parent` (`v1`)' ]] || false
}

@test "foreign-keys: DROP TABLE" {
    dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1)"
    dolt index ls parent
    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1)" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1)" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'CONSTRAINT `fk_name`' ]] || false

    run dolt sql -q "DROP TABLE parent"
    [ "$status" -eq "1" ]
    dolt sql -q "DROP TABLE child"
    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1)" ]] || false
    dolt sql -q "DROP TABLE parent"
}

@test "foreign-keys: dolt table rm" {
    dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1)"
    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1)" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1)" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'CONSTRAINT `fk_name`' ]] || false

    run dolt table rm parent
    [ "$status" -eq "1" ]
    dolt table rm child
    run dolt index ls parent
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "(id) HIDDEN" ]] || false
    dolt table rm parent
}

@test "foreign-keys: dolt table cp" {
    dolt sql <<SQL
CREATE TABLE one (
  id BIGINT PRIMARY KEY,
  extra BIGINT
);
ALTER TABLE one ADD INDEX extra (extra);
CREATE TABLE two (
  id BIGINT PRIMARY KEY,
  one_extra BIGINT,
  FOREIGN KEY (one_extra)
    REFERENCES one(extra)
);
SQL
    
    dolt table cp two two_new
    run dolt schema show two_new
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "FOREIGN KEY" ]] || false
    
    run dolt schema show two
    [ "$status" -eq "0" ]
    [[ "$output" =~ "FOREIGN KEY" ]] || false
}

@test "foreign-keys: ALTER TABLE RENAME COLUMN" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1);
ALTER TABLE parent RENAME COLUMN v1 TO v1_new;
ALTER TABLE child RENAME COLUMN v1 TO v1_new;
SQL

    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk1` FOREIGN KEY (`v1_new`) REFERENCES `parent` (`v1_new`)' ]] || false
}

@test "foreign-keys: DROP COLUMN" {
    dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1)"
    dolt add -A
    dolt commit -m "initial commit"
    run dolt sql -q "ALTER TABLE parent DROP COLUMN v1"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "v1" ]] || false
    dolt sql -q "ALTER TABLE child DROP FOREIGN KEY fk_name"
    dolt sql -q "ALTER TABLE parent DROP COLUMN v1"
    
    dolt reset --hard
    run dolt sql -q "ALTER TABLE child DROP COLUMN v1"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "v1" ]] || false
    dolt sql -q "ALTER TABLE child DROP FOREIGN KEY fk_name"
    dolt sql -q "ALTER TABLE child DROP COLUMN v1"
}

@test "foreign-keys: Disallow change column type when SET NULL" {
    dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE SET NULL ON UPDATE SET NULL"
    skip "column type changes are not yet supported"
    run dolt sql -q "ALTER TABLE child CHANGE COLUMN parent_extra parent_extra BIGINT"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "SET NULL" ]] || false
    [[ "$output" =~ "parent_extra" ]] || false
    
    run dolt sql -q "ALTER TABLE child CHANGE COLUMN parent_extra parent_extra BIGINT NULL"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "SET NULL" ]] || false
    [[ "$output" =~ "parent_extra" ]] || false
}

@test "foreign-keys: SQL CASCADE" {
    dolt sql <<SQL
CREATE TABLE one (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
ALTER TABLE one ADD INDEX v1 (v1);
CREATE TABLE two (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  CONSTRAINT fk_name_1 FOREIGN KEY (v1)
    REFERENCES one(v1)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);
ALTER TABLE two ADD INDEX v1v2 (v1, v2);
CREATE TABLE three (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  CONSTRAINT fk_name_2 FOREIGN KEY (v1, v2)
    REFERENCES two(v1, v2)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);
INSERT INTO one VALUES (1, 1, 4), (2, 2, 5), (3, 3, 6), (4, 4, 5);
INSERT INTO two VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3), (5, 4, 4);
INSERT INTO three VALUES (3, 1, 1), (4, 2, 2), (5, 3, 3), (6, 4, 4);
UPDATE one SET v1 = v1 + v2;
DELETE FROM one WHERE pk = 3;
UPDATE two SET v2 = v1 - 2;
SQL

    run dolt schema show two
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name_1` FOREIGN KEY (`v1`) REFERENCES `one` (`v1`) ON DELETE CASCADE ON UPDATE CASCADE' ]] || false
    run dolt schema show three
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name_2` FOREIGN KEY (`v1`,`v2`) REFERENCES `two` (`v1`,`v2`) ON DELETE CASCADE ON UPDATE CASCADE' ]] || false

    run dolt sql -q "SELECT * FROM one" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "1,5,4" ]] || false
    [[ "$output" =~ "2,7,5" ]] || false
    [[ "$output" =~ "4,9,5" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    run dolt sql -q "SELECT * FROM two" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "2,5,3" ]] || false
    [[ "$output" =~ "3,7,5" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM three" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "3,5,3" ]] || false
    [[ "$output" =~ "4,7,5" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "foreign-keys: SQL SET NULL" {
    dolt sql <<SQL
CREATE TABLE one (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
ALTER TABLE one ADD INDEX v1 (v1);
CREATE TABLE two (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  CONSTRAINT fk_name_1 FOREIGN KEY (v1)
    REFERENCES one(v1)
    ON DELETE SET NULL
    ON UPDATE SET NULL
);
INSERT INTO one VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO two VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
UPDATE one SET v1 = v1 * v2;
INSERT INTO one VALUES (4, 4, 4);
INSERT INTO two VALUES (4, 4, 4);
UPDATE one SET v2 = v1 * v2;
SQL
    
    run dolt schema show two
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name_1` FOREIGN KEY (`v1`) REFERENCES `one` (`v1`) ON DELETE SET NULL ON UPDATE SET NULL' ]] || false
    
    run dolt sql -q "SELECT * FROM one" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "1,1,1" ]] || false
    [[ "$output" =~ "2,4,8" ]] || false
    [[ "$output" =~ "3,9,27" ]] || false
    [[ "$output" =~ "4,4,16" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
    run dolt sql -q "SELECT * FROM two" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "1,1,1" ]] || false
    [[ "$output" =~ "2,,2" ]] || false
    [[ "$output" =~ "3,,3" ]] || false
    [[ "$output" =~ "4,4,4" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
}

@test "foreign-keys: SQL RESTRICT" {
    dolt sql <<SQL
CREATE TABLE one (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
ALTER TABLE one ADD INDEX v1 (v1);
CREATE TABLE two (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  CONSTRAINT fk_name_1 FOREIGN KEY (v1)
    REFERENCES one(v1)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT
);
INSERT INTO one VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO two VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
SQL
    
    run dolt schema show two
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name_1` FOREIGN KEY (`v1`) REFERENCES `one` (`v1`) ON DELETE RESTRICT ON UPDATE RESTRICT' ]] || false
    
    run dolt sql -q "UPDATE one SET v1 = v1 + v2;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    dolt sql -q "UPDATE one SET v1 = v1;"
    run dolt sql -q "DELETE FROM one;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
}

@test "foreign-keys: SQL no reference options" {
    dolt sql <<SQL
CREATE TABLE one (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
ALTER TABLE one ADD INDEX v1 (v1);
CREATE TABLE two (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  CONSTRAINT fk_name_1 FOREIGN KEY (v1)
    REFERENCES one(v1)
);
INSERT INTO one VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO two VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
SQL
    
    run dolt schema show two
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name_1` FOREIGN KEY (`v1`) REFERENCES `one` (`v1`)' ]] || false
    
    run dolt sql -q "UPDATE one SET v1 = v1 + v2;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    dolt sql -q "UPDATE one SET v1 = v1;"
    run dolt sql -q "DELETE FROM one;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
}

@test "foreign-keys: SQL INSERT multiple keys violates only one" {
    dolt sql <<SQL
CREATE TABLE one (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
ALTER TABLE one ADD INDEX v1 (v1);
ALTER TABLE one ADD INDEX v2 (v2);
CREATE TABLE two (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  CONSTRAINT fk_name_1 FOREIGN KEY (v1)
    REFERENCES one(v1),
  CONSTRAINT fk_name_2 FOREIGN KEY (v2)
    REFERENCES one(v2)
);
INSERT INTO one VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO two VALUES (1, NULL, 1);
SQL
    
    run dolt sql -q "INSERT INTO two VALUES (2, NULL, 4)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    [[ "$output" =~ "fk_name_2" ]] || false
    
    run dolt sql -q "INSERT INTO two VALUES (3, 4, NULL)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    [[ "$output" =~ "fk_name_1" ]] || false
    
    dolt sql -q "INSERT INTO two VALUES (4, NULL, NULL)" # sanity check
}

@test "foreign-keys: dolt table import" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
INSERT INTO parent VALUES (1, 1, 1), (2, 2, 2);
INSERT INTO child  VALUES (1, 1, 1), (2, 2, 2);
SQL

    echo $'id,v1,v2\n1,3,3\n2,4,4' > update_parent.csv
    dolt table import -u parent update_parent.csv
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1,v2" ]] || false
    [[ "$output" =~ "1,3,3" ]] || false
    [[ "$output" =~ "2,4,4" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1,v2" ]] || false
    [[ "$output" =~ "1,3,1" ]] || false
    [[ "$output" =~ "2,4,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    echo $'id,v1,v2\n1,1,1\n2,2,2' > update_child.csv
    run dolt table import -u child update_child.csv
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false

    echo $'id,v1,v2\n3,3,3\n4,4,4' > update_child.csv
    dolt table import -u child update_child.csv
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1,v2" ]] || false
    [[ "$output" =~ "1,3,1" ]] || false
    [[ "$output" =~ "2,4,2" ]] || false
    [[ "$output" =~ "3,3,3" ]] || false
    [[ "$output" =~ "4,4,4" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false

    echo $'id,v1,v2\n1,1,1\n2,2,2' > update_child.csv
    run dolt table import -r child update_child.csv
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false

    echo $'id,v1,v2\n3,3,3\n4,4,4' > update_child.csv
    dolt table import -r child update_child.csv
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1,v2" ]] || false
    [[ "$output" =~ "3,3,3" ]] || false
    [[ "$output" =~ "4,4,4" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "foreign-keys: Commit all" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE RESTRICT;
SQL

    dolt schema show child
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false
    dolt add -A
    dolt commit -m "has fk"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false

    dolt checkout -b still_has_fk
    dolt checkout master
    dolt table rm child
    dolt add -A
    run dolt schema show
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "FOREIGN KEY" ]] || false
    dolt commit -m "removed child"
    run dolt schema show
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "FOREIGN KEY" ]] || false

    dolt checkout still_has_fk
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false
    dolt sql -q "rename table parent to super_parent"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `super_parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false
    dolt add super_parent
    dolt commit -m "renamed parent"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `super_parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false
    
    dolt checkout -b last_commit HEAD~1
    dolt reset --hard # See issue https://github.com/liquidata-inc/dolt/issues/752
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false
    
    dolt checkout master
    run dolt schema show
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "FOREIGN KEY" ]] || false
}

@test "foreign-keys: Commit then rename parent, child, and columns" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
SQL

    dolt add -A
    dolt commit -m "has fk"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)' ]] || false
    dolt checkout -b original
    dolt checkout master
    
    dolt sql <<SQL
RENAME TABLE parent TO new_parent;
RENAME TABLE child TO new_child;
ALTER TABLE new_parent RENAME COLUMN v1 TO vnew;
ALTER TABLE new_child RENAME COLUMN v1 TO vnew;
SQL
    run dolt schema show new_child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`vnew`) REFERENCES `new_parent` (`vnew`)' ]] || false
    dolt add -A
    dolt commit -m "renamed everything"
    run dolt schema show new_child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`vnew`) REFERENCES `new_parent` (`vnew`)' ]] || false
    
    dolt checkout original
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)' ]] || false
}

@test "foreign-keys: Commit then recreate key with different columns" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
SQL

    dolt add -A
    dolt commit -m "has fk"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)' ]] || false
    dolt checkout -b original
    dolt checkout master
    
    dolt sql <<SQL
DROP TABLE child;
ALTER TABLE parent ADD INDEX v1v2 (v1,v2);
CREATE TABLE child (
  pk int PRIMARY KEY,
  v1 int,
  v2 int,
  CONSTRAINT fk_name FOREIGN KEY (v1,v2)
    REFERENCES parent(v1,v2)
);
SQL
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`,`v2`) REFERENCES `parent` (`v1`,`v2`)' ]] || false
    dolt add -A
    skip "todo: error in staging logic"
    dolt commit -m "different fk same name"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`,`v2`) REFERENCES `parent` (`v1`,`v2`)' ]] || false
    
    dolt checkout original
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)' ]] || false
}

@test "foreign-keys: Commit then recreate key with parent columns" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
SQL

    dolt add -A
    dolt commit -m "has fk"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)' ]] || false
    dolt checkout -b original
    dolt checkout master
    
    dolt sql <<SQL
ALTER TABLE child DROP FOREIGN KEY fk_name;
DROP TABLE parent;
CREATE TABLE parent (
  pk int PRIMARY KEY,
  vnew int
);
ALTER TABLE parent ADD INDEX vnew (vnew);
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(vnew);
SQL
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`vnew`)' ]] || false
    dolt add -A
    skip "todo: error in staging logic"
    dolt commit -m "different fk new parent col name"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`vnew`)' ]] || false
    
    dolt checkout original
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)' ]] || false
}

@test "foreign-keys: Commit then recreate key with parent renamed" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
SQL

    dolt add -A
    dolt commit -m "has fk"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)' ]] || false
    dolt checkout -b original
    dolt checkout master
    
    dolt sql <<SQL
ALTER TABLE child DROP FOREIGN KEY fk_name;
DROP TABLE parent;
CREATE TABLE new_parent (
  pk int PRIMARY KEY,
  v1 int,
  v2 int
);
ALTER TABLE new_parent ADD INDEX v1 (v1);
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES new_parent(v1);
SQL
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `new_parent` (`v1`)' ]] || false
    dolt add -A
    skip "todo: error in staging logic"
    dolt commit -m "different fk new parent"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `new_parent` (`v1`)' ]] || false
    
    dolt checkout original
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)' ]] || false
}

@test "foreign-keys: Commit --force" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
SQL

    dolt add child
    run dolt commit -m "will fail"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "parent" ]] || false
    dolt commit --force -m "will succeed"
    
    dolt checkout -b last_commit HEAD~1
    run dolt commit -m "nothing changed, will fail"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "no changes" ]] || false
    dolt add parent
    dolt commit -m "parent commits just fine without child, child not in working set anymore"
}

@test "foreign-keys: Commit then delete foreign key" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
SQL

    dolt add -A
    dolt commit -m "has fk"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)' ]] || false
    dolt checkout -b original
    dolt checkout master
    
    dolt sql <<SQL
ALTER TABLE child DROP FOREIGN KEY fk_name;
SQL
    run dolt schema show child
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "FOREIGN KEY" ]] || false
    dolt add -A
    skip "todo: error in staging logic"
    dolt commit -m "no foreign key"
    run dolt schema show child
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "FOREIGN KEY" ]] || false
    
    dolt checkout original
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)' ]] || false
}

@test "foreign-keys: Reset staged table" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
SQL

    dolt add -A
    dolt reset parent
    run dolt commit -m "will fail since parent is missing"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "parent" ]] || false
}

@test "foreign-keys: Commit, rename parent, commit only child" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
SQL

    dolt add -A
    dolt commit -m "has fk"
    
    dolt sql <<SQL
RENAME TABLE parent TO super_parent;
SQL
    dolt add child
    run dolt commit -m "will fail since super_parent is missing"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "super_parent" ]] || false
    dolt add super_parent
    dolt commit -m "passes now"
}

@test "foreign-keys: Merge valid onto parent" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
INSERT INTO parent VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO child VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3);
SQL

    dolt add -A
    dolt commit -m "initial commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql <<SQL
INSERT INTO parent VALUES (4, 3, 3);
SQL
    dolt add -A
    dolt commit -m "added row"
    dolt checkout other

    dolt sql <<SQL
SET FOREIGN_KEY_CHECKS=0;
UPDATE parent SET v1 = v1 - 1;
SQL
    dolt add -A
    dolt commit -m "updated parent"
    dolt checkout master
    skip "todo: FK merge"
    dolt merge other
    
    run dolt sql -q "SELECT * FROM parent ORDER BY pk ASC" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,0" ]] || false
    [[ "$output" =~ "2,1" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "$output" =~ "4,3" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
    run dolt sql -q "SELECT * FROM child ORDER BY pk ASC" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,1" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "$output" =~ "4,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "foreign-keys: Merge invalid onto parent" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
INSERT INTO parent VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO child VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3);
SQL

    dolt add -A
    dolt commit -m "initial commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql <<SQL
INSERT INTO parent VALUES (4, 4, 4);
SQL
    dolt add -A
    dolt commit -m "added row"
    dolt checkout other

    dolt sql <<SQL
SET FOREIGN_KEY_CHECKS=0;
UPDATE parent SET v1 = v1 - 1;
SQL
    dolt add -A
    dolt commit -m "updated parent"
    dolt checkout master
    skip "todo: FK merge"
    run dolt merge other
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    [[ "$output" =~ "3" ]] || false
}

@test "foreign-keys: Merge valid onto child" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
INSERT INTO parent VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO child VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3);
SQL

    dolt add -A
    dolt commit -m "initial commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql <<SQL
INSERT INTO parent VALUES (4, 4, 4);
SQL
    dolt add -A
    dolt commit -m "added row"
    dolt checkout other

    dolt sql <<SQL
SET FOREIGN_KEY_CHECKS=0;
UPDATE child SET v1 = v1 + 1;
SQL
    dolt add -A
    dolt commit -m "updated child"
    dolt checkout master
    skip "todo: FK merge"
    dolt merge other
    
    run dolt sql -q "SELECT * FROM parent ORDER BY pk ASC" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "4,4" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
    run dolt sql -q "SELECT * FROM child ORDER BY pk ASC" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "4,4" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "foreign-keys: Merge invalid onto child" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
INSERT INTO parent VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO child VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3);
SQL

    dolt add -A
    dolt commit -m "initial commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql <<SQL
INSERT INTO parent VALUES (4, 4, 4);
SQL
    dolt add -A
    dolt commit -m "added row"
    dolt checkout other

    dolt sql <<SQL
SET FOREIGN_KEY_CHECKS=0;
UPDATE child SET v1 = v1 - 1;
SQL
    dolt add -A
    dolt commit -m "updated child"
    dolt checkout master
    skip "todo: FK merge"
    run dolt merge other
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    [[ "$output" =~ "0" ]] || false
}

@test "foreign-keys: Merge valid onto parent and child" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
INSERT INTO parent VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO child VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3);
SQL

    dolt add -A
    dolt commit -m "initial commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql <<SQL
INSERT INTO parent VALUES (4, 3, 3), (5, 4, 4);
SQL
    dolt add -A
    dolt commit -m "added row"
    dolt checkout other

    dolt sql <<SQL
SET FOREIGN_KEY_CHECKS=0;
UPDATE parent SET v1 = v1 - 1;
UPDATE child SET v1 = v1 + 1;
SQL
    dolt add -A
    dolt commit -m "updated both"
    skip "todo: FK merge"
    dolt checkout master
    dolt merge other
    
    run dolt sql -q "SELECT * FROM parent ORDER BY pk ASC" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,0" ]] || false
    [[ "$output" =~ "2,1" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "$output" =~ "4,3" ]] || false
    [[ "$output" =~ "5,4" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt sql -q "SELECT * FROM child ORDER BY pk ASC" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "4,4" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "foreign-keys: Merge invalid onto parent and child" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE CASCADE;
INSERT INTO parent VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO child VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3);
SQL

    dolt add -A
    dolt commit -m "initial commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql <<SQL
INSERT INTO parent VALUES (4, 3, 3);
SQL
    dolt add -A
    dolt commit -m "added row"
    dolt checkout other

    dolt sql <<SQL
SET FOREIGN_KEY_CHECKS=0;
UPDATE parent SET v1 = v1 - 1;
UPDATE child SET v1 = v1 + 1;
SQL
    dolt add -A
    dolt commit -m "updated both"
    dolt checkout master
    skip "todo: FK merge"
    run dolt merge other
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    [[ "$output" =~ "4" ]] || false
}
