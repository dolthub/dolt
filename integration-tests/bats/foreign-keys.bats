#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

    dolt sql <<SQL
CREATE TABLE parent (
    id int PRIMARY KEY,
    v1 int,
    v2 int,
    INDEX v1 (v1),
    INDEX v2 (v2)
);
CREATE TABLE child (
    id int primary key,
    v1 int,
    v2 int
);
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "foreign-keys: test foreign-key on commit checks" {
    dolt reset --hard
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

    dolt add .
    dolt commit -m "initialize"

    # delete a color that isn't used
    # delete a color that is used, and replace it with a new row with the same value
    # modify a used color and the corresponding object using it
    # add an object and point to an old color that has not been modified
    # add an object and point to the new color
    # add an object with a null color
    dolt sql <<SQL
      SET FOREIGN_KEY_CHECKS=0;
      DELETE FROM colors where id = 3 or id = 4;
      INSERT INTO colors (id,color) VALUES (5,'blue');
      UPDATE colors SET color='orange' WHERE color = 'green';
      UPDATE objects SET color='orange' WHERE color = 'green';
      INSERT INTO objects (id,name,color) VALUES (4,'car','red'),(5,'dress','orange');
      INSERT INTO objects (id,name) VALUES (6,'glass slipper')
SQL

    dolt sql -q 'select * from colors'
    dolt sql -q 'select * from objects'
    dolt add .
    dolt commit -m 'update 1'
}

@test "foreign-keys: test multi-field foreign-key on commit checks" {
    dolt reset --hard
    dolt sql <<SQL
      CREATE TABLE colors (
          id INT NOT NULL,
          color VARCHAR(32) NOT NULL,

          PRIMARY KEY (id),
          INDEX color_index(color)
      );
      CREATE TABLE materials (
          id INT NOT NULL,
          material VARCHAR(32) NOT NULL,
          color VARCHAR(32),

          PRIMARY KEY(id),
          FOREIGN KEY (color) REFERENCES colors(color),
          INDEX color_mat_index(color, material)
      );
      CREATE TABLE objects (
          id INT NOT NULL,
          name VARCHAR(64) NOT NULL,
          color VARCHAR(32),
          material VARCHAR(32),

          PRIMARY KEY(id),
          FOREIGN KEY (color,material) REFERENCES materials(color,material)
      );
      INSERT INTO colors (id,color) VALUES (1,'red'),(2,'green'),(3,'blue'),(4,'purple'),(10,'brown');
      INSERT INTO materials (id,material,color) VALUES (1,'steel','red'),(2,'rubber','green'),(3,'leather','blue'),(10,'dirt','brown'),(11,'air',NULL);
      INSERT INTO objects (id,name,color,material) VALUES (1,'truck','red','steel'),(2,'ball','green','rubber'),(3,'shoe','blue','leather'),(11,'tornado',NULL,'air');
SQL

    dolt add .
    dolt commit -m "initialize"

    dolt sql <<SQL
      SET FOREIGN_KEY_CHECKS=0;
      DELETE FROM colors where id = 3 or id = 4;
      INSERT INTO colors (id,color) VALUES (5,'blue');
      DELETE FROM materials WHERE id IN (1,10);
      INSERT INTO materials (id,material,color) VALUES (4,'steel','red'),(5,'fiber glass','red'),(6,'cotton','orange');
      UPDATE colors SET color='orange' WHERE color = 'green';
      UPDATE materials SET color='orange' WHERE color = 'green';
      UPDATE objects SET color='orange' WHERE color = 'green';
      INSERT INTO objects (id,name,color,material) VALUES (4,'car','red','fiber glass'),(5,'dress','orange','cotton');
      INSERT INTO materials (id,material) VALUES (7,'glass');
      INSERT INTO objects (id,name,material) VALUES (6,'glass slipper','glass');
      DELETE FROM objects WHERE material = 'air';
      DELETE FROM materials WHERE material = 'air'
SQL

    dolt sql -q 'select * from colors'
    dolt sql -q 'select * from materials'
    dolt sql -q 'select * from objects'
    dolt add .
    dolt commit -m 'update 1'
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

@test "foreign-keys: parent table index required" {
    # parent doesn't have an index over (v1,v2) to reference
    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1,v2) REFERENCES parent(v1,v2);"
    [ "$status" -ne "0" ]

    # parent implicitly has an index over its primary key
    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_id FOREIGN KEY (v1) REFERENCES parent(id);"
    [ "$status" -eq "0" ]
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
    [[ "$output" =~ "duplicate foreign key" ]] || false
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

@test "foreign-keys: ALTER TABLE SET NULL on non-nullable column" {
    dolt sql -q 'ALTER TABLE child MODIFY COLUMN v1 int NOT NULL'
    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE SET NULL"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "SET NULL" ]] || false
    [[ "$output" =~ "v1" ]] || false

    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON UPDATE SET NULL"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "SET NULL" ]] || false
    [[ "$output" =~ "v1" ]] || false

    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE SET NULL ON UPDATE SET NULL"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "SET NULL" ]] || false
    [[ "$output" =~ "v1" ]] || false
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

@test "foreign-keys: indexes used by foreign keys can't be dropped" {
    dolt sql <<SQL
ALTER TABLE child ADD INDEX v1 (v1);
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1);
SQL
    run dolt sql -q "ALTER TABLE child DROP INDEX v1"
    [ "$status" -ne "0" ]
    [[ "$output" =~ 'cannot drop index: `v1` is used by foreign key `fk_name`' ]] || false
    run dolt sql -q "ALTER TABLE parent DROP INDEX v1"
    [ "$status" -ne "0" ]
    [[ "$output" =~ 'cannot drop index: `v1` is used by foreign key `fk_name`' ]] || false

    run dolt sql -q "ALTER TABLE child DROP FOREIGN KEY fk_name"
    [ "$status" -eq "0" ]
    run dolt sql -q "ALTER TABLE child DROP INDEX v1"
    [ "$status" -eq "0" ]
    run dolt sql -q "ALTER TABLE parent DROP INDEX v1"
    [ "$status" -eq "0" ]
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

@test "foreign-keys: ALTER TABLE MODIFY COLUMN type change not allowed" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1);
SQL

    run dolt sql -q "ALTER TABLE parent MODIFY v1 MEDIUMINT;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "type" ]] || false
    run dolt sql -q "ALTER TABLE child MODIFY v1 MEDIUMINT;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "type" ]] || false
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
    run dolt sql -q "ALTER TABLE child CHANGE COLUMN parent_extra parent_extra BIGINT"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "parent_extra" ]] || false

    run dolt sql -q "ALTER TABLE child CHANGE COLUMN parent_extra parent_extra BIGINT NULL"
    [ "$status" -eq "1" ]
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
    run dolt table import -u parent update_parent.csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Rows Processed: 2, Additions: 0, Modifications: 2, Had No Effect: 0" ]] || false

    run dolt sql -r csv -q "select * from parent order by id"
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1,v2" ]] || false
    [[ "$output" =~ "1,3,3" ]] || false
    [[ "$output" =~ "2,4,4" ]] || false

    run dolt sql -r csv -q "select * from child order by id"
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1,v2" ]] || false
    [[ "$output" =~ "1,3,1" ]] || false
    [[ "$output" =~ "2,4,2" ]] || false
}

@test "foreign-keys: Commit all" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE CASCADE ON UPDATE RESTRICT;
SQL

    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false
    dolt add -A
    dolt commit -m "has fk"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false

    dolt checkout -b still_has_fk
    dolt checkout main
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
    dolt add .
    dolt commit -m "renamed parent"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `super_parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false

    dolt checkout -b last_commit HEAD~1
    dolt reset --hard # See issue https://github.com/dolthub/dolt/issues/752
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false

    dolt checkout main
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
    dolt checkout main

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
    dolt checkout main

    dolt sql <<SQL
ALTER TABLE parent ADD INDEX v1v2 (v1,v2);
ALTER TABLE child DROP FOREIGN KEY fk_name;
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1,v2) REFERENCES parent(v1,v2)
SQL
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`,`v2`) REFERENCES `parent` (`v1`,`v2`)' ]] || false
    dolt add -A
    dolt commit -m "different fk same name"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`,`v2`) REFERENCES `parent` (`v1`,`v2`)' ]] || false

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
    dolt checkout main

    dolt sql <<SQL
ALTER TABLE child DROP FOREIGN KEY fk_name;
SQL
    run dolt schema show child
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "FOREIGN KEY" ]] || false
    dolt add -A
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
    run dolt reset parent
    [ "$status" -eq "1" ]
    [[ "$output" =~ "parent" ]] || false
    run dolt reset
    [ "$status" -eq "0" ]
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
    dolt checkout main
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
    dolt commit --force -m "updated parent"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM parent ORDER BY id ASC" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1" ]] || false
    [[ "$output" =~ "1,0" ]] || false
    [[ "$output" =~ "2,1" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "$output" =~ "4,3" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
    run dolt sql -q "SELECT * FROM child ORDER BY id ASC" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1" ]] || false
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
    dolt checkout main
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
    dolt commit --force -m "updated parent"
    dolt checkout main
    dolt merge other
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
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
    dolt checkout main
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
    dolt commit --force -m "updated child"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM parent ORDER BY id ASC" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "4,4" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
    run dolt sql -q "SELECT * FROM child ORDER BY id ASC" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1" ]] || false
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
    dolt checkout main
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
    dolt commit --force -m "updated child"
    dolt checkout main
    dolt merge other
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
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
    dolt checkout main
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
    dolt commit --force -m "updated both"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM parent ORDER BY id ASC" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1" ]] || false
    [[ "$output" =~ "1,0" ]] || false
    [[ "$output" =~ "2,1" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "$output" =~ "4,3" ]] || false
    [[ "$output" =~ "5,4" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
    run dolt sql -q "SELECT * FROM child ORDER BY id ASC" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1" ]] || false
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
    dolt checkout main
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
    dolt commit --force -m "updated both"
    dolt checkout main
    dolt merge other
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
}

@test "foreign-keys: Resolve catches violations" {
    dolt sql <<SQL
ALTER TABLE child ADD CONSTRAINT fk_v1 FOREIGN KEY (v1) REFERENCES parent(v1);
INSERT INTO parent VALUES (0,0,0);
INSERT INTO child VALUES (0,0,0);
SQL
    dolt add -A
    dolt commit -m "added tables"
    dolt branch other
    dolt sql <<SQL
INSERT INTO parent VALUES (1,1,1);
INSERT INTO child VALUES (1,1,1);
SQL
    dolt add -A
    dolt commit -m "added 1s"
    dolt checkout other
    dolt sql <<SQL
INSERT INTO parent VALUES (1,2,2);
INSERT INTO child VALUES (1,2,2);
SQL
    dolt add -A
    dolt commit -m "added 2s"
    dolt checkout main
    dolt merge other
    run dolt conflicts resolve --theirs parent
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    run dolt conflicts resolve --theirs child
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
}

@test "foreign-keys: FKs move with the working set on checkout" {
    dolt add . && dolt commit -m "added parent and child tables"
    dolt branch other
    dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_v1 FOREIGN KEY (v1) REFERENCES parent(v1);"

    run dolt checkout other
    [ "$status" -eq "0" ]

    run dolt schema show child
    [ "$status" -eq "0" ]
    skip "foreign keys don't travel with the working set when checking out a new branch"
    [[ "$output" =~ "fk_v1" ]] || false
}

@test "foreign-keys: extended names supported" {
    dolt sql <<SQL
CREATE TABLE parent2 (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  INDEX idx_v1 (v1)
);
CREATE TABLE child2 (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  INDEX idx_v1 (v1),
  CONSTRAINT circuits_123abc4d_fk_circuits_ FOREIGN KEY (pk) REFERENCES parent2 (pk)
);
ALTER TABLE child2 ADD CONSTRAINT \`\$not-possible-before_\` FOREIGN KEY (v1) REFERENCES parent2 (v1);
SQL
    run dolt schema show child2
    [ "$status" -eq "0" ]
    [[ "$output" =~ "circuits_123abc4d_fk_circuits_" ]] || false
    [[ "$output" =~ '`$not-possible-before_`' ]] || false
}

@test "foreign-keys: self-referential same column(s)" {
    # We differ from MySQL here as we do not allow duplicate indexes (required in MySQL to reference the same
    # column in self-referential) but we do reuse existing indexes (MySQL requires unique indexes for parent and
    # child rows).
    dolt sql <<SQL
CREATE INDEX v1v2 ON parent(v1, v2);
SQL
    run dolt sql -q "ALTER TABLE parent ADD CONSTRAINT fk_name1 FOREIGN KEY (v1) REFERENCES parent(v1);"
    [ "$status" -eq "0" ]
    run dolt sql -q "ALTER TABLE parent ADD CONSTRAINT fk_name2 FOREIGN KEY (v1, v2) REFERENCES parent(v1, v2);"
    [ "$status" -eq "0" ]
}

@test "foreign-keys: self-referential child column follows parent RESTRICT" {
    # default reference option is RESTRICT
    dolt sql <<SQL
ALTER TABLE parent ADD CONSTRAINT fk_named FOREIGN KEY (v2) REFERENCES parent(v1);
INSERT INTO parent VALUES (1,1,1), (2, 2, 1), (3, 3, NULL);
UPDATE parent SET v1 = 1 WHERE id = 1;
UPDATE parent SET v1 = 4 WHERE id = 3;
DELETE FROM parent WHERE id = 3;
SQL
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1,v2" ]] || false
    [[ "$output" =~ "1,1,1" ]] || false
    [[ "$output" =~ "2,2,1" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    run dolt sql -q "DELETE FROM parent WHERE v1 = 1;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    run dolt sql -q "UPDATE parent SET v1 = 2;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    run dolt sql -q "REPLACE INTO parent VALUES (1, 1, 1);"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
}

@test "foreign-keys: self-referential child column follows parent CASCADE" {
    dolt sql <<SQL
ALTER TABLE parent ADD CONSTRAINT fk_named FOREIGN KEY (v2) REFERENCES parent(v1) ON UPDATE CASCADE ON DELETE CASCADE;
INSERT INTO parent VALUES (1,1,1), (2, 2, 1), (3, 3, NULL);
UPDATE parent SET v1 = 1 WHERE id = 1;
UPDATE parent SET v1 = 4 WHERE id = 3;
DELETE FROM parent WHERE id = 3;
SQL

    run dolt sql -q "UPDATE parent SET v1 = 2;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    dolt sql -q "REPLACE INTO parent VALUES (1, 1, 1), (2, 2, 2);"
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1,v2" ]] || false
    [[ "$output" =~ "1,1,1" ]] || false
    [[ "$output" =~ "2,2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    run dolt sql -q "UPDATE parent SET v1 = 2;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    run dolt sql -q "UPDATE parent SET v1 = 2 WHERE id = 1;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    run dolt sql -q "REPLACE INTO parent VALUES (1,1,2), (2,2,1);"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false

    dolt sql <<SQL
UPDATE parent SET v2 = 2 WHERE id = 1;
UPDATE parent SET v2 = 1 WHERE id = 2;
SQL
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1,v2" ]] || false
    [[ "$output" =~ "1,1,2" ]] || false
    [[ "$output" =~ "2,2,1" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    # Seems like it should work but this is what MySQL does
    run dolt sql -q "UPDATE parent SET v1 = 2;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    run dolt sql -q "UPDATE parent SET v1 = 2 WHERE id = 1;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false

    dolt sql -q "DELETE FROM parent WHERE v1 = 1;"
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1,v2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "foreign-keys: self-referential child column follows parent SET NULL" {
    dolt sql <<SQL
ALTER TABLE parent ADD CONSTRAINT fk_named FOREIGN KEY (v2) REFERENCES parent(v1) ON UPDATE SET NULL ON DELETE SET NULL;
INSERT INTO parent VALUES (1,1,1), (2, 2, 1), (3, 3, NULL);
UPDATE parent SET v1 = 1 WHERE id = 1;
UPDATE parent SET v1 = 4 WHERE id = 3;
DELETE FROM parent WHERE id = 3;
SQL

    run dolt sql -q "UPDATE parent SET v1 = 2;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    dolt sql -q "REPLACE INTO parent VALUES (1, 1, 1), (2, 2, 2);"
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1,v2" ]] || false
    [[ "$output" =~ "1,1,1" ]] || false
    [[ "$output" =~ "2,2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    run dolt sql -q "UPDATE parent SET v1 = 2;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    run dolt sql -q "UPDATE parent SET v1 = 2 WHERE id = 1;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    dolt sql -q "REPLACE INTO parent VALUES (1,1,2), (2,2,1);"
    run dolt sql -q "SELECT * FROM parent ORDER BY 1" -r=json
    [ "$status" -eq "0" ]
    [[ "$output" =~ '{"rows": [{"id":1,"v1":1},{"id":2,"v1":2,"v2":1}]}' ]] || false

    dolt sql <<SQL
UPDATE parent SET v2 = 2 WHERE id = 1;
UPDATE parent SET v2 = 1 WHERE id = 2;
SQL
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1,v2" ]] || false
    [[ "$output" =~ "1,1,2" ]] || false
    [[ "$output" =~ "2,2,1" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    # Seems like it should work but this is what MySQL does
    run dolt sql -q "UPDATE parent SET v1 = 2;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    run dolt sql -q "UPDATE parent SET v1 = 2 WHERE id = 1;"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false

    dolt sql -q "DELETE FROM parent WHERE v1 = 1;"
    run dolt sql -q "SELECT * FROM parent" -r=json
    [ "$status" -eq "0" ]
    [[ "$output" =~ '{"rows": [{"id":2,"v1":2}]}' ]] || false
}

@test "foreign-keys: self referential foreign keys do not break committing" {
    dolt sql <<SQL
CREATE TABLE test (id char(32) NOT NULL PRIMARY KEY);
ALTER TABLE test ADD COLUMN new_col char(32) NULL;
ALTER TABLE test ADD CONSTRAINT fk_test FOREIGN KEY (new_col) REFERENCES test(id);
SQL
    dolt add -A
    dolt commit -m "committed"
}

@test "foreign-keys: deleting and readding" {
    dolt sql <<SQL
CREATE TABLE parent2 (
  pk BIGINT PRIMARY KEY
);
CREATE TABLE child2 (
  pk BIGINT PRIMARY KEY,
  CONSTRAINT child2_fk FOREIGN KEY (pk) references parent2 (pk)
);
SQL
    dolt add -A
    dolt commit -m "parent2 and child2"
    dolt sql -q "DROP TABLE child2"
    dolt commit -am "drop child"
    dolt sql <<SQL
CREATE TABLE child2 (
  pk BIGINT PRIMARY KEY,
  CONSTRAINT child2_fk FOREIGN KEY (pk) references parent2 (pk)
);
SQL
    dolt add -A
    dolt commit -m "new child"
    run dolt schema show child2
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'child2_fk' ]] || false
}

@test "foreign-keys: child violation correctly detected" {
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
    CONSTRAINT color_fk FOREIGN KEY (color) REFERENCES colors(color)
);
INSERT INTO colors (id,color) VALUES (1,'red'),(2,'green'),(3,'blue'),(4,'purple');
INSERT INTO objects (id,name,color) VALUES (1,'truck','red'),(2,'ball','green'),(3,'shoe','blue');
SQL

    # Run a query and assert that no changes were made
    run dolt sql -q "DELETE FROM colors where color='green'"
    [ "$status" -eq "1" ]
    [[ "$output" =~ 'cannot delete or update a parent row' ]] || false

    run dolt sql -r csv -q "SELECT * FROM colors"
    [ "$status" -eq "0" ]
    [[ $output =~ 'id,color' ]] || false
    [[ "$output" =~ '1,red' ]] || false
    [[ "$output" =~ '2,green' ]] || false
    [[ "$output" =~ '3,blue' ]] || false
    [[ "$output" =~ '4,purple' ]] || false

    run dolt sql -r csv -q "SELECT COUNT(*) FROM colors"
    [ "$status" -eq "0" ]
    [[ $output =~ '4' ]] || false
}

@test "foreign-keys: insert ignore into works correctly w/ FK violations" {
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
    CONSTRAINT color_fk FOREIGN KEY (color) REFERENCES colors(color)
);
INSERT INTO colors (id,color) VALUES (1,'red'),(2,'green'),(3,'blue'),(4,'purple');
INSERT INTO objects (id,name,color) VALUES (1,'truck','red'),(2,'ball','green'),(3,'shoe','blue');
SQL

    # Run a query and assert that no changes were made
    run dolt sql -q "INSERT IGNORE INTO objects (id,name,color) VALUES (5, 'hi', 'yellow');"
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'Query OK, 0 rows affected' ]] || false

    # Validate the data is correct
    run dolt sql -q "SELECT * FROM objects ORDER BY id" -r csv
    [ "$status" -eq "0" ]
    [[ $output =~ 'id,name,color' ]] || false
    [[ "$output" =~ '1,truck,red' ]] || false
    [[ "$output" =~ '2,ball,green' ]] || false
    [[ "$output" =~ '3,shoe,blue' ]] || false

    # Run the query again and this time assert warnings
    run dolt sql  <<SQL
INSERT IGNORE INTO objects (id,name,color) VALUES (5, 'hi', 'yellow');
SHOW WARNINGS;
SQL
    [ "$status" -eq "0" ]
    [[ "$output" =~ '1452' ]] || false # first ensure the proper code
    [[ "$output" =~ 'cannot add or update a child row - Foreign key violation on fk: `color_fk`, table: `objects`, referenced table: `colors`, key: `[yellow]`' ]] || false
}

@test "foreign-keys: updating to null works as expected in commit" {
    dolt sql -q "create table unprocessed_t (id int primary key);"
    dolt sql -q "create table additional_t (id int primary key);"
    dolt sql <<SQL
create table t (
  id int primary key,
  unprocessed_id int,
  foreign key (unprocessed_id) references unprocessed_t(id) on delete cascade on update cascade,
  additional_id int,
  foreign key (additional_id) references additional_t(id) on delete cascade on update cascade
);
SQL

    dolt add .
    dolt commit -m 'schema'

    dolt sql -q 'insert into additional_t values (20)'
    dolt sql -q 'insert into t (id, additional_id) values (1,20);'
    dolt add .
    dolt commit -m 'initial'

    dolt sql -q 'insert into unprocessed_t values (20)'
    dolt sql -q 'update t set additional_id = null, unprocessed_id = 20 where id = 1'
    dolt sql -q 'delete from additional_t'
    dolt add .
    dolt commit -m 'this should not break'
}

@test "foreign-keys: dolt table import with null in nullable FK field should work (issue #2108)" {
    dolt sql <<SQL
CREATE TABLE naics (
  naics_2017 char(6) NOT NULL,
  PRIMARY KEY (naics_2017)
);
CREATE TABLE businesses (
  name varchar(180) NOT NULL,
  naics_2017 char(6),
  PRIMARY KEY (name),
  KEY naics_2017 (naics_2017),
  CONSTRAINT naics FOREIGN KEY (naics_2017) REFERENCES naics (naics_2017)
);
INSERT INTO naics VALUES ("100");
SQL

    echo $'name,naics_2017\n"test",\n"test2","100"' > fk_test.csv

    run dolt table import -u businesses fk_test.csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'Rows Processed: 2, Additions: 2, Modifications: 0, Had No Effect: 0' ]] || false

    run dolt sql -r csv -q "SELECT * FROM businesses order by name"
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'name,naics_2017' ]] || false
    [[ "$output" =~ 'test,' ]] || false
    [[ "$output" =~ 'test2,100' ]] || false
}

@test "foreign-keys: Delayed foreign key resolution" {
    dolt sql <<SQL
SET FOREIGN_KEY_CHECKS=0;
CREATE TABLE delayed_child (
  pk int PRIMARY KEY,
  v1 int,
  CONSTRAINT fk_delayed FOREIGN KEY (v1) REFERENCES delayed_parent(v1)
);
CREATE TABLE delayed_parent (
  pk int PRIMARY KEY,
  v1 int,
  INDEX (v1)
);
INSERT INTO delayed_child VALUES (1, 2);
SET FOREIGN_KEY_CHECKS=1;
SQL
    run dolt schema show delayed_child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "fk_delayed" ]] || false
    run dolt sql -q "SELECT * FROM delayed_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM delayed_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    run dolt sql -q "INSERT INTO delayed_child VALUES (2, 3);"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "foreign key constraint fails" ]] || false
    [[ "$output" =~ "fk_delayed" ]] || false
    dolt sql <<SQL
INSERT INTO delayed_parent VALUES (1, 2), (2, 3);
INSERT INTO delayed_child VALUES (2, 3);
SQL
    run dolt sql -q "SELECT * FROM delayed_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "2,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "foreign-keys: Delayed foreign key resolution through file" {
    # Generally foreign keys are disabled when importing a MySQL dump so this tests that exact scenario
    # Should logically be the same as the above test but you never know what may change
    cat <<SQL > delayed.sql
SET FOREIGN_KEY_CHECKS=0;
CREATE TABLE delayed_child (
  pk int PRIMARY KEY,
  v1 int,
  CONSTRAINT fk_delayed FOREIGN KEY (v1) REFERENCES delayed_parent(v1)
);
CREATE TABLE delayed_parent (
  pk int PRIMARY KEY,
  v1 int,
  INDEX (v1)
);
INSERT INTO delayed_child VALUES (1, 2);
SET FOREIGN_KEY_CHECKS=1;
SQL
    dolt sql < delayed.sql
    run dolt schema show delayed_child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "fk_delayed" ]] || false
    run dolt sql -q "SELECT * FROM delayed_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM delayed_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    run dolt sql -q "INSERT INTO delayed_child VALUES (2, 3);"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "foreign key constraint fails" ]] || false
    [[ "$output" =~ "fk_delayed" ]] || false
    dolt sql <<SQL
INSERT INTO delayed_parent VALUES (1, 2), (2, 3);
INSERT INTO delayed_child VALUES (2, 3);
SQL
    run dolt sql -q "SELECT * FROM delayed_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "2,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "foreign-keys: Delayed foreign key resolution resetting FOREIGN_KEY_CHECKS" {
    dolt sql <<SQL
SET FOREIGN_KEY_CHECKS=0;
CREATE TABLE delayed_child (
  pk int PRIMARY KEY,
  v1 int,
  CONSTRAINT fk_delayed FOREIGN KEY (v1) REFERENCES delayed_parent(v1)
);
INSERT INTO delayed_child VALUES (1, 2);
SET FOREIGN_KEY_CHECKS=1;
SQL
    run dolt schema show delayed_child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "fk_delayed" ]] || false
    run dolt sql -q "SELECT * FROM delayed_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    run dolt sql -q "INSERT INTO delayed_child VALUES (2, 3);"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "foreign key constraint fails" ]] || false
    [[ "$output" =~ "fk_delayed" ]] || false
    dolt sql <<SQL
CREATE TABLE delayed_parent (
  pk int PRIMARY KEY,
  v1 int,
  INDEX (v1)
);
INSERT INTO delayed_parent VALUES (1, 2), (2, 3);
INSERT INTO delayed_child VALUES (2, 3);
SQL
    run dolt sql -q "SELECT * FROM delayed_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "2,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "foreign-keys: DROP TABLE with FOREIGN_KEY_CHECKS=0" {
    dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_dropped FOREIGN KEY (v1) REFERENCES parent(v1)"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'fk_dropped' ]] || false

    run dolt sql -q "DROP TABLE parent"
    [ "$status" -eq "1" ]
    dolt sql <<SQL
SET FOREIGN_KEY_CHECKS=0;
DROP TABLE PARENT;
SET FOREIGN_KEY_CHECKS=1;
SQL
    run dolt sql -q "INSERT INTO child VALUES (4, 5, 6);"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "foreign key constraint fails" ]] || false
    [[ "$output" =~ "fk_dropped" ]] || false
    dolt sql <<SQL
CREATE TABLE parent (pk INT PRIMARY KEY, v1 INT, INDEX (v1));
INSERT INTO parent VALUES (1, 5);
INSERT INTO child VALUES (4, 5, 6);
SQL
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,5" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,v1,v2" ]] || false
    [[ "$output" =~ "4,5,6" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "foreign-keys: alter table add constraint for different database" {
    run dolt sql  <<SQL
CREATE DATABASE public;
CREATE TABLE public.cities (pk integer NOT NULL, city varchar(255), state varchar(2));
CREATE TABLE public.states (state_id integer NOT NULL, state varchar(2));
ALTER TABLE public.cities ADD CONSTRAINT cities_pkey PRIMARY KEY (pk);
ALTER TABLE public.states ADD CONSTRAINT states_pkey PRIMARY KEY (state_id);
ALTER TABLE public.cities ADD CONSTRAINT foreign_key1 FOREIGN KEY (state) REFERENCES public.states(state);
SQL
    [ $status -eq 1 ]
    [[ $output =~ "missing index for foreign key" ]] || false

    run dolt sql -q "SHOW CREATE TABLE public.cities"
    [[ $output =~ "PRIMARY KEY (\`pk\`)" ]] || false
    [[ ! $output =~ "CONSTRAINT" ]] || false

    run dolt sql -q "SHOW CREATE TABLE public.states"
    [[ $output =~ "PRIMARY KEY (\`state_id\`)" ]] || false
    [[ ! $output =~ "KEY \`foreign_key1\` (\`state\`)" ]] || false

    run dolt sql <<SQL
CREATE INDEX foreign_key1 ON public.states(state);
ALTER TABLE public.cities ADD CONSTRAINT foreign_key1 FOREIGN KEY (state) REFERENCES public.states(state);
SQL
    [ $status -eq 0 ]

    run dolt sql -q "SHOW CREATE TABLE public.cities"
    [[ $output =~ "CONSTRAINT \`foreign_key1\` FOREIGN KEY (\`state\`) REFERENCES \`states\` (\`state\`)" ]] || false

    run dolt sql -q "SHOW CREATE TABLE public.states"
    [[ $output =~ "KEY \`foreign_key1\` (\`state\`)" ]] || false
}

@test "foreign-keys: alter table add constraint with foreign key check is off" {
    skip "Add foreign key constraint statement is ignored instead of failing"
    run dolt sql  <<SQL
set foreign_key_checks = off;
CREATE DATABASE public;
CREATE TABLE public.cities (pk integer NOT NULL, city varchar(255), state varchar(2));
CREATE TABLE public.states (state_id integer NOT NULL, state varchar(2));
ALTER TABLE public.cities ADD CONSTRAINT cities_pkey PRIMARY KEY (pk);
ALTER TABLE public.states ADD CONSTRAINT states_pkey PRIMARY KEY (state_id);
ALTER TABLE public.cities ADD CONSTRAINT foreign_key1 FOREIGN KEY (state) REFERENCES public.states(state);
SQL
    [ $status -eq 1 ]
    [[ $output =~ "missing index for constraint" ]] || false

    run dolt sql -q "SHOW CREATE TABLE public.cities"
    [[ $output =~ "PRIMARY KEY (\`pk\`)" ]] || false
    [[ ! $output =~ "CONSTRAINT" ]] || false

    run dolt sql -q "SHOW CREATE TABLE public.states"
    [[ $output =~ "PRIMARY KEY (\`state_id\`)" ]] || false
    [[ ! $output =~ "KEY \`foreign_key1\` (\`state\`)" ]] || false

    run dolt sql <<SQL
CREATE INDEX foreign_key1 ON public.states(state);
ALTER TABLE public.cities ADD CONSTRAINT foreign_key1 FOREIGN KEY (state) REFERENCES public.states(state);
SQL
    [ $status -eq 0 ]

    run dolt sql -q "SHOW CREATE TABLE public.cities"
    [[ $output =~ "CONSTRAINT \`foreign_key1\` FOREIGN KEY (\`state\`) REFERENCES \`states\` (\`state\`)" ]] || false

    run dolt sql -q "SHOW CREATE TABLE public.states"
    [[ $output =~ "KEY \`foreign_key1\` (\`state\`)" ]] || false
}

@test "foreign-keys: creating a foreign key constraint on a table with an unsupported type works" {

    # https://github.com/dolthub/dolt/issues/3023
    dolt sql <<SQL
CREATE TABLE IF NOT EXISTS restaurants (
    id INT PRIMARY KEY,
    coordinate POINT
);
CREATE TABLE IF NOT EXISTS hours (
    restaurant_id INT PRIMARY KEY AUTO_INCREMENT,
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(id)
);
SQL

    run dolt sql -q "show create table hours";
    [ $status -eq 0 ]
    [[ $output =~ "FOREIGN KEY (\`restaurant_id\`) REFERENCES \`restaurants\` (\`id\`)" ]] || false
}

@test "foreign-keys: create foreign key onto primary key" {
    dolt sql <<SQL
    drop table child;
    drop table parent;
    create table parent (a int, b int, c int, primary key (b,a));
    create table child (a int primary key, b int);
SQL

    # ok, it's a prefix
    dolt sql -q "alter table child add constraint fk1 foreign key (b) references parent (b)"

    # not a prefix
    run dolt sql -q "alter table child add constraint fk2 foreign key (a) references parent (a)"

    # not a prefix
    run dolt sql -q "alter table child add constraint fk3 foreign key (a,b) references parent (a,b)"

    # ok
    run dolt sql -q "alter table child add constraint fk4 foreign key (b,a) references parent (b,a)"
    [ $status -eq 0 ]

    # the prefix key should not be unique
    run dolt sql -q "show create table parent"
    [ $status -eq 0 ]
    [[ $output =~ "KEY \`b\` (\`b\`)" ]] || false
    [[ ! $output =~ "UNIQUE" ]] || false

    run dolt sql -q "show create table child"
    [ $status -eq 0 ]
    [[ $output =~ "CONSTRAINT \`fk1\` FOREIGN KEY (\`b\`) REFERENCES \`parent\` (\`b\`)" ]] || false
    [[ $output =~ "CONSTRAINT \`fk4\` FOREIGN KEY (\`b\`,\`a\`) REFERENCES \`parent\` (\`b\`,\`a\`)" ]] || false
}

@test "foreign-keys: creating a foreign key constraint on a table with rows violating it" {
    dolt sql <<SQL
CREATE TABLE a (
  id int NOT NULL AUTO_INCREMENT,
  a text NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO a (id,a) VALUES (1,'a');
CREATE TABLE b (
  id int NOT NULL AUTO_INCREMENT,
  b text NOT NULL,
  a_id int,
  PRIMARY KEY (id),
  KEY b_a_id (a_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO b (id, b, a_id) VALUES (1, 'b', NULL);
INSERT INTO b (id, b, a_id) VALUES (2, 'b2', 31337);
INSERT INTO b (id, b, a_id) VALUES (3, 'b2', 31337);
INSERT INTO b (id, b, a_id) VALUES (4, 'b2', 31337);
SQL

    run dolt sql -q "ALTER TABLE b ADD CONSTRAINT fk_b_a_id_refs_a FOREIGN KEY (a_id) REFERENCES a (id)";
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic:" ]] || false
    [[ "$output" =~ "cannot add or update a child row - Foreign key violation on fk: \`fk_b_a_id_refs_a\`, table: \`b\`, referenced table: \`a\`, key: \`[31337]\`" ]] || false
}
