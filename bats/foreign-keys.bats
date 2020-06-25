#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "foreign-keys: CREATE TABLE Single Unnamed FOREIGN KEY" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT,
  FOREIGN KEY (parent_id)
    REFERENCES parent(id)
);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(id) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_id) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`)' ]] || false
}

@test "foreign-keys: CREATE TABLE Single Unnamed FOREIGN KEY With Either UPDATE/DELETE" {
    dolt sql <<SQL
CREATE TABLE parent1 (
  id INT PRIMARY KEY
);
CREATE TABLE child1 (
  id INT PRIMARY KEY,
  parent_id INT,
  FOREIGN KEY (parent_id)
    REFERENCES parent1(id)
    ON UPDATE CASCADE
);
CREATE TABLE parent2 (
  id INT PRIMARY KEY
);
CREATE TABLE child2 (
  id INT PRIMARY KEY,
  parent_id INT,
  FOREIGN KEY (parent_id)
    REFERENCES parent2(id)
    ON DELETE CASCADE
);
SQL

    run dolt index ls parent1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(id) HIDDEN" ]] || false
    run dolt index ls child1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_id) HIDDEN" ]] || false
    run dolt index ls parent2
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(id) HIDDEN" ]] || false
    run dolt index ls child2
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_id) HIDDEN" ]] || false
    run dolt schema show child1
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child1_parent1_1` FOREIGN KEY (`parent_id`) REFERENCES `parent1` (`id`) ON UPDATE CASCADE' ]] || false
    run dolt schema show child2
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child2_parent2_1` FOREIGN KEY (`parent_id`) REFERENCES `parent2` (`id`) ON DELETE CASCADE' ]] || false
}

@test "foreign-keys: CREATE TABLE Single Unnamed FOREIGN KEY With Both UPDATE/DELETE" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT,
  FOREIGN KEY (parent_id)
    REFERENCES parent(id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(id) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_id) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE CASCADE ON UPDATE CASCADE' ]] || false
}

@test "foreign-keys: CREATE TABLE Single Named FOREIGN KEY" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT,
  CONSTRAINT fk_named FOREIGN KEY (parent_id)
    REFERENCES parent(id)
);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(id) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_id) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_named` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`)' ]] || false
}

@test "foreign-keys: CREATE TABLE Single Named FOREIGN KEY With Both UPDATE/DELETE" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT,
  CONSTRAINT fk_named FOREIGN KEY (parent_id)
    REFERENCES parent(id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(id) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_id) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_named` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE CASCADE ON UPDATE CASCADE' ]] || false
}

@test "foreign-keys: CREATE TABLE Single Unnamed FOREIGN KEY Multi-column" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY,
  v1 VARCHAR(20),
  v2 DATETIME
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME,
  FOREIGN KEY (parent_v1, parent_v2)
    REFERENCES parent(v1, v2)
);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1, v2) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_v1, parent_v2) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_1` FOREIGN KEY (`parent_v1`,`parent_v2`) REFERENCES `parent` (`v1`,`v2`)' ]] || false
}

@test "foreign-keys: CREATE TABLE Single Named FOREIGN KEY Multi-column" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY,
  v1 VARCHAR(20),
  v2 DATETIME
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME,
  CONSTRAINT fk_name FOREIGN KEY (parent_v1, parent_v2)
    REFERENCES parent(v1, v2)
);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1, v2) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_v1, parent_v2) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`parent_v1`,`parent_v2`) REFERENCES `parent` (`v1`,`v2`)' ]] || false
}

@test "foreign-keys: CREATE TABLE Multiple Unnamed FOREIGN KEY" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY,
  v1 VARCHAR(20),
  v2 DATETIME,
  v3 TIMESTAMP,
  v4 BIT(4)
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME,
  parent_v3 TIMESTAMP,
  parent_v4 BIT(4),
  FOREIGN KEY (parent_v1)
    REFERENCES parent(v1),
  FOREIGN KEY (parent_v2)
    REFERENCES parent(v2)
    ON UPDATE CASCADE,
  FOREIGN KEY (parent_v3)
    REFERENCES parent(v3)
    ON DELETE CASCADE,
  FOREIGN KEY (parent_v4)
    REFERENCES parent(v4)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1) HIDDEN" ]] || false
    [[ "$output" =~ "(v2) HIDDEN" ]] || false
    [[ "$output" =~ "(v3) HIDDEN" ]] || false
    [[ "$output" =~ "(v4) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_v1) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v2) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v3) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v4) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_1` FOREIGN KEY (`parent_v1`) REFERENCES `parent` (`v1`)' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_2` FOREIGN KEY (`parent_v2`) REFERENCES `parent` (`v2`) ON UPDATE CASCADE' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_3` FOREIGN KEY (`parent_v3`) REFERENCES `parent` (`v3`) ON DELETE CASCADE' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_4` FOREIGN KEY (`parent_v4`) REFERENCES `parent` (`v4`) ON DELETE CASCADE ON UPDATE CASCADE' ]] || false
}

@test "foreign-keys: CREATE TABLE Multiple Named FOREIGN KEY" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY,
  v1 VARCHAR(20),
  v2 DATETIME,
  v3 TIMESTAMP,
  v4 BIT(4)
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME,
  parent_v3 TIMESTAMP,
  parent_v4 BIT(4),
  CONSTRAINT fk_name_1 FOREIGN KEY (parent_v1)
    REFERENCES parent(v1),
  CONSTRAINT fk_name_2 FOREIGN KEY (parent_v2)
    REFERENCES parent(v2)
    ON UPDATE CASCADE,
  CONSTRAINT fk_name_3 FOREIGN KEY (parent_v3)
    REFERENCES parent(v3)
    ON DELETE CASCADE,
  CONSTRAINT fk_name_4 FOREIGN KEY (parent_v4)
    REFERENCES parent(v4)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1) HIDDEN" ]] || false
    [[ "$output" =~ "(v2) HIDDEN" ]] || false
    [[ "$output" =~ "(v3) HIDDEN" ]] || false
    [[ "$output" =~ "(v4) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_v1) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v2) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v3) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v4) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name_1` FOREIGN KEY (`parent_v1`) REFERENCES `parent` (`v1`)' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name_2` FOREIGN KEY (`parent_v2`) REFERENCES `parent` (`v2`) ON UPDATE CASCADE' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name_3` FOREIGN KEY (`parent_v3`) REFERENCES `parent` (`v3`) ON DELETE CASCADE' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name_4` FOREIGN KEY (`parent_v4`) REFERENCES `parent` (`v4`) ON DELETE CASCADE ON UPDATE CASCADE' ]] || false
}

@test "foreign-keys: CREATE TABLE Name Collision" {
    run dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT,
  CONSTRAINT fk_name FOREIGN KEY (parent_id)
    REFERENCES parent(id),
  CONSTRAINT fk_name FOREIGN KEY (parent_id)
    REFERENCES parent(id)
);
SQL

    [ "$status" -eq "1" ]
    [[ "$output" =~ "already exists" ]] || false
}

@test "foreign-keys: CREATE TABLE Type Mismatch" {
    run dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY,
  v1 VARCHAR(20),
  v2 DATETIME
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME,
  FOREIGN KEY (parent_v2, parent_v1)
    REFERENCES parent(v1, v2)
);
SQL
    [ "$status" -eq "1" ]
    #TODO: match on error detailing our specific decision to force type parity
}

@test "foreign-keys: CREATE TABLE Key Count Mismatch" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY,
  v1 VARCHAR(20),
  v2 DATETIME
);
SQL

    run dolt sql <<SQL
CREATE TABLE child1 (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME,
  FOREIGN KEY (parent_v1)
    REFERENCES parent(v1, v2)
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "number of columns" ]] || false

    run dolt sql <<SQL
CREATE TABLE child2 (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME,
  FOREIGN KEY (parent_v1, parent_v2)
    REFERENCES parent(v1)
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "number of columns" ]] || false
}

@test "foreign-keys: CREATE TABLE UPDATE/DELETE Options" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY,
  v1 VARCHAR(20),
  v2 DATETIME,
  v3 TIMESTAMP,
  v4 BIT(4),
  v5 BIGINT UNSIGNED
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME,
  parent_v3 TIMESTAMP,
  parent_v4 BIT(4),
  parent_v5 BIGINT UNSIGNED,
  FOREIGN KEY (parent_v1)
    REFERENCES parent(v1),
  FOREIGN KEY (parent_v2)
    REFERENCES parent(v2)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  FOREIGN KEY (parent_v3)
    REFERENCES parent(v3)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  FOREIGN KEY (parent_v4)
    REFERENCES parent(v4)
    ON DELETE SET NULL
    ON UPDATE SET NULL,
  FOREIGN KEY (parent_v5)
    REFERENCES parent(v5)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1) HIDDEN" ]] || false
    [[ "$output" =~ "(v2) HIDDEN" ]] || false
    [[ "$output" =~ "(v3) HIDDEN" ]] || false
    [[ "$output" =~ "(v4) HIDDEN" ]] || false
    [[ "$output" =~ "(v5) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_v1) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v2) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v3) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v4) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v5) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_1` FOREIGN KEY (`parent_v1`) REFERENCES `parent` (`v1`)' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_2` FOREIGN KEY (`parent_v2`) REFERENCES `parent` (`v2`) ON DELETE RESTRICT ON UPDATE RESTRICT' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_3` FOREIGN KEY (`parent_v3`) REFERENCES `parent` (`v3`) ON DELETE CASCADE ON UPDATE CASCADE' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_4` FOREIGN KEY (`parent_v4`) REFERENCES `parent` (`v4`) ON DELETE SET NULL ON UPDATE SET NULL' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_5` FOREIGN KEY (`parent_v5`) REFERENCES `parent` (`v5`) ON DELETE NO ACTION ON UPDATE NO ACTION' ]] || false

    run dolt sql <<SQL
CREATE TABLE child2 (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  FOREIGN KEY (parent_v1)
    REFERENCES parent(v1)
    ON DELETE SET DEFAULT
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "\"SET DEFAULT\" is not supported" ]] || false

    run dolt sql <<SQL
CREATE TABLE child3 (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  FOREIGN KEY (parent_v1)
    REFERENCES parent(v1)
    ON UPDATE SET DEFAULT
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "\"SET DEFAULT\" is not supported" ]] || false

    run dolt sql <<SQL
CREATE TABLE child4 (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  FOREIGN KEY (parent_v1)
    REFERENCES parent(v1)
    ON DELETE SET DEFAULT
    ON UPDATE SET DEFAULT
);
SQL
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
    run dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT,
  FOREIGN KEY (parent_id)
    REFERENCES father(id)
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "`father` does not exist" ]] || false
}

@test "foreign-keys: CREATE TABLE Non-existent Columns" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
SQL

    run dolt sql <<SQL
CREATE TABLE child1 (
  id INT PRIMARY KEY,
  parent_id INT,
  FOREIGN KEY (random)
    REFERENCES parent(id)
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "does not have column" ]] || false

    run dolt sql <<SQL
CREATE TABLE child2 (
  id INT PRIMARY KEY,
  parent_id INT,
  FOREIGN KEY (parent_id)
    REFERENCES parent(random)
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "does not have column" ]] || false
}

@test "foreign-keys: ALTER TABLE Single Unnamed FOREIGN KEY" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT
);
ALTER TABLE child ADD FOREIGN KEY (parent_id) REFERENCES parent(id);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(id) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_id) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`)' ]] || false
}

@test "foreign-keys: ALTER TABLE Single Unnamed FOREIGN KEY With Either UPDATE/DELETE" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT
);
ALTER TABLE child ADD FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE;
ALTER TABLE child ADD FOREIGN KEY (parent_id) REFERENCES parent(id) ON UPDATE CASCADE;
SQL

    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE CASCADE' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_2` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON UPDATE CASCADE' ]] || false
}

@test "foreign-keys: ALTER TABLE Single Unnamed FOREIGN KEY With Both UPDATE/DELETE" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT
);
ALTER TABLE child ADD FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE CASCADE;
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(id) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_id) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE CASCADE ON UPDATE CASCADE' ]] || false
}

@test "foreign-keys: ALTER TABLE Single Named FOREIGN KEY" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT
);
ALTER TABLE child ADD CONSTRAINT fk_named FOREIGN KEY (parent_id) REFERENCES parent(id);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(id) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_id) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_named` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`)' ]] || false
}

@test "foreign-keys: ALTER TABLE Single Named FOREIGN KEY With Both UPDATE/DELETE" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT
);
ALTER TABLE child ADD CONSTRAINT fk_named FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE CASCADE;
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(id) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_id) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_named` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE CASCADE ON UPDATE CASCADE' ]] || false
}

@test "foreign-keys: ALTER TABLE Single Unnamed FOREIGN KEY Multi-column" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY,
  v1 VARCHAR(20),
  v2 DATETIME
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME
);
ALTER TABLE child ADD FOREIGN KEY (parent_v1, parent_v2) REFERENCES parent(v1, v2);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1, v2) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_v1, parent_v2) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_1` FOREIGN KEY (`parent_v1`,`parent_v2`) REFERENCES `parent` (`v1`,`v2`)' ]] || false
}

@test "foreign-keys: ALTER TABLE Single Named FOREIGN KEY Multi-column" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY,
  v1 VARCHAR(20),
  v2 DATETIME
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME
);
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (parent_v1, parent_v2) REFERENCES parent(v1, v2);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1, v2) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_v1, parent_v2) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`parent_v1`,`parent_v2`) REFERENCES `parent` (`v1`,`v2`)' ]] || false
}

@test "foreign-keys: ALTER TABLE Multiple Unnamed FOREIGN KEY" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY,
  v1 VARCHAR(20),
  v2 DATETIME,
  v3 TIMESTAMP,
  v4 BIT(4)
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME,
  parent_v3 TIMESTAMP,
  parent_v4 BIT(4)
);
ALTER TABLE child ADD FOREIGN KEY (parent_v1) REFERENCES parent(v1);
ALTER TABLE child ADD FOREIGN KEY (parent_v2) REFERENCES parent(v2) ON UPDATE CASCADE;
ALTER TABLE child ADD FOREIGN KEY (parent_v3) REFERENCES parent(v3) ON DELETE CASCADE;
ALTER TABLE child ADD FOREIGN KEY (parent_v4) REFERENCES parent(v4) ON DELETE CASCADE ON UPDATE CASCADE;
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1) HIDDEN" ]] || false
    [[ "$output" =~ "(v2) HIDDEN" ]] || false
    [[ "$output" =~ "(v3) HIDDEN" ]] || false
    [[ "$output" =~ "(v4) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_v1) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v2) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v3) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v4) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_1` FOREIGN KEY (`parent_v1`) REFERENCES `parent` (`v1`)' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_2` FOREIGN KEY (`parent_v2`) REFERENCES `parent` (`v2`) ON UPDATE CASCADE' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_3` FOREIGN KEY (`parent_v3`) REFERENCES `parent` (`v3`) ON DELETE CASCADE' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_4` FOREIGN KEY (`parent_v4`) REFERENCES `parent` (`v4`) ON DELETE CASCADE ON UPDATE CASCADE' ]] || false
}

@test "foreign-keys: ALTER TABLE Multiple Named FOREIGN KEY" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY,
  v1 VARCHAR(20),
  v2 DATETIME,
  v3 TIMESTAMP,
  v4 BIT(4)
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME,
  parent_v3 TIMESTAMP,
  parent_v4 BIT(4)
);
ALTER TABLE child ADD CONSTRAINT fk_name_1 FOREIGN KEY (parent_v1) REFERENCES parent(v1);
ALTER TABLE child ADD CONSTRAINT fk_name_2 FOREIGN KEY (parent_v2) REFERENCES parent(v2) ON UPDATE CASCADE;
ALTER TABLE child ADD CONSTRAINT fk_name_3 FOREIGN KEY (parent_v3) REFERENCES parent(v3) ON DELETE CASCADE;
ALTER TABLE child ADD CONSTRAINT fk_name_4 FOREIGN KEY (parent_v4) REFERENCES parent(v4) ON DELETE CASCADE ON UPDATE CASCADE;
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1) HIDDEN" ]] || false
    [[ "$output" =~ "(v2) HIDDEN" ]] || false
    [[ "$output" =~ "(v3) HIDDEN" ]] || false
    [[ "$output" =~ "(v4) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_v1) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v2) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v3) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v4) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name_1` FOREIGN KEY (`parent_v1`) REFERENCES `parent` (`v1`)' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name_2` FOREIGN KEY (`parent_v2`) REFERENCES `parent` (`v2`) ON UPDATE CASCADE' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name_3` FOREIGN KEY (`parent_v3`) REFERENCES `parent` (`v3`) ON DELETE CASCADE' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name_4` FOREIGN KEY (`parent_v4`) REFERENCES `parent` (`v4`) ON DELETE CASCADE ON UPDATE CASCADE' ]] || false
}

@test "foreign-keys: ALTER TABLE Name Collision" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT
);
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (parent_id) REFERENCES parent(id);
SQL

    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (parent_id) REFERENCES parent(id)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "already exists" ]] || false
}

@test "foreign-keys: ALTER TABLE Type Mismatch" {
    run dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY,
  v1 VARCHAR(20),
  v2 DATETIME
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME
);
ALTER TABLE child ADD FOREIGN KEY (parent_v2, parent_v1) REFERENCES parent(v1, v2);
SQL

    [ "$status" -eq "1" ]
    #TODO: match on error detailing our specific decision to force type parity
}

@test "foreign-keys: ALTER TABLE Key Count Mismatch" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY,
  v1 VARCHAR(20),
  v2 DATETIME
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME
);
SQL

    run dolt sql -q "ALTER TABLE child ADD FOREIGN KEY (parent_v1) REFERENCES parent(v1, v2)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "number of columns" ]] || false
    run dolt sql -q "ALTER TABLE child ADD FOREIGN KEY (parent_v1, parent_v2) REFERENCES parent(v1)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "number of columns" ]] || false
}

@test "foreign-keys: ALTER TABLE UPDATE/DELETE Options" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY,
  v1 VARCHAR(20),
  v2 DATETIME,
  v3 TIMESTAMP,
  v4 BIT(4),
  v5 BIGINT UNSIGNED
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_v1 VARCHAR(20),
  parent_v2 DATETIME,
  parent_v3 TIMESTAMP,
  parent_v4 BIT(4),
  parent_v5 BIGINT UNSIGNED
);
ALTER TABLE child ADD FOREIGN KEY (parent_v1) REFERENCES parent(v1);
ALTER TABLE child ADD FOREIGN KEY (parent_v2) REFERENCES parent(v2) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE child ADD FOREIGN KEY (parent_v3) REFERENCES parent(v3) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE child ADD FOREIGN KEY (parent_v4) REFERENCES parent(v4) ON DELETE SET NULL ON UPDATE SET NULL;
ALTER TABLE child ADD FOREIGN KEY (parent_v5) REFERENCES parent(v5) ON DELETE NO ACTION ON UPDATE NO ACTION;
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(v1) HIDDEN" ]] || false
    [[ "$output" =~ "(v2) HIDDEN" ]] || false
    [[ "$output" =~ "(v3) HIDDEN" ]] || false
    [[ "$output" =~ "(v4) HIDDEN" ]] || false
    [[ "$output" =~ "(v5) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_v1) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v2) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v3) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v4) HIDDEN" ]] || false
    [[ "$output" =~ "(parent_v5) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_1` FOREIGN KEY (`parent_v1`) REFERENCES `parent` (`v1`)' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_2` FOREIGN KEY (`parent_v2`) REFERENCES `parent` (`v2`) ON DELETE RESTRICT ON UPDATE RESTRICT' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_3` FOREIGN KEY (`parent_v3`) REFERENCES `parent` (`v3`) ON DELETE CASCADE ON UPDATE CASCADE' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_4` FOREIGN KEY (`parent_v4`) REFERENCES `parent` (`v4`) ON DELETE SET NULL ON UPDATE SET NULL' ]] || false
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_5` FOREIGN KEY (`parent_v5`) REFERENCES `parent` (`v5`) ON DELETE NO ACTION ON UPDATE NO ACTION' ]] || false
    run dolt sql -q "ALTER TABLE child ADD FOREIGN KEY (parent_v1) REFERENCES parent(v1) ON DELETE SET DEFAULT"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "\"SET DEFAULT\" is not supported" ]] || false
    run dolt sql -q "ALTER TABLE child ADD FOREIGN KEY (parent_v1) REFERENCES parent(v1) ON UPDATE SET DEFAULT"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "\"SET DEFAULT\" is not supported" ]] || false
    run dolt sql -q "ALTER TABLE child ADD FOREIGN KEY (parent_v1) REFERENCES parent(v1) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "\"SET DEFAULT\" is not supported" ]] || false
}

@test "foreign-keys: ALTER TABLE Disallow TEXT/BLOB" {
    dolt sql <<SQL
CREATE TABLE parent1 (
  id INT PRIMARY KEY,
  v1 TINYTEXT,
  v2 TEXT,
  v3 MEDIUMTEXT,
  v4 LONGTEXT
);
CREATE TABLE child11 (
  id INT PRIMARY KEY,
  parent_v1 TINYTEXT
);
CREATE TABLE child12 (
  id INT PRIMARY KEY,
  parent_v2 TEXT
);
CREATE TABLE child13 (
  id INT PRIMARY KEY,
  parent_v3 MEDIUMTEXT
);
CREATE TABLE child14 (
  id INT PRIMARY KEY,
  parent_v4 LONGTEXT
);
SQL

    run dolt sql -q "ALTER TABLE child11 ADD FOREIGN KEY (parent_v1) REFERENCES parent1(v1)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false
    run dolt sql -q "ALTER TABLE child12 ADD FOREIGN KEY (parent_v2) REFERENCES parent1(v2)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false
    run dolt sql -q "ALTER TABLE child13 ADD FOREIGN KEY (parent_v3) REFERENCES parent1(v3)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false
    run dolt sql -q "ALTER TABLE child14 ADD FOREIGN KEY (parent_v4) REFERENCES parent1(v4)"
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
CREATE TABLE child21 (
  id INT PRIMARY KEY,
  parent_v1 TINYBLOB
);
CREATE TABLE child22 (
  id INT PRIMARY KEY,
  parent_v2 BLOB
);
CREATE TABLE child23 (
  id INT PRIMARY KEY,
  parent_v3 MEDIUMBLOB
);
CREATE TABLE child24 (
  id INT PRIMARY KEY,
  parent_v4 LONGBLOB
);
SQL

    run dolt sql -q "ALTER TABLE child21 ADD FOREIGN KEY (parent_v1) REFERENCES parent2(v1)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false
    run dolt sql -q "ALTER TABLE child22 ADD FOREIGN KEY (parent_v2) REFERENCES parent2(v2)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false
    run dolt sql -q "ALTER TABLE child23 ADD FOREIGN KEY (parent_v3) REFERENCES parent2(v3)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false
    run dolt sql -q "ALTER TABLE child24 ADD FOREIGN KEY (parent_v4) REFERENCES parent2(v4)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "not valid type" ]] || false
}

@test "foreign-keys: ALTER TABLE Non-existent Table" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT
);
SQL

    run dolt sql -q "ALTER TABLE child ADD FOREIGN KEY (parent_id) REFERENCES father(id)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "`father` does not exist" ]] || false
}

@test "foreign-keys: ALTER TABLE Non-existent Columns" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT
);
SQL

    run dolt sql -q "ALTER TABLE child ADD FOREIGN KEY (random) REFERENCES parent(id)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "does not have column" ]] || false
    run dolt sql -q "ALTER TABLE child ADD FOREIGN KEY (parent_id) REFERENCES parent(random)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "does not have column" ]] || false
}

@test "foreign-keys: ALTER TABLE DROP FOREIGN KEY" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT,
  CONSTRAINT fk_name FOREIGN KEY (parent_id)
    REFERENCES parent(id)
);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(id) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_id) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'CONSTRAINT `fk_name`' ]] || false

    dolt sql -q "ALTER TABLE child DROP FOREIGN KEY fk_name"
    run dolt index ls parent
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "(id) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "(parent_id) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ 'CONSTRAINT `fk_name`' ]] || false
    run dolt sql -q "ALTER TABLE child DROP FOREIGN KEY fk_name"
    [ "$status" -eq "1" ]
}

@test "foreign-keys: RENAME TABLE" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT,
  CONSTRAINT fk_name FOREIGN KEY (parent_id)
    REFERENCES parent(id)
);
RENAME TABLE parent TO new_parent;
SQL

    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`parent_id`) REFERENCES `new_parent` (`id`)' ]] || false
    dolt sql -q "RENAME TABLE child TO new_child;"
    run dolt schema show new_child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`parent_id`) REFERENCES `new_parent` (`id`)' ]] || false
}

@test "foreign-keys: dolt table mv" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT,
  CONSTRAINT fk_name FOREIGN KEY (parent_id)
    REFERENCES parent(id)
);
SQL

    dolt table mv parent new_parent
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`parent_id`) REFERENCES `new_parent` (`id`)' ]] || false
    dolt table mv child new_child;
    run dolt schema show new_child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`parent_id`) REFERENCES `new_parent` (`id`)' ]] || false
}

@test "foreign-keys: DROP TABLE" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT,
  CONSTRAINT fk_name FOREIGN KEY (parent_id)
    REFERENCES parent(id)
);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(id) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_id) HIDDEN" ]] || false
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'CONSTRAINT `fk_name`' ]] || false
    run dolt sql -q "DROP TABLE parent"
    [ "$status" -eq "1" ]
    dolt sql -q "DROP TABLE child"
    run dolt index ls parent
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "(id) HIDDEN" ]] || false
    dolt sql -q "DROP TABLE parent"
}

@test "foreign-keys: dolt table rm" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT,
  CONSTRAINT fk_name FOREIGN KEY (parent_id)
    REFERENCES parent(id)
);
SQL

    run dolt index ls parent
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(id) HIDDEN" ]] || false
    run dolt index ls child
    [ "$status" -eq "0" ]
    [[ "$output" =~ "(parent_id) HIDDEN" ]] || false
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

@test "foreign-keys: ALTER TABLE RENAME COLUMN" {
    dolt sql <<SQL
CREATE TABLE parent (
  id INT PRIMARY KEY
);
CREATE TABLE child (
  id INT PRIMARY KEY,
  parent_id INT,
  FOREIGN KEY (parent_id)
    REFERENCES parent(id)
);
ALTER TABLE parent RENAME COLUMN id TO id_new;
ALTER TABLE child RENAME COLUMN parent_id TO parent_id_new;
SQL

    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_child_parent_1` FOREIGN KEY (`parent_id_new`) REFERENCES `parent` (`id_new`)' ]] || false
}

@test "foreign-keys: dolt sql" {
    dolt sql <<SQL
CREATE TABLE one (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE two (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  FOREIGN KEY (v1)
    REFERENCES one(v1)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);
CREATE TABLE three (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  FOREIGN KEY (v1, v2)
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
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_two_one_1` FOREIGN KEY (`v1`) REFERENCES `one` (`v1`)' ]] || false
    run dolt schema show three
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_three_two_1` FOREIGN KEY (`v1`,`v2`) REFERENCES `two` (`v1`,`v2`)' ]] || false

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

@test "foreign-keys: dolt table import" {
    dolt sql <<SQL
CREATE TABLE parent (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE child (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  FOREIGN KEY (v1)
    REFERENCES parent(v1)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);
INSERT INTO parent VALUES (1, 1, 1), (2, 2, 2);
INSERT INTO child  VALUES (1, 1, 1), (2, 2, 2);
SQL

    echo $'pk,v1,v2\n1,3,3\n2,4,4' > update_parent.csv
    dolt table import -u parent update_parent.csv
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "1,3,3" ]] || false
    [[ "$output" =~ "2,4,4" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "1,3,1" ]] || false
    [[ "$output" =~ "2,4,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    echo $'pk,v1,v2\n1,1,1\n2,2,2' > update_child.csv
    run dolt table import -u child update_child.csv
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false

    echo $'pk,v1,v2\n3,3,3\n4,4,4' > update_child.csv
    dolt table import -u child update_child.csv
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "1,3,1" ]] || false
    [[ "$output" =~ "2,4,2" ]] || false
    [[ "$output" =~ "3,3,3" ]] || false
    [[ "$output" =~ "4,4,4" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false

    echo $'pk,v1,v2\n1,1,1\n2,2,2' > update_child.csv
    run dolt table import -r child update_child.csv
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false

    echo $'pk,v1,v2\n3,3,3\n4,4,4' > update_child.csv
    dolt table import -r child update_child.csv
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "3,3,3" ]] || false
    [[ "$output" =~ "4,4,4" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}
