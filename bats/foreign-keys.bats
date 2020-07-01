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

@test "foreign-keys: CREATE TABLE SET NULL on non-nullable column" {
    dolt sql <<SQL
CREATE TABLE parent (
  id BIGINT PRIMARY KEY,
  extra BIGINT
);
SQL

    run dolt sql <<SQL
CREATE TABLE child (
  id BIGINT PRIMARY KEY,
  parent_extra BIGINT NOT NULL,
  CONSTRAINT fk_name FOREIGN KEY (parent_extra)
    REFERENCES parent(extra)
    ON DELETE SET NULL
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "SET NULL" ]] || false
    [[ "$output" =~ "parent_extra" ]] || false
    
    run dolt sql <<SQL
CREATE TABLE child (
  id BIGINT PRIMARY KEY,
  parent_extra BIGINT NOT NULL,
  CONSTRAINT fk_name FOREIGN KEY (parent_extra)
    REFERENCES parent(extra)
    ON UPDATE SET NULL
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "SET NULL" ]] || false
    [[ "$output" =~ "parent_extra" ]] || false
    
    run dolt sql <<SQL
CREATE TABLE child (
  id BIGINT PRIMARY KEY,
  parent_extra BIGINT NOT NULL,
  CONSTRAINT fk_name FOREIGN KEY (parent_extra)
    REFERENCES parent(extra)
    ON DELETE SET NULL
    ON UPDATE SET NULL
);
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "SET NULL" ]] || false
    [[ "$output" =~ "parent_extra" ]] || false
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
    [[ "$output" =~ "table not found" ]] || false
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

@test "foreign-keys: ALTER TABLE SET NULL on non-nullable column" {
    dolt sql <<SQL
CREATE TABLE parent (
  id BIGINT PRIMARY KEY,
  extra BIGINT
);
CREATE TABLE child (
  id BIGINT PRIMARY KEY,
  parent_extra BIGINT NOT NULL
);
SQL

    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (parent_extra) REFERENCES parent(extra) ON DELETE SET NULL"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "SET NULL" ]] || false
    [[ "$output" =~ "parent_extra" ]] || false
    
    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (parent_extra) REFERENCES parent(extra) ON UPDATE SET NULL"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "SET NULL" ]] || false
    [[ "$output" =~ "parent_extra" ]] || false
    
    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (parent_extra) REFERENCES parent(extra) ON DELETE SET NULL ON UPDATE SET NULL"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "SET NULL" ]] || false
    [[ "$output" =~ "parent_extra" ]] || false
}

@test "foreign-keys: ADD FOREIGN KEY fails on existing table when data would cause violation" {
    dolt sql <<SQL
CREATE TABLE parent (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT
);
CREATE TABLE child (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT
);
INSERT INTO parent VALUES (1, 1), (2, 2);
INSERT INTO child  VALUES (1, 1), (2, 3);
SQL

    run dolt sql -q "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1)"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    [[ "$output" =~ "fk_name" ]] || false
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

@test "foreign-keys: dolt table cp" {
    dolt sql <<SQL
CREATE TABLE one (
  id BIGINT PRIMARY KEY,
  extra BIGINT
);
CREATE TABLE two (
  id BIGINT PRIMARY KEY,
  one_extra BIGINT,
  FOREIGN KEY (one_extra)
    REFERENCES one(extra)
);
SQL
    
    dolt table cp two two_new
    run dolt index ls two_new
    [ "$status" -eq "0" ]
    [[ "$output" =~ "No indexes" ]] || false
    run dolt schema show two_new
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "FOREIGN KEY" ]] || false
    
    run dolt index ls two
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "No indexes" ]] || false
    run dolt schema show two
    [ "$status" -eq "0" ]
    [[ "$output" =~ "FOREIGN KEY" ]] || false
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

@test "foreign-keys: DROP COLUMN" {
    dolt sql <<SQL
CREATE TABLE parent (
  id BIGINT PRIMARY KEY,
  extra BIGINT
);
CREATE TABLE child (
  id BIGINT PRIMARY KEY,
  parent_extra BIGINT,
  CONSTRAINT fk_name FOREIGN KEY (parent_extra)
    REFERENCES parent(extra)
);
SQL

    dolt add -A
    dolt commit -m "initial commit"
    run dolt sql -q "ALTER TABLE parent DROP COLUMN extra"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "extra" ]] || false
    dolt sql -q "ALTER TABLE child DROP FOREIGN KEY fk_name"
    dolt sql -q "ALTER TABLE parent DROP COLUMN extra"
    
    dolt reset --hard
    run dolt sql -q "ALTER TABLE child DROP COLUMN parent_extra"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "parent_extra" ]] || false
    dolt sql -q "ALTER TABLE child DROP FOREIGN KEY fk_name"
    dolt sql -q "ALTER TABLE child DROP COLUMN parent_extra"
}

@test "foreign-keys: Disallow change column type when SET NULL" {
    dolt sql <<SQL
CREATE TABLE parent (
  id BIGINT PRIMARY KEY,
  extra BIGINT
);
CREATE TABLE child (
  id BIGINT PRIMARY KEY,
  parent_extra BIGINT,
  CONSTRAINT fk_name FOREIGN KEY (parent_extra)
    REFERENCES parent(extra)
    ON DELETE SET NULL
    ON UPDATE SET NULL
);
SQL
    
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
CREATE TABLE two (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  CONSTRAINT fk_name_1 FOREIGN KEY (v1)
    REFERENCES one(v1)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);
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

@test "foreign-keys: Commit all" {
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
    ON UPDATE RESTRICT
);
SQL

    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false
    dolt add -A
    dolt commit -m "has fk"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false

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
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false
    dolt sql -q "rename table parent to super_parent"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'FOREIGN KEY (`v1`) REFERENCES `super_parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false
    dolt add super_parent
    dolt commit -m "renamed parent"
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'FOREIGN KEY (`v1`) REFERENCES `super_parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false
    
    dolt checkout -b last_commit HEAD~1
    dolt reset --hard # See issue https://github.com/liquidata-inc/dolt/issues/752
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`) ON DELETE CASCADE ON UPDATE RESTRICT' ]] || false
    
    dolt checkout master
    run dolt schema show
    [ "$status" -eq "0" ]
    ! [[ "$output" =~ "FOREIGN KEY" ]] || false
}

@test "foreign-keys: Commit then rename parent, child, and columns" {
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
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
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
CREATE TABLE parent (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE child (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
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
CREATE TABLE child (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  CONSTRAINT fk_name FOREIGN KEY (v1,v2)
    REFERENCES parent(v1,v2)
);
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

@test "foreign-keys: Commit then recreate key with parent columns" {
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
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
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
  pk BIGINT PRIMARY KEY,
  vnew BIGINT
);
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(vnew);
SQL
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`vnew`)' ]] || false
    dolt add -A
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
CREATE TABLE parent (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE child (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
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
  pk BIGINT PRIMARY KEY,
  v1 BIGINT
);
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES new_parent(v1);
SQL
    run dolt schema show child
    [ "$status" -eq "0" ]
    [[ `echo "$output" | tr -d "\n" | tr -s " "` =~ 'CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `new_parent` (`v1`)' ]] || false
    dolt add -A
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
CREATE TABLE parent (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE child (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
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
CREATE TABLE parent (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE child (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
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
CREATE TABLE parent (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE child (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT,
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
SQL

    dolt add -A
    dolt reset parent
    run dolt commit -m "will fail since parent is missing"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "parent" ]] || false
}

@test "foreign-keys: Commit, rename parent, commit only child" {
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
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
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
CREATE TABLE parent (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT
);
CREATE TABLE child (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
INSERT INTO parent VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO child VALUES (2, 1), (3, 2), (4, 3);
SQL

    dolt add -A
    dolt commit -m "initial commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql <<SQL
INSERT INTO parent VALUES (4, 3);
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
CREATE TABLE parent (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT
);
CREATE TABLE child (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
INSERT INTO parent VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO child VALUES (2, 1), (3, 2), (4, 3);
SQL

    dolt add -A
    dolt commit -m "initial commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql <<SQL
INSERT INTO parent VALUES (4, 4);
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
    run dolt merge other
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    [[ "$output" =~ "3" ]] || false
}

@test "foreign-keys: Merge valid onto child" {
    dolt sql <<SQL
CREATE TABLE parent (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT
);
CREATE TABLE child (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
INSERT INTO parent VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO child VALUES (2, 1), (3, 2), (4, 3);
SQL

    dolt add -A
    dolt commit -m "initial commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql <<SQL
INSERT INTO parent VALUES (4, 4);
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
CREATE TABLE parent (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT
);
CREATE TABLE child (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
INSERT INTO parent VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO child VALUES (2, 1), (3, 2), (4, 3);
SQL

    dolt add -A
    dolt commit -m "initial commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql <<SQL
INSERT INTO parent VALUES (4, 4);
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
    run dolt merge other
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    [[ "$output" =~ "0" ]] || false
}

@test "foreign-keys: Merge valid onto parent and child" {
    dolt sql <<SQL
CREATE TABLE parent (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT
);
CREATE TABLE child (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
INSERT INTO parent VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO child VALUES (2, 1), (3, 2), (4, 3);
SQL

    dolt add -A
    dolt commit -m "initial commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql <<SQL
INSERT INTO parent VALUES (4, 3), (5, 4);
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
CREATE TABLE parent (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT
);
CREATE TABLE child (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  CONSTRAINT fk_name FOREIGN KEY (v1)
    REFERENCES parent(v1)
);
INSERT INTO parent VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO child VALUES (2, 1), (3, 2), (4, 3);
SQL

    dolt add -A
    dolt commit -m "initial commit"
    dolt checkout -b other
    dolt checkout master
    dolt sql <<SQL
INSERT INTO parent VALUES (4, 3);
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
    run dolt merge other
    [ "$status" -eq "1" ]
    [[ "$output" =~ "violation" ]] || false
    [[ "$output" =~ "4" ]] || false
}
