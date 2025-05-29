#!/usr/bin/env bats

setup() {
    REPO_NAME="dolt_repo_$$"
    mkdir $REPO_NAME
    cd $REPO_NAME

    dolt init
}

teardown() {
    cd ..
    rm -rf $REPO_NAME
}

@test "import mysqldump: empty database dump" {
    service mysql start
    mysql <<SQL
CREATE DATABASE testdb;
SQL

    mysqldump -B 'testdb' --result-file=dump.sql

    run dolt sql < dump.sql
    [ "$status" -eq 0 ]

    run dolt sql -q "show databases"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "testdb" ]] || false

    mysql <<SQL
DROP DATABASE testdb;
SQL
    # Give the server a chance to drop the database
    sleep 1
    service mysql stop
}

@test "import mysqldump: a simple table dump" {
    service mysql start
    mysql <<SQL
CREATE DATABASE testdb;
USE testdb;
CREATE TABLE mytable (pk int NOT NULL PRIMARY KEY, c1 varchar(25) DEFAULT NULL);
INSERT INTO mytable VALUES (0, 'one'), (1, 'two');
SQL

    mysqldump -B 'testdb' --result-file=dump.sql
    run dolt sql < dump.sql
    [ "$status" -eq 0 ]

    run dolt sql -r csv <<SQL
USE testdb;
SELECT * FROM mytable;
SQL
    [[ "$output" =~ "pk,c1
0,one
1,two" ]] || false

    mysql <<SQL
DROP DATABASE testdb;
SQL
    # Give the server a chance to drop the database
    sleep 1
    service mysql stop
}

@test "import mysqldump: database with view" {
    run dolt sql <<SQL
DROP TABLE IF EXISTS mytable;
CREATE TABLE mytable (
  id bigint NOT NULL,
  col2 bigint DEFAULT '999',
  col3 datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

LOCK TABLES mytable WRITE;
/*!40000 ALTER TABLE mytable DISABLE KEYS */;
/*!40000 ALTER TABLE mytable ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary view structure for view myview
--

DROP TABLE IF EXISTS myview;
/*!50001 DROP VIEW IF EXISTS myview*/;
/*!50001 CREATE VIEW myview AS SELECT
 1 AS id,
 1 AS col2,
 1 AS col3*/;

--
-- Final view structure for view myview
--

/*!50001 DROP VIEW IF EXISTS myview*/;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=\`root\`@\`localhost\` SQL SECURITY DEFINER */
/*!50001 VIEW \`myview\` AS select \`mytable\`.\`id\` AS \`id\`,\`mytable\`.\`col2\` AS \`col2\`,\`mytable\`.\`col3\` AS \`col3\` from \`mytable\` */;
SQL
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO mytable (id, col3) VALUES (1, TIMESTAMP('2003-12-31'));"
    run dolt sql -q "SELECT * FROM myview;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,999,2003-12-31 00:00:00" ]] || false

    run dolt sql -q "SHOW CREATE VIEW myview;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE VIEW \`myview\` AS select \`mytable\`.\`id\` AS \`id\`,\`mytable\`.\`col2\` AS \`col2\`,\`mytable\`.\`col3\` AS \`col3\` from \`mytable\`" ]] || false
}

@test "import mysqldump: database with trigger" {
    run dolt sql <<SQL
DROP TABLE IF EXISTS mytable;
CREATE TABLE mytable (
  pk bigint NOT NULL,
  v1 bigint DEFAULT NULL,
  PRIMARY KEY (pk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

LOCK TABLES mytable WRITE;
/*!40000 ALTER TABLE mytable DISABLE KEYS */;
INSERT INTO mytable VALUES (0,2),(1,3),(2,44);
/*!40000 ALTER TABLE mytable ENABLE KEYS */;
UNLOCK TABLES;

/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=\`root\`@\`localhost\`*/ /*!50003 TRIGGER tt BEFORE INSERT ON mytable FOR EACH ROW SET NEW.v1 = NEW.v1 * 11 */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
SQL
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO mytable VALUES (6,8)"
    run dolt sql -q "SELECT * FROM mytable" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6,88" ]] || false

    run dolt sql -q "SELECT trigger_name, event_object_table, action_statement, definer FROM information_schema.triggers" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tt,mytable,SET NEW.v1 = NEW.v1 * 11,root@localhost" ]] || false
}

@test "import mysqldump: database with procedure dumped with --routines flag" {
    run dolt sql <<SQL
/*!50003 DROP PROCEDURE IF EXISTS new_proc */;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=\`root\`@\`localhost\` PROCEDURE new_proc(x DOUBLE, y DOUBLE)
SELECT x*y ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "CALL new_proc(2, 3);" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6" ]] || false

    run dolt sql -q "SHOW PROCEDURE STATUS" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new_proc,PROCEDURE,root@localhost" ]] || false
}

@test "import mysqldump: a table with all types with DEFAULT NULL dump" {
    run dolt sql <<SQL
CREATE TABLE all_types (
  pk int NOT NULL,
  v1 binary(1) DEFAULT NULL,
  v2 bigint DEFAULT NULL,
  v3 bit(1) DEFAULT NULL,
  v4 blob,
  v5 char(1) DEFAULT NULL,
  v6 date DEFAULT NULL,
  v7 datetime DEFAULT NULL,
  v8 decimal(5,2) DEFAULT NULL,
  v9 double DEFAULT NULL,
  v10 enum('s','m','l') DEFAULT NULL,
  v11 float DEFAULT NULL,
  v12 geometry DEFAULT NULL,
  v13 int DEFAULT NULL,
  v14 json DEFAULT NULL,
  v15 linestring DEFAULT NULL,
  v16 longblob,
  v17 longtext,
  v18 mediumblob,
  v19 mediumint DEFAULT NULL,
  v20 mediumtext,
  v21 point DEFAULT NULL,
  v22 polygon DEFAULT NULL,
  v23 set('one','two') DEFAULT NULL,
  v24 smallint DEFAULT NULL,
  v25 text,
  v26 time(6) DEFAULT NULL,
  v27 timestamp NULL DEFAULT NULL,
  v28 tinyblob,
  v29 tinyint DEFAULT NULL,
  v30 tinytext,
  v31 varchar(255) DEFAULT NULL,
  v32 varbinary(255) DEFAULT NULL,
  v33 year DEFAULT NULL,
  PRIMARY KEY (pk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
INSERT INTO all_types VALUES (2,0x01,1,0x01,0x616263,'i','2022-02-22','2022-02-22 22:22:22',999.99,1.1,'s',1.1,0x000000000101000000000000000000F03F0000000000000040,1,'{\"a\": 1}',0x0000000001020000000200000000000000000000000000000000000000000000000000F03F0000000000000040,0x616263,'abc',0x616263,1,'abc',0x000000000101000000000000000000F03F0000000000000040,0x00000000010300000001000000050000000000000000000000000000000000000000000000000020400000000000000000000000000000284000000000000022400000000000000000000000000000224000000000000000000000000000000000,'one',1,'abc','11:59:59.000000','2021-01-19 11:14:07',0x616263,1,'abc','varchar value',0x3131313131,2018);
SQL
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO all_types (pk) VALUES (1);"
    run dolt sql -q "SELECT st_aswkt(v15) from all_types;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "LINESTRING(0 0,1 2)" ]] || false
    [[ "$output" =~ "NULL" ]] || false
}

@test "import mysqldump: a table with all types with DEFAULT not-null VALUE dump" {
    run dolt sql <<SQL
CREATE TABLE types_default (
  pk int NOT NULL,
  v1 binary(1) DEFAULT '1',
  v2 bigint DEFAULT '1',
  v3 bit(2) DEFAULT b'10',
  v4 blob DEFAULT (_utf8mb4'abc'),
  v5 char(1) DEFAULT 'i',
  v6 date DEFAULT '2022-02-22',
  v7 datetime DEFAULT '2022-02-22 22:22:22',
  v8 decimal(5,2) DEFAULT '999.99',
  v9 double DEFAULT '1.1',
  v10 enum('s','m','l') DEFAULT 's',
  v11 float DEFAULT '1.1',
  v12 geometry DEFAULT (point(1.3,3)),
  v13 int DEFAULT '1',
  v14 json DEFAULT (json_object(_utf8mb4'a',1)),
  v15 linestring DEFAULT (linestring(point(0,0),point(1,2))),
  v16 longblob DEFAULT (_utf8mb4'abc'),
  v17 longtext DEFAULT (_utf8mb4'abc'),
  v18 mediumblob DEFAULT (_utf8mb4'abc'),
  v19 mediumint DEFAULT '1',
  v20 mediumtext DEFAULT (_utf8mb4'abc'),
  v21 point DEFAULT (point(1,2)),
  v22 polygon DEFAULT (polygon(linestring(point(0,0),point(8,0),point(12,9),point(0,9),point(0,0)))),
  v23 set('one','two') DEFAULT 'one',
  v24 smallint DEFAULT '1',
  v25 text DEFAULT (_utf8mb4'abc'),
  v26 time(6) DEFAULT '11:59:59.000000',
  v27 timestamp NULL DEFAULT '2021-01-19 11:14:07',
  v28 tinyblob DEFAULT (_utf8mb4'abc'),
  v29 tinyint DEFAULT '1',
  v30 tinytext DEFAULT (_utf8mb4'abc'),
  v31 varchar(255) DEFAULT 'varchar value',
  v32 varbinary(255) DEFAULT '11111',
  v33 year DEFAULT '2018',
  PRIMARY KEY (pk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
SQL
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO types_default (pk) VALUES (1);"
    run dolt sql -q "SELECT hex(st_aswkb(v12)) from types_default;"
    [ "$status" -eq 0 ]
    # should be "000000000101000000CDCCCCCCCCCCF43F0000000000000840"
    [[ "$output" =~ "0101000000CDCCCCCCCCCCF43F0000000000000840" ]] || false
}

@test "import mysqldump: a table with string literal representation in column definition" {
    run dolt sql <<SQL
CREATE TABLE mytable (
  pk int NOT NULL,
  col2 int DEFAULT (date_format(now(),_utf8mb4'%Y')),
  col3 varchar(20) NOT NULL DEFAULT 'sometext',
  PRIMARY KEY (pk),
  CONSTRAINT status CHECK ((col3 like _utf8mb4'%sometext%'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
SQL
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO mytable VALUES (1, 2003, 'first_sometext');"
    run dolt sql -q "SELECT * FROM mytable;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,2003,first_sometext" ]] || false
}

@test "import mysqldump: charset introducer in tables from mysql db" {
    run dolt sql <<SQL
CREATE TABLE engine_cost (
  engine_name varchar(64) NOT NULL,
  device_type int NOT NULL,
  cost_name varchar(64) NOT NULL,
  cost_value float DEFAULT NULL,
  last_update timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  comment varchar(1024) DEFAULT NULL,
  default_value float GENERATED ALWAYS AS ((case cost_name when _utf8mb3'io_block_read_cost' then 1.0 when _utf8mb3'memory_block_read_cost' then 0.25 else NULL end)) VIRTUAL,
  PRIMARY KEY (cost_name,engine_name,device_type)
) /*!50100 TABLESPACE mysql */ ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 STATS_PERSISTENT=0 ROW_FORMAT=DYNAMIC;
SQL
    [ "$status" -eq 0 ]

    skip "generated always as functionality is not supported"
    run dolt sql -q "SHOW CREATE TABLE engine_cost"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "GENERATED ALWAYS AS" ]] || false
}

@test "import mysqldump: string byte length is more than defined varchar character length" {
    run dolt sql <<SQL
CREATE TABLE city (
  ID int NOT NULL AUTO_INCREMENT,
  Name char(35) NOT NULL DEFAULT '',
  CountryCode char(3) NOT NULL DEFAULT '',
  District char(20) NOT NULL DEFAULT '',
  Population int NOT NULL DEFAULT '0',
  PRIMARY KEY (ID)
);
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "INSERT INTO city VALUES (1,'San Pedro de Macorís','DOM','San Pedro de Macorís',124735);"
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT * FROM city;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,San Pedro de Macorís,DOM,San Pedro de Macorís,124735" ]] || false
}

@test "import mysqldump: show create table on table with geometry type" {
    run dolt sql <<SQL
CREATE TABLE geometry_type (
  pk int NOT NULL,
  g geometry DEFAULT (point(1,2)),
  PRIMARY KEY (pk)
);
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "show create table geometry_type;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "\`g\` geometry DEFAULT (point(1,2))," ]] || false

    run dolt sql <<SQL
CREATE TABLE polygon_type (
  pk int NOT NULL,
  p polygon DEFAULT (polygon(linestring(point(0,0),point(8,0),point(12,9),point(0,9),point(0,0)))),
  PRIMARY KEY (pk)
);
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "show create table polygon_type;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "\`p\` polygon DEFAULT (polygon(linestring(point(0,0),point(8,0),point(12,9),point(0,9),point(0,0))))," ]] || false
}

@test "import mysqldump: select into syntax in procedure" {
    run dolt sql <<SQL
CREATE TABLE inventory (item_id int primary key, shelf_id int, item varchar(10));
INSERT INTO inventory VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c'), (4, 1, 'd'), (5, 4, 'e');
DELIMITER $$
CREATE PROCEDURE count_and_print(IN p_shelf_id INT, OUT p_count INT) BEGIN
  SELECT item FROM inventory WHERE shelf_id = p_shelf_id ORDER BY item ASC;
  SELECT COUNT(*) INTO p_count FROM inventory WHERE shelf_id = p_shelf_id;
END$$
DELIMITER ;
SQL
    [ "$status" -eq 0 ]

    run dolt sql -r csv <<SQL
CALL count_and_print(1, @total);
SELECT @total;
SQL
    [ "$status" -eq 0 ]
    [ "$output" = "item
a
b
d
@total
3" ]
}

@test "import mysqldump: allow non-existent table in create trigger" {
    run dolt sql <<SQL
CREATE TABLE film (
  film_id smallint unsigned NOT NULL AUTO_INCREMENT,
  title varchar(128) NOT NULL,
  description text,
  PRIMARY KEY (film_id)
);
DELIMITER $$
CREATE TRIGGER ins_film AFTER INSERT ON film FOR EACH ROW BEGIN
  INSERT INTO film_text (film_id, title, description) VALUES (new.film_id, new.title, new.description);
END$$
DELIMITER ;
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "INSERT INTO film VALUES (1,'ACADEMY DINOSAUR','A Epic Drama in The Canadian Rockies')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found: film_text" ]] || false

    run dolt sql <<SQL
CREATE TABLE film_text (
  film_id smallint NOT NULL,
  title varchar(255) NOT NULL,
  description text,
  PRIMARY KEY (film_id)
);
INSERT INTO film VALUES (1,'ACADEMY DINOSAUR','A Epic Drama in The Canadian Rockies')
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT * FROM film;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,ACADEMY DINOSAUR,A Epic Drama in The Canadian Rockies" ]] || false
}

@test "import mysqldump: allow non-existent procedure in create trigger" {
    run dolt sql <<SQL
CREATE TABLE t0 (id INT PRIMARY KEY AUTO_INCREMENT, v1 INT, v2 TEXT);
CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT, v1 INT, v2 TEXT);
INSERT INTO t0 VALUES (1, 2, 'abc'), (2, 3, 'def');
DELIMITER $$
CREATE PROCEDURE add_entry(i INT, s TEXT) BEGIN
  IF i > 50 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'too big number';
  END IF;
  INSERT INTO t0 (v1, v2) VALUES (i, s);
END$$
CREATE TRIGGER trig AFTER INSERT ON t0 FOR EACH ROW BEGIN
  CALL back_up(NEW.v1, NEW.v2);
END$$
DELIMITER ;
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "CALL add_entry(5, 'xyz');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "stored procedure \"back_up\" does not exist" ]] || false

    run dolt sql <<SQL
CREATE PROCEDURE back_up(num INT, msg TEXT) INSERT INTO t1 (v1, v2) VALUES (num*2, msg);
CALL add_entry(6, 'lmn');
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT * FROM t0;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6,lmn" ]] || false
    [[ ! "$output" =~ "5,xyz" ]] || false

    run dolt sql -q "SELECT * FROM t1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "12,lmn" ]] || false

    run dolt sql -q "CALL add_entry(55, 'kkk');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "too big number" ]] || false
}

@test "import mysqldump: show create table on table with geometry type with SRID value" {
    run dolt sql <<SQL
CREATE TABLE address (
  address_id smallint unsigned NOT NULL AUTO_INCREMENT,
  address varchar(50) NOT NULL,
  address2 varchar(50) DEFAULT NULL,
  district varchar(20) NOT NULL,
  city_id smallint unsigned NOT NULL,
  postal_code varchar(10) DEFAULT NULL,
  phone varchar(20) NOT NULL,
  location geometry NOT NULL /*!80003 SRID 0 */,
  last_update timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (address_id)
) ENGINE=InnoDB AUTO_INCREMENT=606 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "show create table address;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "\`location\` geometry NOT NULL /*!80003 SRID 0 */," ]] || false
}

@test "import mysqldump: dolt dump --no-autocommit can be loaded back into mysql" {
    service mysql start
    dolt sql -q "CREATE TABLE IF NOT EXISTS mytable (pk int NOT NULL PRIMARY KEY, c1 varchar(25) DEFAULT NULL)"
    dolt sql -q "INSERT IGNORE INTO mytable VALUES (0, 'one'), (1, 'two')"

    # Setup the database we are loading data into
    mysql <<SQL
CREATE DATABASE IF NOT EXISTS testdb;
SQL

    run dolt dump --no-autocommit --no-create-db
    [ -f doltdump.sql ]

    # remove the utf8mb4_0900_bin collation which is not supported in this installation of mysql
    sed -i 's/COLLATE=utf8mb4_0900_bin//' doltdump.sql

    mysql testdb < doltdump.sql
    run mysql <<SQL
SELECT count(*) from testdb.mytable
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    # Give the server a chance to complete the last query
    # TODO: `service mysqld stop` is hanging when a check above fails.
    sleep 1
    service mysql stop
}
