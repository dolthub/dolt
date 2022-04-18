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

    usr/bin/mysql -u root <<SQL
DROP DATABASE testdb;
SQL
    # Give the server a chance to drop the database
    sleep 1
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

    usr/bin/mysql -u root <<SQL
DROP DATABASE testdb;
SQL
    # Give the server a chance to drop the database
    sleep 1
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
    [[ "$output" =~ "1,999,2003-12-31 00:00:00 +0000 UTC" ]] || false

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
    [[ "$output" =~ "tt,mytable,SET NEW.v1 = NEW.v1 * 11,\`root\`@\`localhost\`" ]] || false
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
    [[ "$output" =~ "new_proc,PROCEDURE,\`root\`@\`localhost\`" ]] || false
}

@test "import mysqldump: a table with string literal representation in column definition" {
    skip "charset introducer needs to be supported in LIKE filter"
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
    skip "utf8mb3 charset introducer needs to be supported for mysql db tables"
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
