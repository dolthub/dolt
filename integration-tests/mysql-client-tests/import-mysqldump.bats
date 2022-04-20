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

@test "import mysqldump: a table with various spatial types" {
    run dolt sql << SQL
    --
    -- Table structure for table `geom_table`
    --

    DROP TABLE IF EXISTS `geom_table`;
    /*!40101 SET @saved_cs_client     = @@character_set_client */;
    /*!50503 SET character_set_client = utf8mb4 */;
    CREATE TABLE `geom_table` (
        `g` geometry DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

    /*!40101 SET character_set_client = @saved_cs_client */;
    --
    -- Dumping data for table `geom_table`
    --

    LOCK TABLES `geom_table` WRITE;
    /*!40000 ALTER TABLE `geom_table` DISABLE KEYS */;
    INSERT INTO `geom_table` VALUES (0x000000000101000000000000000000F03F0000000000000040);
    /*!40000 ALTER TABLE `geom_table` ENABLE KEYS */;
    UNLOCK TABLES;

    --
    -- Table structure for table `line_table`
    --

    DROP TABLE IF EXISTS `line_table`;
    /*!40101 SET @saved_cs_client     = @@character_set_client */;
    /*!50503 SET character_set_client = utf8mb4 */;
    CREATE TABLE `line_table` (
        `l` linestring DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    /*!40101 SET character_set_client = @saved_cs_client */;

    --
    -- Dumping data for table `line_table`
    --

    LOCK TABLES `line_table` WRITE;
              /*!40000 ALTER TABLE `line_table` DISABLE KEYS */;
    INSERT INTO `line_table` VALUES (0x00000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040);
    /*!40000 ALTER TABLE `line_table` ENABLE KEYS */;
    UNLOCK TABLES;

              --
              -- Table structure for table `point_table`
              --

    DROP TABLE IF EXISTS `point_table`;
    /*!40101 SET @saved_cs_client     = @@character_set_client */;
    /*!50503 SET character_set_client = utf8mb4 */;
    CREATE TABLE `point_table` (
        `p` point DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    /*!40101 SET character_set_client = @saved_cs_client */;

    --
    -- Dumping data for table `point_table`
    --

    LOCK TABLES `point_table` WRITE;
          /*!40000 ALTER TABLE `point_table` DISABLE KEYS */;
    INSERT INTO `point_table` VALUES (0x000000000101000000000000000000F03F0000000000000040);
    /*!40000 ALTER TABLE `point_table` ENABLE KEYS */;
    UNLOCK TABLES;

    --
    -- Table structure for table `poly_table`
    --

    DROP TABLE IF EXISTS `poly_table`;
    /*!40101 SET @saved_cs_client     = @@character_set_client */;
    /*!50503 SET character_set_client = utf8mb4 */;
    CREATE TABLE `poly_table` (
        `p` polygon DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    /*!40101 SET character_set_client = @saved_cs_client */;

    --
    -- Dumping data for table `poly_table`
    --

    LOCK TABLES `poly_table` WRITE;
    /*!40000 ALTER TABLE `poly_table` DISABLE KEYS */;
    INSERT INTO `poly_table` VALUES (0x000000000103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F0000000000000040000000000000004000000000000000000000000000000000);
    /*!40000 ALTER TABLE `poly_table` ENABLE KEYS */;
    UNLOCK TABLES;
    SQL
    [ "$status" -eq 0 ]
    run dolt sql -q "select st_aswkt(g) from geom_table"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "POINT(1 2)" ]]
    run dolt sql -q "select st_aswkt(p) from point_table"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "POINT(1 2)" ]]
    run dolt sql -q "select st_aswkt(l) from line_table"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "LINESTRING(1 2,3 4)" ]]
    run dolt sql -q "select st_aswkt(p) from poly_table"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "POLYGON((0 0,1 1,2 2,0 0))" ]]
}
