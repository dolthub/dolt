#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "import-mysqldump: import empty dump" {
    run dolt sql <<SQL
-- MySQL dump 10.13  Distrib 8.0.27, for macos12.0 (x86_64)
--
-- Host: localhost    Database: test
-- ------------------------------------------------------
-- Server version	8.0.17

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2022-03-16 10:22:21
SQL
    [ "$status" -eq 0 ]
}

@test "import-mysqldump: import empty database dump" {
    run dolt sql <<SQL
-- MySQL dump 10.13  Distrib 8.0.27, for macos12.0 (x86_64)
--
-- Host: localhost    Database: test
-- ------------------------------------------------------
-- Server version	8.0.27

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Current Database: test
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ test /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;

USE test;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2022-04-12 14:00:34
SQL
    [ "$status" -eq 0 ]
}

@test "import-mysqldump: import a simple database" {
    run dolt sql <<SQL
DROP TABLE IF EXISTS test;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE test (
  pk int(11) NOT NULL,
  c1 varchar(32) DEFAULT NULL,
  PRIMARY KEY (pk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

LOCK TABLES test WRITE;
/*!40000 ALTER TABLE test DISABLE KEYS */;
INSERT INTO test VALUES (0,'foo'),(1,'bar');
/*!40000 ALTER TABLE test ENABLE KEYS */;
UNLOCK TABLES;
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [ "$output" = "pk,c1
0,foo
1,bar" ]
}

@test "import-mysqldump: import a database with view" {
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
/*!50013 DEFINER=root@localhost SQL SECURITY DEFINER */
/*!50001 VIEW myview AS select mytable.id AS id,mytable.col2 AS col2,mytable.col3 AS col3 from mytable */;
SQL
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO mytable (id, col3) VALUES (1, TIMESTAMP('2003-12-31'));"
    run dolt sql -q "SELECT * FROM myview;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 1  | 999  | 2003-12-31 00:00:00 +0000 UTC |" ]] || false
}

@test "import-mysqldump: import a database with trigger" {
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
/*!50003 CREATE*/ /*!50017 DEFINER=root@localhost*/ /*!50003 TRIGGER tt BEFORE INSERT ON mytable FOR EACH ROW SET NEW.v1 = NEW.v1 * 11 */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
SQL
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO mytable VALUES (6,8);"
    run dolt sql -q "SELECT * FROM mytable;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 6  | 88 |" ]] || false
}

@test "import-mysqldump: import a database with procedure dumped with --routines flag" {
    run dolt sql <<SQL
/*!50003 DROP PROCEDURE IF EXISTS new_proc */;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=root@localhost PROCEDURE new_proc(x DOUBLE, y DOUBLE)
SELECT x*y ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "CALL new_proc(2, 3);"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 6       |" ]] || false
}

@test "import-mysqldump: a table with string literal representation in column definition" {
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
    run dolt sql -q "SELECT * FROM mytable;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 1  | 2003 | first_sometext |" ]] || false
}

@test "import-mysqldump: some of tables from mysql db, dump with --all-databases flag" {
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
}
