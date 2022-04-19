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