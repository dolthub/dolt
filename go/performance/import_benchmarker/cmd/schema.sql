CREATE TABLE `test` (
  `pk` int NOT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` char(1) DEFAULT NULL,
  `c3` datetime DEFAULT NULL,
  `c4` double DEFAULT NULL,
  `c5` tinyint DEFAULT NULL,
  `c6` float DEFAULT NULL,
  `c7` varchar(255) DEFAULT NULL,
  `c8` varbinary(255) DEFAULT NULL,
  `c9` text DEFAULT NULL,
  PRIMARY KEY (`pk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;