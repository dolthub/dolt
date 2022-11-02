DROP TABLE IF EXISTS `test_int`;
CREATE TABLE `test_int` (
  `pk` bigint NOT NULL,
  `c1` bigint,
  `c2` bigint,
  `c3` bigint,
  `c4` bigint,
  `c5` bigint,
  PRIMARY KEY (`pk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
INSERT INTO `test_int` (`pk`,`c1`,`c2`,`c3`,`c4`,`c5`) VALUES (0,1,2,3,4,5);
