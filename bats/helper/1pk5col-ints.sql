DROP TABLE IF EXISTS `test_int`;
CREATE TABLE `test_int` (
  `pk` BIGINT NOT NULL COMMENT 'tag:0',
  `c1` BIGINT COMMENT 'tag:1',
  `c2` BIGINT COMMENT 'tag:2',
  `c3` BIGINT COMMENT 'tag:3',
  `c4` BIGINT COMMENT 'tag:4',
  `c5` BIGINT COMMENT 'tag:5',
  PRIMARY KEY (`pk`)
);
INSERT INTO `test_int` (`pk`,`c1`,`c2`,`c3`,`c4`,`c5`) VALUES (0,1,2,3,4,5);
