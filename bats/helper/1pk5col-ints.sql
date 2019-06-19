DROP TABLE IF EXISTS `test`;
CREATE TABLE `test` (
  `pk` int not null comment 'tag:0',
  `c1` int comment 'tag:1',
  `c2` int comment 'tag:2',
  `c3` int comment 'tag:3',
  `c4` int comment 'tag:4',
  `c5` int comment 'tag:5',
  primary key (`pk`)
);
INSERT INTO `test` (`pk`,`c1`,`c2`,`c3`,`c4`,`c5`) VALUES (0,1,2,3,4,5);
