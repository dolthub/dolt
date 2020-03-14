CREATE TABLE `newtable`(
    `pk` BIGINT NOT NULL COMMENT 'tag:10',
    `int` BIGINT COMMENT 'tag:11',
    `string` LONGTEXT COMMENT 'tag:12',
    `boolean` BOOLEAN COMMENT 'tag:13',
    `float` DOUBLE COMMENT 'tag:14',
    `uint` BIGINT UNSIGNED COMMENT 'tag:15',
    `uuid` LONGTEXT COMMENT 'tag:16',
    PRIMARY KEY (`pk`)
);
