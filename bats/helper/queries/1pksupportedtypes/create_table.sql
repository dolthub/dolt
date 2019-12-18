CREATE TABLE `newtable`(
    `pk` BIGINT NOT NULL COMMENT 'tag:0',
    `int` BIGINT COMMENT 'tag:1',
    `string` LONGTEXT COMMENT 'tag:2',
    `boolean` BOOLEAN COMMENT 'tag:3',
    `float` DOUBLE COMMENT 'tag:4',
    `uint` BIGINT UNSIGNED COMMENT 'tag:5',
    `uuid` LONGTEXT COMMENT 'tag:6',
    PRIMARY KEY (`pk`)
);
