## Overview

This is test data that captures a repository on January 13, 2020. This may be used to verify that any serialization/deserialization changes are compatible with people's pre-existing data.

# Branch master

## Schema

```
CREATE TABLE `abc` (
  `pk` BIGINT NOT NULL COMMENT 'tag:0',
  `a` LONGTEXT COMMENT 'tag:694',
  `b` DATETIME COMMENT 'tag:2902',
  PRIMARY KEY (`pk`)
);
```

## Data

```
+----+------+---------------------------+
| pk | a    | b                         |
+----+------+---------------------------+
| 1  | data | 2020-01-13 20:45:18.53558 |
+----+------+---------------------------+
```

# Branch conflict

## Schema

```
CREATE TABLE `abc` (
  `pk` BIGINT NOT NULL COMMENT 'tag:0',
  `a` LONGTEXT COMMENT 'tag:694',
  `b` DATETIME COMMENT 'tag:2902',
  PRIMARY KEY (`pk`)
);
```

## Data

```
+----+-----------+---------------------------+
| pk | a         | b                         |
+----+-----------+---------------------------+
| 1  | data      | 2020-01-13 20:45:18.53558 |
| 2  | something | 2020-01-14 20:48:37.13061 |
+----+-----------+---------------------------+
```

# Branch newcolumn

## Schema

```
CREATE TABLE `abc` (
  `pk` BIGINT NOT NULL COMMENT 'tag:0',
  `a` LONGTEXT COMMENT 'tag:694',
  `b` DATETIME COMMENT 'tag:2902',
  `c` BIGINT UNSIGNED COMMENT 'tag:4657',
  PRIMARY KEY (`pk`)
);
```

## Data

```
+----+-----------+---------------------------+---------+
| pk | a         | b                         | c       |
+----+-----------+---------------------------+---------+
| 1  | data      | 2020-01-13 20:45:18.53558 | 2133    |
| 2  | something | 2020-01-13 20:48:37.13061 | 1132020 |
+----+-----------+---------------------------+---------+
```
