## Overview

This is a collection of repositories that are to be created whenever the on-disk format is changed in any way. This way, we can catch some errors that may arise from the new changes not properly reading the older formats. The test file is `back-compat.bats`, which loops over each directory and runs a set of basic tests to ensure that the latest code can both read and modify pre-existing data. Each repository will have the same branch names and same general schema.

## Steps

Whenever the format is updated, a new repository should be created from scratch with the new format. The following steps may be used to create a compatible repository:

```bash
dolt init
"UPDATE README.md"
dolt sql -q "CREATE TABLE abc(pk BIGINT PRIMARY KEY, a LONGTEXT COMMENT 'tag:694', b DATETIME COMMENT 'tag:2902')"
dolt add .
dolt commit -m "Created table abc"
dolt sql -q "INSERT INTO abc VALUES (1, 'data', '2020-01-13 20:45:18.53558')"
dolt add .
dolt commit -m "Initial data"
dolt checkout -b conflict
dolt sql -q "INSERT INTO abc VALUES (2, 'something', '2020-01-14 20:48:37.13061')"
dolt add .
dolt commit -m "Added something row"
dolt checkout master
dolt checkout -b newcolumn
dolt sql -q "ALTER TABLE abc ADD COLUMN c BIGINT UNSIGNED COMMENT 'tag:4657'"
dolt sql -q "UPDATE abc SET c = 2133 WHERE c IS NULL"
dolt sql -q "INSERT INTO abc VALUES (2, 'something', '2020-01-13 20:48:37.13061', 1132020)"
dolt add .
dolt commit -m "Added something row and column c"
dolt checkout master
```

The generated `LICENSE.md` may be removed, and the `README.md` should be updated to reflect the information contained in the repository (this is assumed to be done immediately after `dolt init`). It is required for the repository to be on the `master` branch, as the tests assume this starting point.