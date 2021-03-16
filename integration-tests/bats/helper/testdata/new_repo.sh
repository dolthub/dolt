#!/bin/bash

dolt init
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
