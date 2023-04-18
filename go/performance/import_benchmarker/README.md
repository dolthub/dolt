## Import Benchmarking

Benchmark different import workflows expressed as yaml files.

Unit tests in `import_test.go` are not suitable for reporting performance
comparisons.

## Usage

Sample:
```bash
go build \
  github.com/dolthub/dolt/go/performance/import_benchmarker/cmd \
  -test testdata/shuffle.yaml
```

Requirements:

Tests that use dolt require a `dolt` binary in `PATH` for performance comparisons.

Tests with an `external-server` configuration are expected to be available
from the host machine on startup.

Example `mysqld` server on the host OS, assuming an initialized `datadir`
and pre-existing database:
```bash
mysqld --port 3308 --local-infile=1 --socket=/tmp/mysqld2.sock
````

Example mysql server `docker-compose.yml` config:
```yaml
mysql:
  image: mysql/mysql-server:8.0
  container_name: mysql-import-perf
  ports:
      - "3308:3306"
  command: --local-infile=1 --socket=/tmp/mysqld2.sock
  volumes:
      - ./mysql:/var/lib/mysql
  restart: always # always restart unless stopped manually
  environment:
      MYSQL_USER: root
      MYSQL_ROOT_PASSWORD: password
      MYSQL_PASSWORD: password
      MYSQL_DATABASE: test
```

Note the `--local-infile` parameter, which permits `LOAD DATA`, and
the `--socket` parameter, which specifies a non-default socket that
will not conflict with any `dolt sql-server` instances. All other
parameters, including the database name, are configurable in the test
file yaml.

## Inputs 

Specify imports for different servers and workloads along several
dimensions:

- repo
  - dolt server (server field)
  - dolt cli (omit server field)
  - mysql server (external server field)

- table spec
  - fmt (string): file format for importing
    - csv: comma separated lines
    - sql: dump file of insert statements
  - rows (int): number of rows to import
  - schema (string): CREATE_TABLE statement for table to import
  - shuffle (bool): by default generated rows are sorted; indicate `true` to shuffle
  - batch (bool): whether to batch insert statements (only applies to fmt=sql)

For an examples of the specific yaml input syntax, see the example
files below, or refer to the tests in `testdata/`.

Server Details:

- For dolt sql-server tests, a new sql-server will be constructed individually
  for each test run.
- External servers are provided outside of the lifecycle of the `import_benchmarker`
  command. The same database instance is used for every table import test.
- Import files are cached on the schema, row number, and format in between
  tests.

## Outputs

The output format is a `.sql` file with the following schema:

```sql
CREATE TABLE IF NOT EXISTS import_perf_results (
  test_name varchar(64),
  server varchar(64),
  detail varchar(64),
  row_cnt int,
  time double,
  file_format varchar(8),
  sorted bool,
  primary key (test_name, server, detail)
);
```

A sample import file:
```sql
insert into import_perf_results values
    ('primary key types', 'mysql', 'int', 400000, 2.20, 'csv', 1);
insert into import_perf_results values
    ('primary key types', 'mysql', 'float', 400000, 1.98, 'csv', 1);
insert into import_perf_results values
    ('primary key types', 'mysql', 'varchar', 400000, 3.46, 'csv', 1);
insert into import_perf_results values
    ('config width', 'mysql', '2 cols', 400000, 1.71, 'csv', 1);
insert into import_perf_results values
    ('config width', 'mysql', '4 cols', 400000, 1.78, 'csv', 1);
insert into import_perf_results values
    ('config width', 'mysql', '8 cols', 400000, 2.10, 'csv', 1);
insert into import_perf_results values
    ('pk type', 'mysql', 'int', 400000, 1.70, 'csv', 1);
insert into import_perf_results values
    ('pk type', 'mysql', 'float', 400000, 1.95, 'csv', 1);
insert into import_perf_results values
    ('pk type', 'mysql', 'varchar', 400000, 3.86, 'csv', 1);

insert into import_perf_results values
    ('primary key types', 'dolt', 'int', 400000, 2.10, 'csv', 1);
insert into import_perf_results values
    ('primary key types', 'dolt', 'float', 400000, 2.83, 'csv', 1);
insert into import_perf_results values
    ('primary key types', 'dolt', 'varchar', 400000, 5.01, 'csv', 1);
insert into import_perf_results values
    ('config width', 'dolt', '2 cols', 400000, 2.12, 'csv', 1);
insert into import_perf_results values
    ('config width', 'dolt', '4 cols', 400000, 2.47, 'csv', 1);
insert into import_perf_results values
    ('config width', 'dolt', '8 cols', 400000, 2.84, 'csv', 1);
insert into import_perf_results values
    ('pk type', 'dolt', 'int', 400000, 2.06, 'csv', 1);
insert into import_perf_results values
    ('pk type', 'dolt', 'float', 400000, 2.27, 'csv', 1);
insert into import_perf_results values
    ('pk type', 'dolt', 'varchar', 400000, 5.34, 'csv', 1);

insert into import_perf_results values
('primary key types', 'dolt_cli', 'int', 400000, 2.40, 'csv', 1);
insert into import_perf_results values
('primary key types', 'dolt_cli', 'float', 400000, 2.44, 'csv', 1);
insert into import_perf_results values
('primary key types', 'dolt_cli', 'varchar', 400000, 5.58, 'csv', 1);
insert into import_perf_results values
('config width', 'dolt_cli', '2 cols', 400000, 2.40, 'csv', 1);
insert into import_perf_results values
('config width', 'dolt_cli', '4 cols', 400000, 2.77, 'csv', 1);
insert into import_perf_results values
('config width', 'dolt_cli', '8 cols', 400000, 3.23, 'csv', 1);
insert into import_perf_results values
('pk type', 'dolt_cli', 'int', 400000, 2.37, 'csv', 1);
insert into import_perf_results values
('pk type', 'dolt_cli', 'float', 400000, 2.43, 'csv', 1);
insert into import_perf_results values
('pk type', 'dolt_cli', 'varchar', 400000, 5.52, 'csv', 1);
```
Ingest the result file and run queries like the ones below to compare
import runtimes:

```sql
-- compare two servers
> select
    a.test_name as test_name,
    a.detail as detail,
    a.row_cnt as row_cnt,
    a.sorted as sorted,
    a.time as dolt_time,
    b.time as mysql_time,
    round((a.time / b.time),2) as multiple
from import_perf_results a
    join import_perf_results b
 on
    a.test_name = b.test_name and
    a.detail = b.detail
where
    a.server = 'dolt' and
    b.server = 'mysql'
order by 1,2;

+-------------------+--------------+---------+--------+-----------+------------+----------+
| test_name         | detail       | row_cnt | sorted | dolt_time | mysql_time | multiple |
+-------------------+--------------+---------+--------+-----------+------------+----------+
| blobs             | 1 blob       | 400000  | 1      | 34.94     | 2.16       | 16.18    |
| blobs             | 2 blobs      | 400000  | 1      | 62.23     | 2.08       | 29.92    |
| blobs             | no blob      | 400000  | 1      | 2.91      | 2.09       | 1.39     |
| config width      | 2 cols       | 400000  | 1      | 2.12      | 1.71       | 1.24     |
| config width      | 4 cols       | 400000  | 1      | 2.47      | 1.78       | 1.39     |
| config width      | 8 cols       | 400000  | 1      | 2.84      | 2.1        | 1.35     |
| pk type           | float        | 400000  | 1      | 2.27      | 1.95       | 1.16     |
| pk type           | int          | 400000  | 1      | 2.06      | 1.7        | 1.21     |
| pk type           | varchar      | 400000  | 1      | 5.34      | 3.86       | 1.38     |
+-------------------+--------------+---------+--------+------------+----------+----------+

-- compare three servers
> select
  o.test_name as test_name,
  o.detail,
  o.row_cnt,
  o.sorted as sorted,
  o.time as mysql_time,
  (
  select round((a.time / b.time),2) m
  from import_perf_results a
  join import_perf_results b
  on
    a.test_name = b.test_name and
    a.detail = b.detail
  where
    a.server = 'dolt' and
    b.server = 'mysql' and
    a.test_name = o.test_name and
    a.detail = o.detail
  ) as sql_mult,
  (
  select round((a.time / b.time),2) m
  from import_perf_results a
  join import_perf_results b
  on
    a.test_name = b.test_name and
    a.detail = b.detail
  where
    a.server = 'dolt_cli' and
    b.server = 'mysql' and
    a.test_name = o.test_name and
    a.detail = o.detail
  ) as cli_mult
from import_perf_results as o
where o.server = 'mysql'
order by 1,2;

+-------------------+--------------+---------+--------+------------+----------+----------+
| test_name         | detail       | row_cnt | sorted | mysql_time | sql_mult | cli_mult |
+-------------------+--------------+---------+--------+------------+----------+----------+
| blobs             | 1 blob       | 400000  | 1      | 2.16       | 16.18    | 13.43    |
| blobs             | 2 blobs      | 400000  | 1      | 2.08       | 29.92    | 26.71    |
| blobs             | no blob      | 400000  | 1      | 2.09       | 1.39     | 1.33     |
| config width      | 2 cols       | 400000  | 1      | 1.71       | 1.24     | 1.4      |
| config width      | 4 cols       | 400000  | 1      | 1.78       | 1.39     | 1.56     |
| config width      | 8 cols       | 400000  | 1      | 2.1        | 1.35     | 1.54     |
| pk type           | float        | 400000  | 1      | 1.95       | 1.16     | 1.25     |
| pk type           | int          | 400000  | 1      | 1.7        | 1.21     | 1.39     |
| pk type           | varchar      | 400000  | 1      | 3.86       | 1.38     | 1.43     |
+-------------------+--------------+---------+--------+------------+----------+----------+
```

## Example tests

Example test spec 1:
```yaml
tests:
- name: "sorting"
  repos:
  - name: repo1
    server:
      port: 3308
  tables:
  - name: "shuffle"
    shuffle: true
    rows: 100000
    schema: |
      create table xy (
        x int primary key,
        y varchar(30)
      );
  - name: "sorted"
    shuffle: false
    rows: 100000
    schema: |
      create table xy (
        x int primary key,
        y varchar(30)
      );
```

We will import two tables with a dolt sql-server on port `3308`.
Both tables have 100,000 rows, and a schema with two columns.
The "sorted" test imports the default sorted rows, while the
"shuffle" imports unsorted rows. 

Example import spec 2:

```yaml
tests:
- name: "row count"
  repos:
   - name: mysql
     external-server:
       name: test
       host: 127.0.0.1
       user: root
       password: password
       port: 4306
  tables:
  - name: "400k"
    fmt: "csv"
    rows: 40000
    schema: |
      create table xy (
        x int primary key,
        y varchar(30)
      );
```

We will connect to a database server named `test` on port `4306`
with the credentials above to run a 40,000 row import of a table
with two columns.