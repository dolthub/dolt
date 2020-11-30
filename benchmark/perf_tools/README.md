## Background
This directory contains a toolkit for benchmarking Dolt SQL performance. [`sysbench`](https://github.com/akopytov/sysbench) is an industry standard tool for benchmarking database performance, particularly MySQL. The goal is make it convenient for the Dolt team and open source contributors to measure the impact of changes.

The output of this toolchain is benchmark results for one, or two, versions of Dolt, along with MySQL. These benchmark results are associated with a unique "run ID", a hash value that ties together benchmarks that were run on the same machine at the same time using the same command. The motivation for this design is to create comparative benchmarks that isolate hardware conditions to some reasonable degree while maintaining an easy to use interface.

## Architecture
To produce this output we provide an interface for running a specified set of benchmarks against one, or two, versions of Dolt. The versions are specified by "committish", essentially Git reference that can be resolved to a commit. For example we might wish to compare the SQL performance of Dolt at:
- the HEAD of the currently checked out branch and the tag referencing the most recent release
- two branches with different approaches to a performance problem
- a commit containing a performance regression to the HEAD of master

To achieve this the goal for each committish provided we:
- build Dolt at the specified committish inside a Docker container for repeatability
- stand up container running Dolt SQL server
- stand up container running [`sysbench` MySQL driver](https://github.com/akopytov/sysbench/tree/master/src/drivers/mysql) that connects to the Dolt SQL server 
- execute the benchmarks specified in the `sysbench` container, and store the results as a CSV file

We also execute the same set of benchmarks against MySQL for comparison. All the benchmarks, those associated with the Dolt refs and MySQL, are associated with a unique `run_id` which identi an invocation of 

## Example
A common use-case might be to compare Dolt built from the current working set in your local checkout to MySQL. To do this we can run the following:
```
$ ./run_benchmarks.sh all <your-username> current
```

This takes the current checkout of Dolt, builds a binary, and executes the supported benchmarks in a `docker-compose` setup. It does the same for MySQL. Each invocation of `run_benchmarks.sh` is associatd with a run ID, for example `58296063ab3c2a6701f8f986`. This run ID identifies the CSV file: 
```
$ ls -ltr output
total 16
-rw-r--r--  1 oscarbatori  staff  1727 Nov 29 19:59 58296063ab3c2a6701f8f986.csv
```

Each row corresponds to an invocation of test on either MySQL, or a compilation of Dolt. Each row indicates this.

## Requirements
To run this stack a few things are required:
- a bash shell to execute `run_benchmarks.sh`
- Docker: the Dolt binaries are built in Docker, and the MySQL server and the Dolt SQL server are run in a container, as is `sysbench`
- Python: if you want to upload results to DoltHub, we use [Doltpy's](https://pypi.org/project/doltpy/) tools for pushing data to DoltHub, so you need to install Doltpy

## Uploading to DoltHub
We can upload the results to DoltHub using `push_results_to_dolthub.py` as follows:
```
$ python push_outputp_to_dolthub.py --results-file output/58296063ab3c2a6701f8f986.csv --remote-results-db dolthub/dolt-benchmarks-test --branch test-run
```

These results will then be available to the team for analysis, and via our API for rendering on our benchmarking documentation.

## `sysbench_scripts`
This directory contains Lua scripts that implement custom `sysbench` behavior. Above we described `sysbench` as "scriptable". In practice this means defining `sysbench` tests in Lua scripts and passing those scripts to the `sysbench` command. As an example to run `sysbench` using a predefined test one might run:
```
sysbench \
  bulk_insert \
  --table-size=1000000 \
  --db-driver=mysql \
  --mysql-db=test_db \
  --mysql-user=root \
  --mysql-host=127.0.0.1 \
run
```

Suppose we wanted to define custom behavior, or some custom output method, then we would define `sysbench_scripts/my_custom_test.lua`, and then run:
```
sysbench \
  sysbench_scripts/lua/my_custom_test.lua \
  --table-size=1000000 \
  --db-driver=mysql \
  --mysql-db=test_db \
  --mysql-user=root \
  --mysql-host=127.0.0.1 \
run
```

This passes the script `sysbench_scripts/lua/my_custom_test.lua` which is then executed and measured. Here is an example, `bulk_insert`, taken from the [`sysbench` core out-of-the-box SQL benchmarks](https://github.com/akopytov/sysbench/tree/master/src/lua):
```
#!/usr/bin/env sysbench
-- -------------------------------------------------------------------------- --
-- Bulk insert benchmark: do multi-row INSERTs concurrently in --threads
-- threads with each thread inserting into its own table. The number of INSERTs
-- executed by each thread is controlled by either --time or --events.
-- -------------------------------------------------------------------------- --

cursize=0

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()
end

function prepare()
   local i

   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = 1, sysbench.opt.threads do
      print("Creating table 'sbtest" .. i .. "'...")
      con:query(string.format([[
        CREATE TABLE IF NOT EXISTS sbtest%d (
          id INTEGER NOT NULL,
          k INTEGER DEFAULT '0' NOT NULL,
          PRIMARY KEY (id))]], i))
   end
end


function event()

   if (cursize == 0) then
      con:bulk_insert_init("INSERT INTO sbtest" .. sysbench.tid+1 .. " VALUES")
   end

   cursize = cursize + 1

   con:bulk_insert_next("(" .. cursize .. "," .. cursize .. ")")
end

function thread_done()
   con:bulk_insert_done()
   con:disconnect()
end

function cleanup()
   local i

   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = 1, sysbench.opt.threads do
      print("Dropping table 'sbtest" .. i .. "'...")
      con:query("DROP TABLE IF EXISTS sbtest" .. i )
   end

end
``` 

The structure is relatively familiar if you have seen testing frameworks. There is basically a setup, execute and measure, and teardown section. This directory will be home to the custom tests we define to measure Dolt's performance.
