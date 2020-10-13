## Background
This folder contains a toolkit for benchmarking Dolt SQL performance. [`sysbench`](https://github.com/akopytov/sysbench) is an industry standard tool for benchmarking database performance, particularly MySQL. This directory contains a set of tools for benchmarking Dolt using `sysbench` in a way that should make it convenient for Dolt contributors to measure the performance benefits their changes deliver.

## Architecture
The basic goal of these tools is, given a commit, to:
- run Dolt SQL server
- connect to it using the [`sysbench` MySQL driver](https://github.com/akopytov/sysbench/tree/master/src/drivers/mysql)
- execute the benchmarks specified

In order to to do this in a repeatable way we build Dolt at the specified commit inside a Docker container. We then launch a separate container, install `sysbench`, mount the binary we produced in the build step, and execute the specified benchmarks from inside that container.

In the future we will want to provide the ability for users to execute these benchmarks on a wider set of infrastructure. As Dolt becomes more mature it will become increasingly necessary to have finer grained benchmarking. For now this should suffice as a tool for contributors to identify the efficacy of their changes.

## Components
We briefly outline the components used. The main entry points are `run_benchmarks.sh` which takes care of building and running the benchmarks, and `push_output_to_dolthub.py`, which publishes benchmarking results to DoltHub. We also discuss `sysbench_scripts`, which is a way of exposing the `sysbench` scripting API.

### `run_benchmarks.sh`
This is the top level entry point for these tools. Suppose we want to compare our local, potentially dirty, copy of Dolt to some commit, say commit `19f9e571d033374ceae2ad5c277b9cfe905cdd66`. We would then run the following:
```
$ ./run_benchmarks.sh bulk_insert oscarbatori 19f9e571d033374ceae2ad5c277b9cfe905cdd66
```

This will tell `run_benchmarks.sh` to take the following actions:
- build a copy of Dolt at the current commit (noting in the binary name if it is dirty)
- clone Dolt, checkout the specified commit, and build a binary
- run the specified `sysbench` tests with both binaries

If the result of `$(git rev-parse HEAD)` is `c84f34075e55a8e3322f584986ecf808076b309c`, and the repo is dirty then we will get the following:
```
output/c84f34075e55a8e3322f584986ecf808076b309c.csv
output/19f9e571d033374ceae2ad5c277b9cfe905cdd66.csv
```

Each one of these files will contain a line for each test we specified.

### Requirements
To run this stack a few things are required:
- a bash shell to execute `run_benchmarks.sh`
- Docker: the binaries are built inside a container
- Python: if you want to upload results to DoltHub, we use Doltpy's tools for pushing data to DoltHub

### Uploading to DoltHub
We can upload the results to DoltHub using `push_results_to_dolthub.py` as follows:
```
python push_outputp_to_dolthub.py --result-directory output --remote-results-db dolthub/dolt-benchmarks-test --branch test-run
```

These results will then be available to the team for analysis, and via our API for rendering on our benchmarking documentation.

### `sysbench_scripts`
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
