## Overview
This folder contains a Python script that wraps [`sysbench`](https://github.com/akopytov/sysbench), an open source tool commonly used for benchmarking databases. The tool provides a number of pre-canned tests, and exposes an API and hooks for users to define their own tests in Lua scripts. 

Our Python script performs the following functions:
- stands up and tears down a test Dolt database on which to execute benchmarks
- executes the benchmarks and parses the results in to lines of a CSV file
- writes the results to a DoltHub hosted Dolt database containing all the data pulled from the run

The goal of doing this is as follows:
- make it easy for the team to produce new benchmarks against new versions of Dolt, and track the results
- provide a wrapper that we can extend and evolve over time that will be our primary entry point for running benchmarks
- make it easy to configure automated runs of the benchmark against fresh builds of Dolt

The spec that drove can be found [here](https://docs.google.com/document/d/1fEosVGOZlDGRvM1ui1cZxuPgCQ4glpAdipnYAy5fFeA/edit), but is private to the DoltHub team. We will replace this reference with a link to a blog post detailing our efforts in the future. 
  

## Dependencies
It depends on the following components, as well as Dolt:
- Python (tested on 3.8), provided by CI run time (GitHub Actions)
- Doltpy, installed via `pip`
- `systbench`, installed via a platform appropriate package manager



## Running a Benchmark
An example of running the benchmark script is as follows:
```
python main.py \
--tests=bulk_insert \
--print-results \
--write-results-to-dolthub \
--remote-results-db dolthub/dolt-benchmarks-test \
--remote-results-db-branch test-run \
--note 'test run'
```

This will print the results of the benchmark, but also clone `dolthub/dolt-benchmarks-test` and insert the parsed data into a table with the following schema:
```sql
CREATE TABLE sysbench_benchmark (
  `database` VARCHAR(16),
  `username` VARCHAR(32),
  `note` LONGTEXT,
  `timestamp` DATETIME,
  `dolt_version` VARCHAR(16),
  `system_info` VARCHAR(32),
  `test_name` VARCHAR(32),
  `sql_read_queries` INTEGER,
  `sql_write_queries` INTEGER,
  `sql_other_queries` INTEGER,
  `sql_total_queries` INTEGER,
  `sql_transactions` INTEGER,
  `sql_ignored_errors` INTEGER,
  `sql_reconnects` INTEGER,
  `total_time` FLOAT,
  `total_number_of_events` INTEGER,
  `latency_minimum` FLOAT,
  `latency_average` FLOAT,
  `latency_maximum` FLOAT,
  `latency_percentile_95th` FLOAT,
  `latency_sum` FLOAT,
  PRIMARY KEY (`username`, `timestamp`, `dolt_version`)
)
```

## Immediate To Do
The most immediate gains we can make in benchmarking quality can be made by executing the following steps:
- get the current script running in a benchmark on the pushing of a new tag
- get auto-increment working to a wider set of benchmarks start working
- enhance the script to stand up a Dolt instance running on an EC2 spot-instance (or something similar) to benchmark on something other than a subprocess