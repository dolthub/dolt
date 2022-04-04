Sysbench runner is a tool for running sysbench tests against sql servers. Custom sysbench lua scripts used
for benchmarking Dolt are [here](https://github.com/dolthub/sysbench-lua-scripts).

The tool requires a json config file to run:
```bash
$ sysbench_runner --config=config.json
```

Configuration:

```json
{
  "Runs": 1,
  "DebugMode": false,
  "Servers": "[{...}]",
  "TestOptions": [""],
  "Tests": "[{...}]"
}
```

`Runs` number of times to run all tests per server, default 1 (**Optional**)

`DebugMode` logs more output from various commands. (**Optional**)

`Servers` list of servers to test against. See `Server` definitions below. (**Required**)

`TestOptions` list of sysbench test options to supply to all tests (**Optional**)

`Tests` the sysbench tests to run. See `Test` definitions below. (**Optional**) 

If no tests are provided,
the following default tests will be run:
```
oltp_read_only
oltp_insert
oltp_point_select
select_random_points
select_random_ranges
oltp_delete
oltp_write_only
oltp_read_write
oltp_update_index
oltp_update_non_index
```

`Server` is a server to test against.

```json
{
  "Host": "",
  "Port": 0,
  "Server": "",
  "Version": "",
  "ResultsFormat": "",
  "ServerExec": "",
  "ServerArgs": [""],
  "ConnectionProtocol": "",
  "Socket": ""
}
```

`Host` is the server host. (**Required**)

`Port` is the server port. Defaults to **3306** for `dolt` and `mysql` Servers. (**Optional**)

`Server` is the server. Only `dolt` and `mysql` are supported. (**Required**)

`Version` is the server version. (**Required**)

`ResultsFormat` is the format the results should be written in. Only `json` and `csv` are supported. (**Required**)

`ServerExec` is the path to a server binary (**Required**)

`ServerArgs` are the args used to start the server. Will be appended to command `dolt sql-server` for dolt server or `mysqld --user=mysql` for mysql server. (**Optional**)

`ConnectionProtocol` is the protocol for connecting to mysql, either "unix" or "tcp" (**Required for mysql**)

`Socket` is the path to the mysql socket (**Required for mysql with unix protocol**)


`Test` is a sysbench test or lua script.

```json
{
  "Name": "",
  "N": 1,
  "FromScript": false,
  "Options": [""]
}
```

`Name` is the test name or lua script. (**Required**)

`N` number of times to repeat this test, default is 1 (**Optional**)

`FromScript` indicates if this test is from a lua script, defaults to `false` (**Optional**)

`Options` are additional sysbench test options. These will be provided to sysbench in the form:

`sysbench [options]... [testname] [command]`

Note: Be sure that all mysql processes are off when running this locally.
