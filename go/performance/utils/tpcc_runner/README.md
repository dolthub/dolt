TPCC runner is a tool for running TPCC tests against sql servers. These tests run against the 
Percona Labs repo [here](https://github.com/Percona-Lab/sysbench-tpcc).

The tool requires a json config file to run.

```bash
$ go run cmd/main.go --config=sample-tpcc-config.json
```

Note to this run this locally you need to have the TPCC repo cloned. The `ScriptDir` variable should then be linked
to the path of the cloned repo.

Configuration:

```json
{
  "Servers": "[...]",
  "ScriptDir":"/Users/vinairachakonda/go/src/dolthub/sysbench-tpcc",
  "ScaleFactors": [1]
}
```

`Servers`: The server defintions to run the benchmark against. Accepts Dolt and MySQL configuratiosn.

`ScriptDir`: The directory of the TPCC testing scripts

`ScaleFactors`: The number of warehouse to be generated in the test case. 

Note that this configuration is still incomplete for the amount of the variable TPCC varies. This intentional as we 
want expose small amounts of independent variables until Dolt gets more robust. See `config.go` to get a breakdown of all the
variables TPCC varies.

As of now the TPCC runner test requires the environment variable `DOLT_TRANSACTION_MERGE_STOMP` to be 1.