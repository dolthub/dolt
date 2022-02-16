#!/bin/bash

set -eo pipefail

[ ! -d "./sysbench-tpcc" ] && \
	echo "run 'git clone git@github.com:Percona-Lab/sysbench-tpcc.git' to gather tpcc scripts" && \
	exit 1

# parse options
# superuser.com/questions/186272/
while test $# -gt 0
do
    case "$1" in

        # benchmark with new NomsBinFmt
        --new-nbf) export DOLT_FORMAT_FEATURE_FLAG=true
            ;;

    esac
    shift
done

DOLTPATH=`which dolt`

cat <<JSON > tpcc-config.json
{
  "Servers": [
    {
      "Host": "127.0.0.1",
      "Port": 3307,
      "Server": "dolt",
      "Version": "HEAD",
      "ResultsFormat": "csv",
      "ServerExec": "$DOLTPATH"
    }
  ],
  "ScriptDir":"./sysbench-tpcc",
  "ScaleFactors": [1]
}
JSON

go run cmd/main.go --config=tpcc-config.json

# cleanup our env var
unset DOLT_FORMAT_FEATURE_FLAG

echo "success"