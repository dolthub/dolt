#!/bin/bash

[ ! -d "./sysbench-tpcc" ] && \
	echo "run 'git clone git@github.com:Percona-Lab/sysbench-tpcc.git' to gather tpcc scripts" && \
	exit 1

echo "success"