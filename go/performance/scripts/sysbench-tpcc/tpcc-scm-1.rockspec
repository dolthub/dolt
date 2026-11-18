package = "tpcc"
version = "scm-1"
source = {
   url = "git+https://github.com/Percona-Lab/sysbench-tpcc/"
}

description = {
   summary =
   "TPCC-like workload for sysbench",
   detailed = [[
## prepare data and tables

./tpcc.lua --mysql-socket=/tmp/mysql.sock --mysql-user=root --mysql-db=sbt --time=300 --threads=64 --report-interval=1 --tables=10 --scale=100 --db-driver=mysql prepare

## prepare for RocksDB

./tpcc.lua --mysql-socket=/tmp/mysql.sock --mysql-user=root --mysql-db=sbr --time=3000 --threads=64 --report-interval=1 --tables=10 --scale=100 --use_fk=0 --mysql_storage_engine=rocksdb --mysql_table_options='COLLATE latin1_bin' --trx_level=RC --db-driver=mysql prepare

## Run benchmark

./tpcc.lua --mysql-socket=/tmp/mysql.sock --mysql-user=root --mysql-db=sbt --time=300 --threads=64 --report-interval=1 --tables=10 --scale=100 --db-driver=mysql run

## Cleanup

./tpcc.lua --mysql-socket=/tmp/mysql.sock --mysql-user=root --mysql-db=sbt --time=300 --threads=64 --report-interval=1 --tables=10 --scale=100 --db-driver=mysql cleanup
]],
   homepage = "https://github.com/Percona-Lab/sysbench-tpcc/",
   license = "Apache-2.0"
}

dependencies = {
   "lua == 5.1"
}

build = {
   type = "builtin",
   modules = {
      tpcc = "tpcc.lua",
      tpcc_check = "tpcc_check.lua",
      tpcc_common = "tpcc_common.lua",
      tpcc_run = "tpcc_run.lua"
  }
}
