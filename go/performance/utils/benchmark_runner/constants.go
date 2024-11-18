// Copyright 2019-2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package benchmark_runner

import "time"

const (
	Dolt     ServerType = "dolt"
	Doltgres ServerType = "doltgres"
	Postgres ServerType = "postgres"
	MySql    ServerType = "mysql"

	CsvFormat  = "csv"
	JsonFormat = "json"

	CsvExt  = ".csv"
	JsonExt = ".json"

	CpuServerProfile ServerProfile = "cpu"

	defaultHost         = "127.0.0.1"
	defaultDoltPort     = 3306
	defaultMysqlPort    = defaultDoltPort
	defaultDoltgresPort = 5432
	defaultPostgresPort = defaultDoltgresPort

	defaultMysqlSocket = "/var/run/mysqld/mysqld.sock"

	tcpProtocol  = "tcp"
	unixProtocol = "unix"

	sysbenchUsername             = "sysbench"
	sysbenchUserLocal            = "'sysbench'@'localhost'"
	sysbenchPassLocal            = "sysbenchpass"
	sysbenchDbPsModeFlag         = "--db-ps-mode"
	sysbenchDbPsModeDisable      = "disable"
	sysbenchRandTypeFlag         = "--rand-type"
	sysbenchRandTypeUniform      = "uniform"
	sysbenchMysqlDbFlag          = "--mysql-db"
	sysbenchDbDriverFlag         = "--db-driver"
	sysbenchMysqlHostFlag        = "--mysql-host"
	sysbenchMysqlPortFlag        = "--mysql-port"
	sysbenchMysqlUserFlag        = "--mysql-user"
	sysbenchMysqlPasswordFlag    = "--mysql-password"
	sysbenchPostgresDbDriver     = "pgsql"
	sysbenchPostgresDbFlag       = "--pgsql-db"
	sysbenchPostgresHostFlag     = "--pgsql-host"
	sysbenchPostgresPortFlag     = "--pgsql-port"
	sysbenchPostgresUserFlag     = "--pgsql-user"
	sysbenchPostgresPasswordFlag = "--pgsql-password"

	doltSqlServerCommand = "sql-server"

	configFlag                  = "--config"
	userFlag                    = "--user"
	hostFlag                    = "--host"
	portFlag                    = "--port"
	skipBinLogFlag              = "--skip-log-bin"
	profileFlag                 = "--prof"
	profilePathFlag             = "--prof-path"
	cpuProfile                  = "cpu"
	doltgresDataDirFlag         = "--data-dir"
	MysqlDataDirFlag            = "--datadir"
	MysqlInitializeInsecureFlag = "--initialize-insecure"
	cpuProfileFilename          = "cpu.pprof"

	sysbenchOltpReadOnlyTestName       = "oltp_read_only"
	sysbenchOltpInsertTestName         = "oltp_insert"
	sysbenchBulkInsertTestName         = "bulk_insert"
	sysbenchOltpPointSelectTestName    = "oltp_point_select"
	sysbenchSelectRandomPointsTestName = "select_random_points"
	sysbenchSelectRandomRangesTestName = "select_random_ranges"
	sysbenchOltpWriteOnlyTestName      = "oltp_write_only"
	sysbenchOltpReadWriteTestName      = "oltp_read_write"
	sysbenchOltpUpdateIndexTestName    = "oltp_update_index"
	sysbenchOltpUpdateNonIndexTestName = "oltp_update_non_index"

	sysbenchCoveringIndexScanLuaTestName = "covering_index_scan.lua"
	sysbenchGroupByScanLuaTestName       = "groupby_scan.lua"
	sysbenchIndexJoinLuaTestName         = "index_join.lua"
	sysbenchIndexJoinScanLuaTestName     = "index_join_scan.lua"
	sysbenchIndexScanLuaTestName         = "index_scan.lua"
	sysbenchOltpDeleteInsertLuaTestName  = "oltp_delete_insert.lua"
	sysbenchTableScanLuaTestName         = "table_scan.lua"
	sysbenchTypesDeleteInsertLuaTestName = "types_delete_insert.lua"
	sysbenchTypesTableScanLuaTestName    = "types_table_scan.lua"

	sysbenchCoveringIndexScanPostgresLuaTestName = "covering_index_scan_postgres.lua"
	sysbenchGroupByScanPostgresLuaTestName       = "groupby_scan_postgres.lua"
	sysbenchIndexJoinPostgresLuaTestName         = "index_join_postgres.lua"
	sysbenchIndexJoinScanPostgresLuaTestName     = "index_join_scan_postgres.lua"
	sysbenchIndexScanPostgresLuaTestName         = "index_scan_postgres.lua"
	sysbenchOltpDeleteInsertPostgresLuaTestName  = "oltp_delete_insert_postgres.lua"
	sysbenchTableScanPostgresLuaTestName         = "table_scan_postgres.lua"
	sysbenchTypesDeleteInsertPostgresLuaTestName = "types_delete_insert_postgres.lua"
	sysbenchTypesTableScanPostgresLuaTestName    = "types_table_scan_postgres.lua"

	doltConfigUsernameKey = "user.name"
	doltConfigEmailKey    = "user.email"
	doltBenchmarkUser     = "benchmark"
	doltBenchmarkEmail    = "benchmark@dolthub.com"
	doltConfigCommand     = "config"
	doltConfigGlobalFlag  = "--global"
	doltConfigGetFlag     = "--get"
	doltConfigAddFlag     = "--add"
	doltCloneCommand      = "clone"
	doltVersionCommand    = "version"
	doltInitCommand       = "init"
	dbName                = "test"
	bigEmptyRepo          = "max-hoffman/big-empty"
	nbfEnvVar             = "DOLT_DEFAULT_BIN_FORMAT"

	postgresDriver         = "postgres"
	doltgresUser           = "postgres"
	doltgresPassword       = "password"
	doltDataDir            = ".dolt"
	createDatabaseTemplate = "create database %s;"
	psqlDsnTemplate        = "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable"
	doltgresDsnTemplate    = "host=%s port=%d user=%s password=%s sslmode=disable"
	doltgresVersionCommand = "-version"

	expectedServerKilledErrorMessage     = "signal: killed"
	expectedServerTerminatedErrorMessage = "signal: terminated"

	sysbenchCommand        = "sysbench"
	sysbenchVersionFlag    = "--version"
	sysbenchPrepareCommand = "prepare"
	sysbenchRunCommand     = "run"
	sysbenchCleanupCommand = "cleanup"
	luaPathEnvVarTemplate  = "LUA_PATH=%s"
	luaPath                = "?.lua"

	defaultMysqlUser = "root"

	// Note this is built for the SysbenchDocker file. If you want to run locally you'll need to override these variables
	// for your local MySQL setup.
	tpccUserLocal = "'sysbench'@'localhost'"
	tpccPassLocal = "sysbenchpass"

	tpccDbName              = "sbt"
	tpccScaleFactorTemplate = "tpcc-scale-factor-%d"

	tpccDbDriverFlag         = "--db-driver"
	tpccMysqlUsername        = "sysbench"
	tpccMysqlDbFlag          = "--mysql-db"
	tpccMysqlHostFlag        = "--mysql-host"
	tpccMysqlUserFlag        = "--mysql-user"
	tpccMysqlPasswordFlag    = "--mysql-password"
	tpccMysqlPortFlag        = "--mysql-port"
	tpccTimeFlag             = "--time"
	tpccThreadsFlag          = "--threads"
	tpccReportIntervalFlag   = "--report_interval"
	tpccTablesFlag           = "--tables"
	tpccScaleFlag            = "--scale"
	tpccTransactionLevelFlag = "--trx_level"
	tpccReportCsv            = "reportCsv"
	tpccTransactionLevelRr   = "RR"
	tpccLuaFilename          = "tpcc.lua"

	mysqlDriverName                  = "mysql"
	mysqlRootTCPDsnTemplate          = "root@tcp(%s:%d)/"
	mysqlRootUnixDsnTemplate         = "root@unix(%s)/"
	mysqlDropDatabaseSqlTemplate     = "DROP DATABASE IF EXISTS %s;"
	mysqlCreateDatabaseSqlTemplate   = "CREATE DATABASE %s;"
	mysqlDropUserSqlTemplate         = "DROP USER IF EXISTS %s;"
	mysqlCreateUserSqlTemplate       = "CREATE USER %s IDENTIFIED WITH mysql_native_password BY '%s';"
	mysqlGrantPermissionsSqlTemplate = "GRANT ALL ON %s.* to %s;"
	mysqlSetGlobalLocalInfileSql     = "SET GLOBAL local_infile = 'ON';"
	mysqlSetGlobalSqlModeSql         = "SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));"

	postgresInitDbDataDirFlag         = "--pgdata"
	postgresUsernameFlag              = "--username"
	postgresUsername                  = "postgres"
	postgresDataDirFlag               = "-D"
	postgresDropDatabaseSqlTemplate   = "DROP DATABASE IF EXISTS %s;"
	postgresDropUserSqlTemplate       = "DROP USER IF EXISTS %s;"
	postgresCreateUserSqlTemplate     = "CREATE USER %s WITH PASSWORD '%s';"
	postgresCreateDatabaseSqlTemplate = "CREATE DATABASE %s WITH OWNER %s;"
	postgresLcAllEnvVarKey            = "LC_ALL"
	postgresLcAllEnvVarValue          = "C"

	resultsDirname = "results"
	stampFormat    = time.RFC3339
	SqlStatsPrefix = "SQL statistics:"
	read           = "read"
	write          = "write"
	other          = "other"
	totalQueries   = "total"
	totalEvents    = "total number of events"
	min            = "min"
	avg            = "avg"
	max            = "max"
	percentile     = "percentile"
	sum            = "sum"
	transactions   = "transactions"
	queriesPerSec  = "queries"
	ignoredErrors  = "ignored errors"
	reconnects     = "reconnects"
	totalTimeSecs  = "total time"

	ResultFileTemplate = "%s_%s_%s_sysbench_performance%s"
)
