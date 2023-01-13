// Copyright 2022 Dolthub, Inc.
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

package binlogreplication

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TODO: Will we need to run this in a Docker container? Will we have the mysqld binary around?

var mySqlPort, doltPort int
var primaryDatabase, replicaDatabase *sqlx.DB
var mySqlProcess, doltProcess *os.Process
var doltLogFile *os.File
var testDir string
var originalWorkingDir string

func teardown(_ *testing.T) {
	if mySqlProcess != nil {
		mySqlProcess.Kill()
	}
	if doltProcess != nil {
		doltProcess.Kill()
	}
	if toxiClient != nil {
		proxies, _ := toxiClient.Proxies()
		for _, value := range proxies {
			value.Delete()
		}
	}
	// TODO: clean up temp files
	//defer os.RemoveAll(testDir)
}

// TestBinlogReplicationSanityCheck performs the simplest possible binlog replication test. It starts up
// a MySQL primary and a Dolt replica, and asserts that a CREATE TABLE statement properly replicates to the
// Dolt replica.
func TestBinlogReplicationSanityCheck(t *testing.T) {
	startSqlServers(t)
	startReplication(t, mySqlPort)
	defer teardown(t)

	// Make changes on the primary and verify on the replica
	primaryDatabase.MustExec("create table t (pk int primary key)")
	time.Sleep(1 * time.Second)
	expectedStatement := "CREATE TABLE t ( pk int NOT NULL, PRIMARY KEY (pk)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"
	assertCreateTableStatement(t, replicaDatabase, "t", expectedStatement)
}

// TestResetReplica tests that "RESET REPLICA" and "RESET REPLICA ALL" correctly clear out
// replication configuration and metadata.
func TestResetReplica(t *testing.T) {
	startSqlServers(t)
	startReplication(t, mySqlPort)
	defer teardown(t)

	// RESET REPLICA returns an error if replication is running
	rows, err := replicaDatabase.Queryx("RESET REPLICA")
	require.Error(t, err)
	require.ErrorContains(t, err, "unable to reset replica while replication is running")

	// Calling RESET REPLICA clears out any errors
	replicaDatabase.MustExec("STOP REPLICA;")
	rows, err = replicaDatabase.Queryx("RESET REPLICA;")
	require.NoError(t, err)
	rows, err = replicaDatabase.Queryx("SHOW REPLICA STATUS;")
	require.NoError(t, err)
	status := convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "0", status["Last_Errno"])
	require.Equal(t, "", status["Last_Error"])
	require.Equal(t, "0", status["Last_IO_Errno"])
	require.Equal(t, "", status["Last_IO_Error"])
	require.Equal(t, "", status["Last_IO_Error_Timestamp"])
	require.Equal(t, "0", status["Last_SQL_Errno"])
	require.Equal(t, "", status["Last_SQL_Error"])
	require.Equal(t, "", status["Last_SQL_Error_Timestamp"])

	// Calling RESET REPLICA ALL clears out all replica configuration
	rows, err = replicaDatabase.Queryx("RESET REPLICA ALL;")
	require.NoError(t, err)
	rows, err = replicaDatabase.Queryx("SHOW REPLICA STATUS;")
	require.NoError(t, err)
	require.False(t, rows.Next())
	rows, err = replicaDatabase.Queryx("select * from mysql.slave_master_info;")
	require.NoError(t, err)
	require.False(t, rows.Next())
}

// TestStartReplicaErrors tests that the "START REPLICA" command returns appropriate responses
// for various error conditions.
func TestStartReplicaErrors(t *testing.T) {
	startSqlServers(t)
	defer teardown(t)

	// START REPLICA returns an error when no replication source is configured
	_, err := replicaDatabase.Queryx("START REPLICA;")
	require.Error(t, err)
	require.ErrorContains(t, err, ErrServerNotConfiguredAsReplica.Error())

	// For partial source configuration, START REPLICA doesn't throw an error, but an error will
	// be populated in SHOW REPLICA STATUS after START REPLICA returns.
	//START REPLICA doesn't returns an error when replication source is only partially configured
	replicaDatabase.MustExec("CHANGE REPLICATION SOURCE TO SOURCE_PORT=1234, SOURCE_HOST='localhost';")
	replicaDatabase.MustExec("START REPLICA;")
	rows, err := replicaDatabase.Queryx("SHOW REPLICA STATUS;")
	require.NoError(t, err)
	status := convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "0", status["Last_Errno"])
	require.Equal(t, "", status["Last_Error"])
	require.Equal(t, "13117", status["Last_IO_Errno"])
	require.NotEmpty(t, status["Last_IO_Error"])
	require.NotEmpty(t, status["Last_IO_Error_Timestamp"])
	require.Equal(t, "0", status["Last_SQL_Errno"])
	require.Equal(t, "", status["Last_SQL_Error"])
	require.Equal(t, "", status["Last_SQL_Error_Timestamp"])

	// START REPLICA doesn't return an error if replication is already running
	startReplication(t, mySqlPort)
	replicaDatabase.MustExec("START REPLICA;")
}

// TestDoltCommits tests that Dolt commits are created and use correct transaction boundaries.
func TestDoltCommits(t *testing.T) {
	startSqlServers(t)
	startReplication(t, mySqlPort)
	defer teardown(t)

	// First transaction (DDL)
	primaryDatabase.MustExec("create table t1 (pk int primary key);")

	// Second transaction (DDL)
	primaryDatabase.MustExec("create table t2 (pk int primary key);")

	// Third transaction (autocommit DML)
	primaryDatabase.MustExec("insert into t2 values (0);")

	// Disable autocommit so we can manually control transactions
	primaryDatabase.MustExec("set autocommit=0;")

	// Fourth transaction (explicitly controlled transaction)
	primaryDatabase.MustExec("start transaction;")
	primaryDatabase.MustExec("insert into t1 values(1);")
	primaryDatabase.MustExec("insert into t1 values(2);")
	primaryDatabase.MustExec("insert into t1 values(3);")
	primaryDatabase.MustExec("insert into t2 values(3), (2), (1);")
	primaryDatabase.MustExec("commit;")

	// Verify Dolt commit on replica
	time.Sleep(500 * time.Millisecond)
	rows, err := replicaDatabase.Queryx("select count(*) as count from db01.dolt_log;")
	require.NoError(t, err)
	row := convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "5", row["count"])

	// Use dolt_diff so we can see what tables were edited and schema/data changes
	// TODO: Error 1105: unable to lookup roots for database
	//       We started getting this when we used a db qualifier... the dolt_diff system table must not take that
	//       into account and always uses the current database.
	// TODO: Convert the note above into a GH bug
	replicaDatabase.MustExec("use db01;")
	// Note: we don't use an order by clause, since the commits come in so quickly that they get the same timestamp
	rows, err = replicaDatabase.Queryx("select * from db01.dolt_diff;")
	require.NoError(t, err)

	// Fourth transaction
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["data_change"])
	require.Equal(t, "0", row["schema_change"])
	require.Equal(t, "t1", row["table_name"])
	commitId := row["commit_hash"]
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["data_change"])
	require.Equal(t, "0", row["schema_change"])
	require.Equal(t, "t2", row["table_name"])
	require.Equal(t, commitId, row["commit_hash"])

	// Third transaction
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["data_change"])
	require.Equal(t, "0", row["schema_change"])
	require.Equal(t, "t2", row["table_name"])

	// Second transaction
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "0", row["data_change"])
	require.Equal(t, "1", row["schema_change"])
	require.Equal(t, "t2", row["table_name"])

	// First transaction
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "0", row["data_change"])
	require.Equal(t, "1", row["schema_change"])
	require.Equal(t, "t1", row["table_name"])
}

// TestBinlogReplicationForAllTypes tests that operations (inserts, updates, and deletes) on all SQL
// data types can be successfully replicated.
// TODO: This doesn't cover all types; look into generating this data (types, min/max values, etc)
func TestBinlogReplicationForAllTypes(t *testing.T) {
	startSqlServers(t)
	startReplication(t, mySqlPort)
	defer teardown(t)

	// Test inserts on the primary – min, max, and null values
	primaryDatabase.MustExec("create table alltypes(pk int primary key auto_increment, n_bit bit, n_bit_64 bit(64), n_tinyint tinyint, n_utinyint tinyint unsigned, n_bool bool, n_smallint smallint, n_usmallint smallint unsigned, n_float float, n_double double, n_ufloat float unsigned, n_udouble double unsigned, d_date date);")
	primaryDatabase.MustExec("insert into alltypes value (DEFAULT, 0, 1, -128, 0, 0, 0 ,0, -0.1234, -0.12345678, 0.0, 0.0, DATE('1981-02-16'));")
	primaryDatabase.MustExec("insert into alltypes value (DEFAULT, 0, 1, 127, 255, 0, 0 ,0, 0.0, 0.0, 0.1234, 0.12345678, DATE('1981-02-16'));")
	primaryDatabase.MustExec("insert into alltypes value (DEFAULT, null, null, null, null, null, null, null, null, null, null, null, null);")

	// Verify on replica
	time.Sleep(1 * time.Second) // TODO: Could get rid of this manually maintained lag by using toxiproxy
	rows, err := replicaDatabase.Queryx("select * from alltypes;")
	require.NoError(t, err)

	// Test min values
	row := readNextRow(t, rows)
	require.Equal(t, "1", string(row["pk"].([]byte)))
	// MapScan doesn't honor the correct type – it converts everything to a []byte string :-/
	// https://github.com/jmoiron/sqlx/issues/225
	require.EqualValues(t, []byte{0}, row["n_bit"])
	require.EqualValues(t, []byte{0, 0, 0, 0, 0, 0, 0, 1}, row["n_bit_64"])
	require.EqualValues(t, "-0.1234", string(row["n_float"].([]byte)))
	require.EqualValues(t, "-0.12345678", string(row["n_double"].([]byte)))
	require.EqualValues(t, "0", string(row["n_ufloat"].([]byte)))
	require.EqualValues(t, "0", string(row["n_udouble"].([]byte)))
	require.EqualValues(t, "0", string(row["n_usmallint"].([]byte)))
	require.EqualValues(t, "1981-02-16", string(row["d_date"].([]byte)))

	// Test max values
	row = readNextRow(t, rows)
	require.Equal(t, "2", string(row["pk"].([]byte)))
	require.Equal(t, "127", string(row["n_tinyint"].([]byte)))
	require.EqualValues(t, "0", string(row["n_float"].([]byte)))
	require.EqualValues(t, "0", string(row["n_double"].([]byte)))
	require.EqualValues(t, "0.1234", string(row["n_ufloat"].([]byte)))
	require.EqualValues(t, "0.12345678", string(row["n_udouble"].([]byte)))
	require.EqualValues(t, "255", string(row["n_utinyint"].([]byte)))

	// Test null values
	row = readNextRow(t, rows)
	require.Equal(t, "3", toString(row["pk"]))
	require.Equal(t, nil, row["n_bit"])
	require.Equal(t, nil, row["n_tinyint"])
	require.EqualValues(t, nil, row["n_usmallint"])
	require.Equal(t, nil, row["d_date"])

	require.False(t, rows.Next())

	// Test updates
	primaryDatabase.MustExec("update alltypes set n_bit=0x01, n_float=123.4, n_tinyint=42 where pk=2;")
	primaryDatabase.MustExec("update alltypes set n_bit=NULL, n_float=NULL, n_tinyint=NULL where pk=1;")

	time.Sleep(1 * time.Second)
	rows, err = replicaDatabase.Queryx("select * from alltypes order by pk;")
	require.NoError(t, err)

	row = readNextRow(t, rows)
	require.Equal(t, "1", toString(row["pk"]))
	require.Nil(t, row["n_bit"])
	require.Nil(t, row["n_float"])
	require.Nil(t, row["n_tinyint"])

	row = readNextRow(t, rows)
	require.Equal(t, "2", toString(row["pk"]))
	require.EqualValues(t, []byte{1}, row["n_bit"])
	require.EqualValues(t, "123.4", string(row["n_float"].([]byte)))
	require.EqualValues(t, "42", string(row["n_tinyint"].([]byte)))

	// Test deletes
	// TODO: Run deletes on primary
	// TODO: Verify on the replica
}

func toString(value interface{}) string {
	if value == nil {
		// TODO: is this right?
		return "NULL"
	} else if bytes, ok := value.([]byte); ok {
		return string(bytes)
	} else {
		panic(fmt.Sprintf("value is not of type []byte, is: %T", value))
	}
}

func readNextRow(t *testing.T, rows *sqlx.Rows) map[string]interface{} {
	row := make(map[string]interface{})
	require.True(t, rows.Next())
	err := rows.MapScan(row)
	require.NoError(t, err)
	return row
}

func startSqlServers(t *testing.T) {
	var err error
	testDir, err = os.MkdirTemp("", t.Name()+"-"+time.Now().Format("12345"))
	require.NoError(t, err)
	fmt.Printf("temp dir: %v \n", testDir)

	// Start up primary and replica databases
	mySqlPort, mySqlProcess, err = startMySqlServer(testDir)
	require.NoError(t, err)
	doltPort, doltProcess, err = startDoltSqlServer(testDir)
	require.NoError(t, err)
}

func stopDoltSqlServer(t *testing.T) {
	err := doltProcess.Signal(os.Interrupt)
	require.NoError(t, err)
	err = doltProcess.Kill()
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
}

func startReplication(t *testing.T, port int) {
	replicaDatabase.MustExec(
		fmt.Sprintf("change replication source to SOURCE_HOST='localhost', SOURCE_USER='root', "+
			"SOURCE_PASSWORD='', SOURCE_PORT=%v;", port))

	replicaDatabase.MustExec("start replica;")
}

func assertCreateTableStatement(t *testing.T, database *sqlx.DB, table string, expectedStatement string) {
	rows, err := database.Query("show create table " + table + ";")
	require.NoError(t, err)
	var actualTable, actualStatement string
	require.True(t, rows.Next())
	err = rows.Scan(&actualTable, &actualStatement)
	require.NoError(t, err)
	require.Equal(t, table, actualTable)
	require.NotNil(t, actualStatement)
	actualStatement = sanitizeCreateTableString(actualStatement)
	require.Equal(t, expectedStatement, actualStatement)
}

func sanitizeCreateTableString(statement string) string {
	statement = strings.ReplaceAll(statement, "`", "")
	statement = strings.ReplaceAll(statement, "\n", "")
	regex := regexp.MustCompile("\\s+")
	return regex.ReplaceAllString(statement, " ")
}

// findFreePort returns an available port that can be used for a server. If any errors are
// encountered, this function will panic and fail the current test.
func findFreePort() int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(fmt.Sprintf("unable to find available TCP port: %v", err.Error()))
	}
	mySqlPort := listener.Addr().(*net.TCPAddr).Port
	err = listener.Close()
	if err != nil {
		panic(fmt.Sprintf("unable to find available TCP port: %v", err.Error()))
	}

	return mySqlPort
}

// startMySqlServer configures a starts a fresh MySQL server instance and returns the port it is running on,
// and the os.Process handle. If unable to start up the MySQL server, an error is returned.
func startMySqlServer(dir string) (int, *os.Process, error) {
	originalCwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	dir = dir + string(os.PathSeparator) + "mysql" + string(os.PathSeparator)
	dataDir := dir + "mysql_data"
	err = os.MkdirAll(dir, 0700)
	if err != nil {
		return -1, nil, err
	}
	err = os.Chdir(dir)
	if err != nil {
		return -1, nil, err
	}

	mySqlPort = findFreePort()

	// Create a fresh MySQL server for the primary
	cmd := exec.Command("mysqld", "--initialize-insecure", "--user=root", "--datadir="+dataDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return -1, nil, fmt.Errorf("unable to execute command %v: %v – %v", cmd.String(), err.Error(), string(output))
	}

	cmd = exec.Command("mysqld",
		"--datadir="+dataDir,
		"--default-authentication-plugin=mysql_native_password",
		"--gtid-mode=ON",
		"--enforce-gtid-consistency=ON",
		fmt.Sprintf("--port=%v", mySqlPort),
		"--server-id=11223344",
		fmt.Sprintf("--socket=mysql-%v.sock", mySqlPort),
		"--binlog-checksum=NONE",
		"--general_log_file="+dir+"general_log",
		"--log-bin="+dir+"log_bin",
		"--slow_query_log_file="+dir+"slow_query_log",
		"--log-error="+dir+"log_error",
		fmt.Sprintf("--pid-file="+dir+"pid-%v.pid", mySqlPort))
	err = cmd.Start()
	if err != nil {
		// TODO: We should capture the process output here (without blocking for process completion)
		//       to help debug any mysql startup errors.
		return -1, nil, fmt.Errorf("unable to start process %q: %v", cmd.String(), err.Error())
	}

	dsn := fmt.Sprintf("root@tcp(127.0.0.1:%v)/", mySqlPort)
	primaryDatabase = sqlx.MustOpen("mysql", dsn)

	err = waitForSqlServerToStart(primaryDatabase)
	if err != nil {
		return -1, nil, err
	}

	// Create the initial database on the MySQL server
	primaryDatabase.MustExec("create database db01;")

	dsn = fmt.Sprintf("root@tcp(127.0.0.1:%v)/db01", mySqlPort)
	primaryDatabase = sqlx.MustOpen("mysql", dsn)

	os.Chdir(originalCwd)

	fmt.Printf("MySQL server started on port %v \n", mySqlPort)

	return mySqlPort, cmd.Process, nil
}

func startDoltSqlServer(dir string) (int, *os.Process, error) {
	dir = filepath.Join(dir, "dolt")
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		return -1, nil, err
	}

	doltPort = findFreePort()

	// take the CWD and move up four directories to find the go directory
	if originalWorkingDir == "" {
		var err error
		originalWorkingDir, err = os.Getwd()
		if err != nil {
			panic(err)
		}
	}
	goDirPath := filepath.Join(originalWorkingDir, "..", "..", "..", "..")
	err = os.Chdir(goDirPath)
	if err != nil {
		panic(err)
	}

	cmd := exec.Command("go", "run", "./cmd/dolt",
		"sql-server",
		"-uroot",
		"--loglevel=DEBUG",
		fmt.Sprintf("--data-dir=%s", dir),
		fmt.Sprintf("--port=%v", doltPort),
		fmt.Sprintf("--socket=dolt.%v.sock", doltPort))

	doltLogFilePath := filepath.Join(dir, fmt.Sprintf("dolt-%d.out.log", time.Now().Unix()))
	doltLogFile, err = os.Create(doltLogFilePath)
	if err != nil {
		return -1, nil, err
	}
	fmt.Printf("dolt sql-server logs at: %s \n", doltLogFilePath)
	cmd.Stdout = doltLogFile
	cmd.Stderr = doltLogFile
	err = cmd.Start()
	if err != nil {
		return -1, nil, fmt.Errorf("unable to execute command %v: %v", cmd.String(), err.Error())
	}

	dsn := fmt.Sprintf("root@tcp(127.0.0.1:%v)/", doltPort)
	replicaDatabase = sqlx.MustOpen("mysql", dsn)

	err = waitForSqlServerToStart(replicaDatabase)
	if err != nil {
		return -1, nil, err
	}

	fmt.Printf("Dolt server started on port %v \n", doltPort)

	return doltPort, cmd.Process, nil
}

// waitForSqlServerToStart polls the specified database to wait for it to become available, pausing
// between retry attempts, and returning an error if it is not able to verify that the database is
// available.
func waitForSqlServerToStart(database *sqlx.DB) error {
	for counter := 0; counter < 10; counter++ {
		if database.Ping() == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return database.Ping()
}

// comparePrimaryAndReplicaTable asserts that the specified |table| is identical in the MySQL primary
// and in the Dolt replica.
// TODO: Finish this experiment; try dumping the mysql db and loading it into a dolt branch to diff.
func comparePrimaryAndReplicaTable(t *testing.T, table string) {
	dumpFile, err := os.Create("/tmp/mysql.dump")
	require.NoError(t, err)
	defer dumpFile.Close()

	cmd := exec.Command("mysqldump", "--protocol=TCP", "--user=root", fmt.Sprintf("--port=%v", mySqlPort), "db01", table)
	cmd.Stdout = dumpFile
	err = cmd.Run()
	require.NoError(t, err)

	// TODO: Now we need to load our dump into a new dolt database
	//       - we could read each line of the file and execute it against our Dolt database (with a new db)
	//         TODO: If all databases are made read only, then will this still work?
	//               We could shut down the database and then import with `dolt sql < mysql.dump`
	fmt.Printf("Created dump file at: /tmp/mysql.dump \n")
}
