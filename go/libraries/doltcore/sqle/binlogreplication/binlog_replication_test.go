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
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

var mySqlPort, doltPort int
var primaryDatabase, replicaDatabase *sqlx.DB
var mySqlProcess, doltProcess *os.Process
var doltLogFilePath, oldDoltLogFilePath, mysqlLogFilePath string
var doltLogFile, mysqlLogFile *os.File
var testDir string
var originalWorkingDir string

func teardown(t *testing.T) {
	if mySqlProcess != nil {
		mySqlProcess.Kill()
	}
	if doltProcess != nil {
		doltProcess.Kill()
	}
	if mysqlLogFile != nil {
		mysqlLogFile.Close()
	}
	if doltLogFile != nil {
		doltLogFile.Close()
	}

	// Output server logs on failure for easier debugging
	if t.Failed() {
		if oldDoltLogFilePath != "" {
			fmt.Printf("\nDolt server log from %s:\n", oldDoltLogFilePath)
			printFile(oldDoltLogFilePath)
		}

		fmt.Printf("\nDolt server log from %s:\n", doltLogFilePath)
		printFile(doltLogFilePath)
		fmt.Printf("\nMySQL server log from %s:\n", mysqlLogFilePath)
		printFile(mysqlLogFilePath)
	} else {
		// clean up temp files on clean test runs
		defer os.RemoveAll(testDir)
	}

	if toxiClient != nil {
		proxies, _ := toxiClient.Proxies()
		for _, value := range proxies {
			value.Delete()
		}
	}
}

// TestBinlogReplicationSanityCheck performs the simplest possible binlog replication test. It starts up
// a MySQL primary and a Dolt replica, and asserts that a CREATE TABLE statement properly replicates to the
// Dolt replica.
func TestBinlogReplicationSanityCheck(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	startReplication(t, mySqlPort)

	// Make changes on the primary and verify on the replica
	primaryDatabase.MustExec("create table t (pk int primary key)")
	waitForReplicaToCatchUp(t)
	expectedStatement := "CREATE TABLE t ( pk int NOT NULL, PRIMARY KEY (pk)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"
	assertCreateTableStatement(t, replicaDatabase, "t", expectedStatement)
	assertRepoStateFileExists(t, "db01")
}

// TestResetReplica tests that "RESET REPLICA" and "RESET REPLICA ALL" correctly clear out
// replication configuration and metadata.
func TestResetReplica(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	startReplication(t, mySqlPort)

	// RESET REPLICA returns an error if replication is running
	_, err := replicaDatabase.Queryx("RESET REPLICA")
	require.Error(t, err)
	require.ErrorContains(t, err, "unable to reset replica while replication is running")

	// Calling RESET REPLICA clears out any errors
	replicaDatabase.MustExec("STOP REPLICA;")
	rows, err := replicaDatabase.Queryx("RESET REPLICA;")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

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
	require.NoError(t, rows.Close())

	// Calling RESET REPLICA ALL clears out all replica configuration
	rows, err = replicaDatabase.Queryx("RESET REPLICA ALL;")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	rows, err = replicaDatabase.Queryx("SHOW REPLICA STATUS;")
	require.NoError(t, err)
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())

	rows, err = replicaDatabase.Queryx("select * from mysql.slave_master_info;")
	require.NoError(t, err)
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
}

// TestStartReplicaErrors tests that the "START REPLICA" command returns appropriate responses
// for various error conditions.
func TestStartReplicaErrors(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)

	// START REPLICA returns an error if server_id has not been set to a non-zero value
	_, err := replicaDatabase.Queryx("START REPLICA;")
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid server ID configured")

	replicaDatabase.MustExec("SET @@GLOBAL.server_id=4321")

	// START REPLICA returns an error when no replication source is configured
	_, err = replicaDatabase.Queryx("START REPLICA;")
	require.Error(t, err)
	require.ErrorContains(t, err, ErrServerNotConfiguredAsReplica.Error())

	// For partial source configuration, START REPLICA doesn't throw an error, but an error will
	// be populated in SHOW REPLICA STATUS after START REPLICA returns.
	//START REPLICA doesn't return an error when replication source is only partially configured
	replicaDatabase.MustExec("CHANGE REPLICATION SOURCE TO SOURCE_PORT=1234, SOURCE_HOST='localhost';")
	replicaDatabase.MustExec("START REPLICA;")
	rows, err := replicaDatabase.Queryx("SHOW REPLICA STATUS;")
	require.NoError(t, err)
	status := convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "13117", status["Last_IO_Errno"])
	require.NotEmpty(t, status["Last_IO_Error"])
	require.NotEmpty(t, status["Last_IO_Error_Timestamp"])
	require.NoError(t, rows.Close())

	// START REPLICA doesn't return an error if replication is already running
	startReplication(t, mySqlPort)
	replicaDatabase.MustExec("START REPLICA;")
}

// TestDoltCommits tests that Dolt commits are created and use correct transaction boundaries.
func TestDoltCommits(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	startReplication(t, mySqlPort)

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
	waitForReplicaToCatchUp(t)
	rows, err := replicaDatabase.Queryx("select count(*) as count from db01.dolt_log;")
	require.NoError(t, err)
	row := convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "5", row["count"])
	require.NoError(t, rows.Close())

	// Use dolt_diff so we can see what tables were edited and schema/data changes
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

	require.NoError(t, rows.Close())
}

// TestForeignKeyChecks tests that foreign key constraints replicate correctly when foreign key checks are
// enabled and disabled.
func TestForeignKeyChecks(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	startReplication(t, mySqlPort)

	// Insert a record with a foreign key check
	primaryDatabase.MustExec("CREATE TABLE colors (name varchar(100) primary key);")
	primaryDatabase.MustExec("CREATE TABLE t1 (pk int primary key, color varchar(100), FOREIGN KEY (color) REFERENCES colors(name));")
	primaryDatabase.MustExec("START TRANSACTION;")
	primaryDatabase.MustExec("SET foreign_key_checks = 1;")
	primaryDatabase.MustExec("INSERT INTO colors VALUES ('green'), ('red'), ('blue');")
	primaryDatabase.MustExec("INSERT INTO t1 VALUES (1, 'red'), (2, 'green');")
	primaryDatabase.MustExec("COMMIT;")

	// Test the Insert path with foreign key checks turned off
	primaryDatabase.MustExec("START TRANSACTION;")
	primaryDatabase.MustExec("SET foreign_key_checks = 0;")
	primaryDatabase.MustExec("INSERT INTO t1 VALUES (3, 'not-a-color');")
	primaryDatabase.MustExec("COMMIT;")

	// Test the Update and Delete paths with foreign key checks turned off
	primaryDatabase.MustExec("START TRANSACTION;")
	primaryDatabase.MustExec("DELETE FROM colors WHERE name='red';")
	primaryDatabase.MustExec("UPDATE t1 SET color='still-not-a-color' WHERE pk=2;")
	primaryDatabase.MustExec("COMMIT;")

	// Verify the changes on the replica
	waitForReplicaToCatchUp(t)
	rows, err := replicaDatabase.Queryx("select * from db01.t1 order by pk;")
	require.NoError(t, err)
	row := convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["pk"])
	require.Equal(t, "red", row["color"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "2", row["pk"])
	require.Equal(t, "still-not-a-color", row["color"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "3", row["pk"])
	require.Equal(t, "not-a-color", row["color"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())

	rows, err = replicaDatabase.Queryx("select * from db01.colors order by name;")
	require.NoError(t, err)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "blue", row["name"])
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "green", row["name"])
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
}

// TestCharsetsAndCollations tests that we can successfully replicate data using various charsets and collations.
func TestCharsetsAndCollations(t *testing.T) {
	defer teardown(t)
	startSqlServers(t)
	startReplication(t, mySqlPort)

	// Use non-default charset/collations to create data on the primary
	primaryDatabase.MustExec("CREATE TABLE t1 (pk int primary key, c1 varchar(255) COLLATE ascii_general_ci, c2 varchar(255) COLLATE utf16_general_ci);")
	primaryDatabase.MustExec("insert into t1 values (1, \"one\", \"one\");")

	// Verify on the replica
	waitForReplicaToCatchUp(t)
	rows, err := replicaDatabase.Queryx("show create table db01.t1;")
	require.NoError(t, err)
	row := convertByteArraysToStrings(readNextRow(t, rows))
	require.Contains(t, row["Create Table"], "ascii_general_ci")
	require.Contains(t, row["Create Table"], "utf16_general_ci")
	require.NoError(t, rows.Close())

	rows, err = replicaDatabase.Queryx("select * from db01.t1;")
	require.NoError(t, err)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "one", row["c1"])
	require.Equal(t, "\x00o\x00n\x00e", row["c2"])
	require.NoError(t, rows.Close())

	// Test that we get an error for unsupported charsets/collations
	primaryDatabase.MustExec("CREATE TABLE t2 (pk int primary key, c1 varchar(255) COLLATE utf16_german2_ci);")
	waitForReplicaToCatchUp(t)
	replicaDatabase.MustExec("use db01;")
	rows, err = replicaDatabase.Queryx("SHOW TABLES WHERE Tables_in_db01 like 't2';")
	require.NoError(t, err)
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())

	rows, err = replicaDatabase.Queryx("SHOW REPLICA STATUS;")
	require.NoError(t, err)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "1105", row["Last_SQL_Errno"])
	require.NotEmpty(t, row["Last_SQL_Error_Timestamp"])
	require.Contains(t, row["Last_SQL_Error"], "The collation `utf16_german2_ci` has not yet been implemented")
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
}

//
// Test Helper Functions
//

// waitForReplicaToCatchUp waits (up to 20s) for the replica to catch up with the primary database. The
// lag is measured by checking that gtid_executed is the same on the primary and replica.
func waitForReplicaToCatchUp(t *testing.T) {
	timeLimit := 20 * time.Second
	endTime := time.Now().Add(timeLimit)
	for time.Now().Before(endTime) {
		replicaGtid := queryGtid(t, replicaDatabase)
		primaryGtid := queryGtid(t, primaryDatabase)

		if primaryGtid == replicaGtid {
			return
		} else {
			fmt.Printf("primary and replica not in sync yet... (primary: %s, replica: %s)\n", primaryGtid, replicaGtid)
			time.Sleep(250 * time.Millisecond)
		}
	}

	t.Fatal("primary and replica did not synchronize within " + timeLimit.String())
}

// waitForReplicaToReachGtid waits (up to 10s) for the replica's @@gtid_executed sys var to show that
// it has executed the |target| gtid transaction number.
func waitForReplicaToReachGtid(t *testing.T, target int) {
	timeLimit := 10 * time.Second
	endTime := time.Now().Add(timeLimit)
	for time.Now().Before(endTime) {
		time.Sleep(250 * time.Millisecond)
		replicaGtid := queryGtid(t, replicaDatabase)

		if replicaGtid != "" {
			components := strings.Split(replicaGtid, ":")
			require.Equal(t, 2, len(components))
			sourceGtid := components[1]
			if strings.Contains(sourceGtid, "-") {
				gtidRange := strings.Split(sourceGtid, "-")
				require.Equal(t, 2, len(gtidRange))
				sourceGtid = gtidRange[1]
			}

			i, err := strconv.Atoi(sourceGtid)
			require.NoError(t, err)
			if i >= target {
				return
			}
		}

		fmt.Printf("replica has not reached transaction %d yet; currently at: %s \n", target, replicaGtid)
	}

	t.Fatal("replica did not reach target GTID within " + timeLimit.String())
}

func queryGtid(t *testing.T, database *sqlx.DB) string {
	rows, err := database.Queryx("SELECT @@global.gtid_executed as gtid_executed;")
	require.NoError(t, err)
	defer rows.Close()
	row := convertByteArraysToStrings(readNextRow(t, rows))
	if row["gtid_executed"] == nil {
		t.Fatal("no value for @@GLOBAL.gtid_executed")
	}
	return row["gtid_executed"].(string)
}

func readNextRow(t *testing.T, rows *sqlx.Rows) map[string]interface{} {
	row := make(map[string]interface{})
	require.True(t, rows.Next())
	err := rows.MapScan(row)
	require.NoError(t, err)
	return row
}

func readAllRows(t *testing.T, rows *sqlx.Rows) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	for {
		row := make(map[string]interface{})
		if rows.Next() == false {
			return result
		}
		err := rows.MapScan(row)
		require.NoError(t, err)
		row = convertByteArraysToStrings(row)
		result = append(result, row)
	}
}

func startSqlServers(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping binlog replication integ tests on Windows OS")
	} else if runtime.GOOS == "darwin" && os.Getenv("CI") == "true" {
		t.Skip("Skipping binlog replication integ tests in CI environment on Mac OS")
	}

	testDir = filepath.Join(os.TempDir(), t.Name()+"-"+time.Now().Format("12345"))
	err := os.MkdirAll(testDir, 0777)

	cmd := exec.Command("chmod", "777", testDir)
	_, err = cmd.Output()
	if err != nil {
		panic(err)
	}

	require.NoError(t, err)
	fmt.Printf("temp dir: %v \n", testDir)

	// Start up primary and replica databases
	mySqlPort, mySqlProcess, err = startMySqlServer(testDir)
	require.NoError(t, err)
	doltPort, doltProcess, err = startDoltSqlServer(testDir)
	require.NoError(t, err)
}

func stopDoltSqlServer(t *testing.T) {
	// Use the negative process ID so that we grab the entire process group.
	// This is necessary to kill all the processes the child spawns.
	// Note that we use os.FindProcess, instead of syscall.Kill, since syscall.Kill
	// is not available on windows.
	p, err := os.FindProcess(-doltProcess.Pid)
	require.NoError(t, err)

	err = p.Signal(syscall.SIGKILL)
	require.NoError(t, err)
	time.Sleep(250 * time.Millisecond)
}

func startReplication(_ *testing.T, port int) {
	replicaDatabase.MustExec("SET @@GLOBAL.server_id=123;")
	replicaDatabase.MustExec(
		fmt.Sprintf("change replication source to SOURCE_HOST='localhost', SOURCE_USER='root', "+
			"SOURCE_PASSWORD='', SOURCE_PORT=%v;", port))

	replicaDatabase.MustExec("start replica;")
}

func assertCreateTableStatement(t *testing.T, database *sqlx.DB, table string, expectedStatement string) {
	rows, err := database.Queryx("show create table db01." + table + ";")
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
	err = os.MkdirAll(dir, 0777)
	if err != nil {
		return -1, nil, err
	}
	cmd := exec.Command("chmod", "777", dir)
	output, err := cmd.Output()
	if err != nil {
		panic(err)
	}

	err = os.Chdir(dir)
	if err != nil {
		return -1, nil, err
	}

	mySqlPort = findFreePort()

	// MySQL will NOT start up as the root user, so if we're running as root
	// (e.g. in a CI env), use the "mysql" user instead.
	user, err := user.Current()
	if err != nil {
		panic("unable to determine current user: " + err.Error())
	}
	username := user.Username
	if username == "root" {
		fmt.Printf("overriding current user (root) to run mysql as 'mysql' user instead\n")
		username = "mysql"
	}

	// Create a fresh MySQL server for the primary
	chmodCmd := exec.Command("mysqld",
		"--no-defaults",
		"--user="+username,
		"--initialize-insecure",
		"--datadir="+dataDir,
		"--default-authentication-plugin=mysql_native_password")
	output, err = chmodCmd.CombinedOutput()
	if err != nil {
		return -1, nil, fmt.Errorf("unable to execute command %v: %v – %v", cmd.String(), err.Error(), string(output))
	}

	cmd = exec.Command("mysqld",
		"--no-defaults",
		"--user="+username,
		"--datadir="+dataDir,
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

	mysqlLogFilePath = filepath.Join(dir, fmt.Sprintf("mysql-%d.out.log", time.Now().Unix()))
	mysqlLogFile, err = os.Create(mysqlLogFilePath)
	if err != nil {
		return -1, nil, err
	}
	fmt.Printf("MySQL server logs at: %s \n", mysqlLogFilePath)
	cmd.Stdout = mysqlLogFile
	cmd.Stderr = mysqlLogFile
	err = cmd.Start()
	if err != nil {
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

func initializeDevDoltBuild(dir string, goDirPath string) string {
	// If we're not in a CI environment, don't worry about building a dev build
	if os.Getenv("CI") != "true" {
		return ""
	}

	basedir := filepath.Dir(filepath.Dir(dir))
	fullpath := filepath.Join(basedir, fmt.Sprintf("devDolt-%d", os.Getpid()))

	_, err := os.Stat(fullpath)
	if err == nil {
		return fullpath
	}

	fmt.Printf("building dolt dev build at: %s \n", fullpath)
	cmd := exec.Command("go", "build", "-o", fullpath, "./cmd/dolt")
	cmd.Dir = goDirPath

	output, err := cmd.CombinedOutput()
	if err != nil {
		panic("unable to build dolt for binlog integration tests: " + err.Error() + "\nFull output: " + string(output) + "\n")
	}
	return fullpath
}

func startDoltSqlServer(dir string) (int, *os.Process, error) {
	dir = filepath.Join(dir, "dolt")
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		return -1, nil, err
	}

	doltPort = findFreePort()
	fmt.Printf("Starting Dolt sql-server on port: %d, with data dir %s\n", doltPort, dir)

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

	socketPath := filepath.Join("/tmp", fmt.Sprintf("dolt.%v.sock", doltPort))

	args := []string{"go", "run", "./cmd/dolt",
		"sql-server",
		"-uroot",
		"--loglevel=TRACE",
		fmt.Sprintf("--data-dir=%s", dir),
		fmt.Sprintf("--port=%v", doltPort),
		fmt.Sprintf("--socket=%s", socketPath)}

	// If we're running in CI, use a precompiled dolt binary instead of go run
	devDoltPath := initializeDevDoltBuild(dir, goDirPath)
	if devDoltPath != "" {
		args[2] = devDoltPath
		args = args[2:]
	}
	cmd := exec.Command(args[0], args[1:]...)

	// Set a unique process group ID so that we can cleanly kill this process, as well as
	// any spawned child processes later. Mac/Unix can set the "Setpgid" field directly, but
	// on windows, this field isn't present, so we need to use reflection so that this code
	// can still compile for windows, even though we don't run it there.
	procAttr := &syscall.SysProcAttr{}
	ps := reflect.ValueOf(procAttr)
	s := ps.Elem()
	f := s.FieldByName("Setpgid")
	f.SetBool(true)
	cmd.SysProcAttr = procAttr

	// Some tests restart the Dolt sql-server, so if we have a current log file, save a reference
	// to it so we can print the results later if the test fails.
	if doltLogFilePath != "" {
		oldDoltLogFilePath = doltLogFilePath
	}

	doltLogFilePath = filepath.Join(dir, fmt.Sprintf("dolt-%d.out.log", time.Now().Unix()))
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

	fmt.Printf("Dolt CMD: %s\n", cmd.String())

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
	fmt.Printf("Waiting for server to start...\n")
	for counter := 0; counter < 20; counter++ {
		if database.Ping() == nil {
			return nil
		}
		fmt.Printf("not up yet; waiting...\n")
		time.Sleep(500 * time.Millisecond)
	}

	return database.Ping()
}

// printFile opens the specified filepath |path| and outputs the contents of that file to stdout.
func printFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("Unable to open file: %s \n", err)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		s, err := reader.ReadString(byte('\n'))
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		}
		fmt.Print(s)
	}
	fmt.Println()
}

// assertRepoStateFileExists asserts that the repo_state.json file is present for the specified
// database |db|.
func assertRepoStateFileExists(t *testing.T, db string) {
	repoStateFile := filepath.Join(testDir, "dolt", db, ".dolt", "repo_state.json")

	_, err := os.Stat(repoStateFile)
	require.NoError(t, err)
}
