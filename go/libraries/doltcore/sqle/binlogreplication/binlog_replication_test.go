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

// TODO: Move this to a more appropriate package namespace
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

func teardown(t *testing.T) {
	if mySqlProcess != nil {
		mySqlProcess.Kill()
	}
	if doltProcess != nil {
		doltProcess.Kill()
	}
	// TODO: clean up temp files
	//defer os.RemoveAll(dir)
}

func comparePrimaryAndReplicaTable(t *testing.T, table string) {
	// TODO: Finish this experiment
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

func TestBinlogReplicationForAllTypes(t *testing.T) {
	createServersAndStartReplication(t)
	defer teardown(t)

	// Test inserts on the primary
	_, err := primaryDatabase.Query("create table alltypes(pk int primary key auto_increment, n_bit bit, n_bit_64 bit(64), n_tinyint tinyint, n_utinyint tinyint unsigned, n_bool bool, n_smallint smallint, n_usmallint smallint unsigned, n_float float, n_double double, n_ufloat float unsigned, n_udouble double unsigned, d_date date);")
	require.NoError(t, err)
	// Insert minimum values
	primaryDatabase.MustExec("insert into alltypes value (DEFAULT, 0, 1, -128, 0, 0, 0 ,0, -0.1234, -0.12345678, 0.0, 0.0, DATE('1981-02-16'));")
	// Insert maximum values
	primaryDatabase.MustExec("insert into alltypes value (DEFAULT, 0, 1, 127, 255, 0, 0 ,0, 0.0, 0.0, 0.1234, 0.12345678, DATE('1981-02-16'));")
	// Insert null values
	primaryDatabase.MustExec("insert into alltypes value (DEFAULT, null, null, null, null, null, null, null, null, null, null, null, null);")

	// TODO: This causes the dolt server replication process to crash!
	//       What's the best way to debug this?

	// Verify on replica
	time.Sleep(1 * time.Second) // TODO: Could get rid of this manually maintained lag by using toxiproxy
	rows, err := replicaDatabase.Queryx("select * from alltypes;")
	require.NoError(t, err)

	// Test min values
	myMap := make(map[string]interface{})
	require.True(t, rows.Next())
	err = rows.MapScan(myMap)
	require.NoError(t, err)
	require.Equal(t, "1", string(myMap["pk"].([]byte)))
	// MapScan doesn't honor the correct type – it converts everything to a []byte string :-/
	// https://github.com/jmoiron/sqlx/issues/225
	require.EqualValues(t, "\x00", string(myMap["n_bit"].([]byte)))
	require.EqualValues(t, "\x00\x00\x00\x00\x00\x00\x00\x01", string(myMap["n_bit_64"].([]byte)))
	require.EqualValues(t, "-0.1234", string(myMap["n_float"].([]byte)))
	require.EqualValues(t, "-0.12345678", string(myMap["n_double"].([]byte)))
	require.EqualValues(t, "0", string(myMap["n_ufloat"].([]byte)))
	require.EqualValues(t, "0", string(myMap["n_udouble"].([]byte)))
	require.EqualValues(t, "0", string(myMap["n_usmallint"].([]byte)))
	require.EqualValues(t, "1981-02-16", string(myMap["d_date"].([]byte)))

	// Test max values
	require.True(t, rows.Next())
	err = rows.MapScan(myMap)
	require.NoError(t, err)
	require.Equal(t, "2", string(myMap["pk"].([]byte)))
	require.Equal(t, "127", string(myMap["n_tinyint"].([]byte)))
	require.EqualValues(t, "0", string(myMap["n_float"].([]byte)))
	require.EqualValues(t, "0", string(myMap["n_double"].([]byte)))
	require.EqualValues(t, "0.1234", string(myMap["n_ufloat"].([]byte)))
	require.EqualValues(t, "0.12345678", string(myMap["n_udouble"].([]byte)))
	require.EqualValues(t, "255", string(myMap["n_utinyint"].([]byte)))

	// Test null values
	require.True(t, rows.Next())
	err = rows.MapScan(myMap)
	require.NoError(t, err)
	require.Equal(t, "3", string(myMap["pk"].([]byte)))
	require.Equal(t, nil, myMap["n_bit"])
	require.Equal(t, nil, myMap["n_tinyint"])
	require.EqualValues(t, nil, myMap["n_usmallint"])
	require.Equal(t, nil, myMap["d_date"])

	require.False(t, rows.Next())

	// TODO: It might be easier to have data files for the various queries we want to run
	//       Then the commands to execute them and verify them on the replica could be done automatically?
	//       Or maybe we could dump the mysql db and load it into a dolt db to diff the two databases?

	// TODO: Test updates on the primary
	// TODO: Verify on the replica

	// TODO: Test deletes on the primary
	// TODO: Verify on the replica
}

func TestBinlogReplicationSanityCheck(t *testing.T) {
	createServersAndStartReplication(t)
	defer teardown(t)

	// Make changes on the primary
	_, err := primaryDatabase.Query("create table t (pk int primary key)")
	require.NoError(t, err)

	// Verify on replica
	time.Sleep(1 * time.Second)
	expectedStatement := "CREATE TABLE t ( pk int NOT NULL, PRIMARY KEY (pk)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"
	assertCreateTableStatement(t, replicaDatabase, "t", expectedStatement)
}

func createServersAndStartReplication(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestBinlogReplicationForAllTypes-"+time.Now().Format("12345"))
	require.NoError(t, err)
	fmt.Printf("temp dir: %v \n", dir)

	// Start up primary and replica databases
	mySqlPort, mySqlProcess, err = startMySqlServer(dir)
	require.NoError(t, err)
	fmt.Printf("MySQL server started on port %v \n", mySqlPort)
	doltPort, doltProcess, err = startDoltSqlServer(dir)
	require.NoError(t, err)
	fmt.Printf("Dolt server started on port %v \n", doltPort)

	// Configure and start replication
	_, err = replicaDatabase.Query(
		fmt.Sprintf("change replication source to SOURCE_HOST='localhost', SOURCE_USER='root', "+
			"SOURCE_PASSWORD='', SOURCE_PORT=%v;", mySqlPort))
	require.NoError(t, err)
	_, err = replicaDatabase.Query("start replica;")
	require.NoError(t, err)
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

func findFreePort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return -1, fmt.Errorf("unable to find available TCP port: %v", err.Error())
	}
	mySqlPort := listener.Addr().(*net.TCPAddr).Port
	err = listener.Close()
	if err != nil {
		return -1, fmt.Errorf("unable to find available TCP port: %v", err.Error())
	}

	return mySqlPort, nil
}

func startMySqlServer(dir string) (int, *os.Process, error) {
	dir = dir + string(os.PathSeparator) + "mysql" + string(os.PathSeparator)
	dataDir := dir + "mysql_data"
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		return -1, nil, err
	}
	err = os.Chdir(dir)
	if err != nil {
		return -1, nil, err
	}

	mySqlPort, err := findFreePort()
	if err != nil {
		return -1, nil, err
	}

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

	return mySqlPort, cmd.Process, nil
}

func startDoltSqlServer(dir string) (int, *os.Process, error) {
	// TODO: use path/filepath module? https://gobyexample.com/file-paths
	dir = dir + string(os.PathSeparator) + "dolt" + string(os.PathSeparator)
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		return -1, nil, err
	}
	err = os.Chdir(dir)
	if err != nil {
		return -1, nil, err
	}

	doltPort, err := findFreePort()
	if err != nil {
		return -1, nil, err
	}

	// TODO: make sure we are using the local dev build binary!
	// TODO: That means we have to run the build before we can execute these tests?
	//       This is also annoying for local development.
	// TODO: How do the cluster replication tests deal with this? Do they just use the go API directly to launch the command?
	cmd := exec.Command("/Users/jason/go/bin/dolt", "sql-server",
		"-uroot",
		fmt.Sprintf("--port=%v", doltPort),
		fmt.Sprintf("--socket=dolt.%v.sock", doltPort))

	doltLogFilePath := filepath.Join(dir, "dolt.out.log")
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

	// Create the initial database on the Dolt server
	_, err = replicaDatabase.Query("create database db01;")
	if err != nil {
		return -1, nil, err
	}
	_, err = replicaDatabase.Query("use db01;")
	if err != nil {
		return -1, nil, err
	}

	dsn = fmt.Sprintf("root@tcp(127.0.0.1:%v)/db01", doltPort)
	replicaDatabase = sqlx.MustOpen("mysql", dsn)

	return doltPort, cmd.Process, nil
}

func waitForSqlServerToStart(database *sqlx.DB) error {
	for counter := 0; counter < 10; counter++ {
		if database.Ping() == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return database.Ping()
}
