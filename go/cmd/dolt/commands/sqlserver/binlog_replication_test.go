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

package sqlserver

import (
	dbsql "database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TODO: Will we need to run this in a Docker container? Will we have the mysqld binary around?

var mySqlPort, doltPort int
var primaryDatabase, replicaDatabase *dbsql.DB
var mySqlProcess, doltProcess *os.Process

func teardown(t *testing.T) {
	if mySqlProcess != nil {
		mySqlProcess.Kill()
	}
	if doltProcess != nil {
		doltProcess.Kill()
	}
}

func TestBinlogReplicationForAllTypes(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestBinlogReplicationForAllTypes-"+time.Now().Format("12345"))
	require.NoError(t, err)
	fmt.Printf("temp dir: %v \n", dir)

	defer teardown(t)
	//defer os.RemoveAll(dir)

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

	// Make changes on the primary
	_, err = primaryDatabase.Query("create table t (pk int primary key)")
	require.NoError(t, err)

	// Verify on replica
	time.Sleep(1 * time.Second)
	rows, err := replicaDatabase.Query("show create table t;")
	require.NoError(t, err)
	var tableName, createTableStatement string
	require.True(t, rows.Next())
	err = rows.Scan(&tableName, &createTableStatement)
	require.NoError(t, err)
	require.Equal(t, "t", tableName)
	require.NotNil(t, createTableStatement)
	createTableStatement = sanitizeCreateTableString(createTableStatement)

	expectedStatement := "CREATE TABLE t ( pk int NOT NULL, PRIMARY KEY (pk)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"
	require.Equal(t, expectedStatement, createTableStatement)
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
	primaryDatabase, err = dbsql.Open("mysql", dsn)
	if err != nil {
		return -1, nil, err
	}

	err = waitForSqlServerToStart(primaryDatabase)
	if err != nil {
		return -1, nil, err
	}

	// Create the initial database on the MySQL server
	_, err = primaryDatabase.Query("create database db01;")
	if err != nil {
		return -1, nil, err
	}

	dsn = fmt.Sprintf("root@tcp(127.0.0.1:%v)/db01", mySqlPort)
	primaryDatabase, err = dbsql.Open("mysql", dsn)
	if err != nil {
		return -1, nil, err
	}

	return mySqlPort, cmd.Process, nil
}

func startDoltSqlServer(dir string) (int, *os.Process, error) {
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
	// TODO: Do the cluster replication tests deal with this?
	cmd := exec.Command("/Users/jason/go/bin/dolt", "sql-server",
		"-uroot",
		fmt.Sprintf("--port=%v", doltPort),
		fmt.Sprintf("--socket=dolt.%v.sock", doltPort))
	err = cmd.Start()
	if err != nil {
		return -1, nil, fmt.Errorf("unable to execute command %v: %v", cmd.String(), err.Error())
	}

	dsn := fmt.Sprintf("root@tcp(127.0.0.1:%v)/", doltPort)
	replicaDatabase, err = dbsql.Open("mysql", dsn)

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
	replicaDatabase, err = dbsql.Open("mysql", dsn)
	if err != nil {
		return -1, nil, err
	}

	return doltPort, cmd.Process, nil
}

func waitForSqlServerToStart(database *dbsql.DB) error {
	for counter := 0; counter < 10; counter++ {
		if database.Ping() == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return database.Ping()
}
