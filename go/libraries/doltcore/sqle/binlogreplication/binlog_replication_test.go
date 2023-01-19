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

type typeDescription struct {
	//Name         string
	TypeDefinition string
	DefaultValue   interface{}
	MinValue       interface{}
	MinGoValue     interface{} // Optional
	MaxValue       interface{}
	MaxGoValue     interface{} // Optional
}

func (td *typeDescription) ColumnName() string {
	name := "_" + strings.ReplaceAll(td.TypeDefinition, "(", "_")
	name = strings.ReplaceAll(name, ")", "_")
	name = strings.ReplaceAll(name, " ", "_")
	return name
}

var allTypes = []typeDescription{
	{
		TypeDefinition: "bit",
		DefaultValue:   0,
		MinValue:       0,
		MinGoValue:     []uint8{0},
		MaxValue:       1,
	},
	{
		TypeDefinition: "bit(64)",
		DefaultValue:   0,
		MinValue:       "0",
		MinGoValue:     []byte{0, 0, 0, 0, 0, 0, 0, 0},
		MaxValue:       "1",
		MaxGoValue:     []byte{0, 0, 0, 0, 0, 0, 0, 1},
	},
	{
		TypeDefinition: "tinyint",
		DefaultValue:   0,
		MinValue:       "-128",
		MaxValue:       "127",
	},
	{
		TypeDefinition: "tinyint unsigned",
		DefaultValue:   0,
		MinValue:       "0",
		MaxValue:       "255",
	},
	{
		TypeDefinition: "bool",
		DefaultValue:   0,
		MinValue:       "0",
		MaxValue:       "1",
	},
	{
		TypeDefinition: "smallint",
		DefaultValue:   0,
		MinValue:       "-32768",
		MaxValue:       "32767",
	},
	{
		TypeDefinition: "smallint unsigned",
		DefaultValue:   0,
		MinValue:       "0",
		MaxValue:       "65535",
	},
	/*
		    From: https://www.w3resource.com/mysql/mysql-data-types.php

			Type	Length
			in Bytes	Minimum Value
			(Signed)	Maximum Value
			(Signed)	Minimum Value
			(Unsigned)	Maximum Value
			(Unsigned)
			FLOAT	4	-3.402823466E+38	 -1.175494351E-38	 1.175494351E-38 	3.402823466E+38
			DOUBLE	8	-1.7976931348623
			157E+ 308	-2.22507385850720
			14E- 308	0, and
			2.22507385850720
			14E- 308 	1.797693134862315
			7E+ 308
	*/
	{
		TypeDefinition: "float",
		DefaultValue:   0,
		MinValue:       "-0.1234", // Placeholder
		MaxValue:       "0.1234",  // Placeholder
	},
	{
		TypeDefinition: "float unsigned",
		DefaultValue:   0,
		MinValue:       "0",          // Placeholder
		MaxValue:       "0.12345678", // Placeholder
	},
	{
		TypeDefinition: "double",
		DefaultValue:   0,
		MinValue:       "-0.12345678", // Placeholder
		MaxValue:       "0.12345678",  // Placeholder
	},
	{
		TypeDefinition: "double unsigned",
		DefaultValue:   0,
		MinValue:       "0",          // Placeholder
		MaxValue:       "0.12345678", // Placeholder
	},
	{
		TypeDefinition: "date",
		DefaultValue:   0,
		MinValue:       "DATE('1981-02-16')",
		MinGoValue:     "1981-02-16",
		MaxValue:       "DATE('1981-02-16')",
		MaxGoValue:     "1981-02-16",
	},
	// ------------------------
	//{
	//	TypeDefinition: "??",
	//	DefaultValue:   0,
	//	MinValue:       -100,
	//	MaxValue:       100,
	//},
	//{
	//	TypeDefinition: "??",
	//	DefaultValue:   0,
	//	MinValue:       -100,
	//	MaxValue:       100,
	//},
	//{
	//	TypeDefinition: "??",
	//	DefaultValue:   0,
	//	MinValue:       -100,
	//	MaxValue:       100,
	//},
	//{
	//	TypeDefinition: "??",
	//	DefaultValue:   0,
	//	MinValue:       -100,
	//	MaxValue:       100,
	//},
	//{
	//	TypeDefinition: "??",
	//	DefaultValue:   0,
	//	MinValue:       -100,
	//	MaxValue:       100,
	//},
	//{
	//	TypeDefinition: "??",
	//	DefaultValue:   0,
	//	MinValue:       -100,
	//	MaxValue:       100,
	//},
}

// TODO: Break this test out into it's own file since it's going to get large

func generateCreateTableStatement(tableName string) string {
	sb := strings.Builder{}
	sb.WriteString("create table " + tableName)
	sb.WriteString("(pk int primary key auto_increment")
	for _, typeDesc := range allTypes {
		sb.WriteString(fmt.Sprintf(", %s %s",
			typeDesc.ColumnName(), typeDesc.TypeDefinition))
	}
	sb.WriteString(");")
	return sb.String()
}

func generateInsertMaxValuesStatement(tableName string) string {
	sb := strings.Builder{}
	sb.WriteString("insert into " + tableName)
	sb.WriteString(" values (DEFAULT")
	for _, typeDesc := range allTypes {
		sb.WriteString(", " + fmt.Sprintf("%v", typeDesc.MaxValue))
	}
	sb.WriteString(");")

	fmt.Printf("InsertMaxValuesStatement: %s\n", sb.String())
	return sb.String()
}

func generateInsertMinValuesStatement(tableName string) string {
	sb := strings.Builder{}
	sb.WriteString("insert into " + tableName)
	sb.WriteString(" values (DEFAULT")
	for _, typeDesc := range allTypes {
		// TODO: Handle types (e.g. quote strings)
		sb.WriteString(", " + fmt.Sprintf("%v", typeDesc.MinValue))
	}
	sb.WriteString(");")

	fmt.Printf("InsertMinValuesStatement: %s\n", sb.String())
	return sb.String()
}

func generateInsertNullValuesStatement(tableName string) string {
	sb := strings.Builder{}
	sb.WriteString("insert into " + tableName)
	sb.WriteString(" values (DEFAULT")
	for range allTypes {
		sb.WriteString(", null")
	}
	sb.WriteString(");")

	fmt.Printf("InsertNullValuesStatement: %s\n", sb.String())
	return sb.String()
}

func generateUpdateToNullValuesStatement(tableName string, pk int) string {
	sb := strings.Builder{}
	sb.WriteString("update " + tableName + " set ")
	for i, typeDesc := range allTypes {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%s=NULL", typeDesc.ColumnName()))
	}
	sb.WriteString(fmt.Sprintf(" where pk=%d;", pk))

	fmt.Printf("generateUpdateToNullValuesStatement: %s\n", sb.String())
	return sb.String()
}

func generateUpdateToMaxValuesStatement(tableName string, pk int) string {
	sb := strings.Builder{}
	sb.WriteString("update " + tableName + " set ")
	for i, typeDesc := range allTypes {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%s=%v",
			typeDesc.ColumnName(), typeDesc.MaxValue))
	}
	sb.WriteString(fmt.Sprintf(" where pk=%d;", pk))

	fmt.Printf("generateUpdateToMaxValuesStatement: %s\n", sb.String())
	return sb.String()
}

func generateUpdateToMinValuesStatement(tableName string, pk int) string {
	sb := strings.Builder{}
	sb.WriteString("update " + tableName + " set ")
	for i, typeDesc := range allTypes {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%s=%v",
			typeDesc.ColumnName(), typeDesc.MinValue))
	}
	sb.WriteString(fmt.Sprintf(" where pk=%d;", pk))

	fmt.Printf("generateUpdateToMinValuesStatement: %s\n", sb.String())
	return sb.String()
}

func assertMinValues(t *testing.T, row map[string]interface{}) {
	for _, typeDesc := range allTypes {
		expectedValue := typeDesc.MinValue
		if typeDesc.MinGoValue != nil {
			expectedValue = typeDesc.MinGoValue
		}
		require.EqualValues(t, expectedValue, row[typeDesc.ColumnName()],
			"Failed on min value for for column %q", typeDesc.ColumnName())
	}
}

func assertMaxValues(t *testing.T, row map[string]interface{}) {
	for _, typeDesc := range allTypes {
		expectedValue := typeDesc.MaxValue
		if typeDesc.MaxGoValue != nil {
			expectedValue = typeDesc.MaxGoValue
		}
		require.EqualValues(t, expectedValue, row[typeDesc.ColumnName()],
			"Failed on max value for for column %q", typeDesc.ColumnName())
	}
}

func assertNullValues(t *testing.T, row map[string]interface{}) {
	for _, typeDesc := range allTypes {
		require.Nil(t, row[typeDesc.ColumnName()],
			"Failed on NULL value for for column %q", typeDesc.ColumnName())
	}
}

// TestBinlogReplicationForAllTypes tests that operations (inserts, updates, and deletes) on all SQL
// data types can be successfully replicated.
// TODO: This doesn't cover all types; look into generating this data (types, min/max values, etc)
func TestBinlogReplicationForAllTypes(t *testing.T) {
	startSqlServers(t)
	startReplication(t, mySqlPort)
	defer teardown(t)

	tableName := "alltypes"
	createTableStatement := generateCreateTableStatement(tableName)
	primaryDatabase.MustExec(createTableStatement)

	// Make inserts on the primary – min, max, and null values
	primaryDatabase.MustExec(generateInsertMinValuesStatement(tableName))
	primaryDatabase.MustExec(generateInsertMaxValuesStatement(tableName))
	primaryDatabase.MustExec(generateInsertNullValuesStatement(tableName))

	// Verify inserts on replica
	time.Sleep(100 * time.Millisecond)
	rows, err := replicaDatabase.Queryx("select * from alltypes order by pk asc;")
	require.NoError(t, err)
	row := convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["pk"])
	assertMinValues(t, row)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "2", row["pk"])
	assertMaxValues(t, row)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "3", row["pk"])
	assertNullValues(t, row)
	require.False(t, rows.Next())

	// Make updates on the primary
	primaryDatabase.MustExec(generateUpdateToNullValuesStatement(tableName, 1))
	primaryDatabase.MustExec(generateUpdateToMinValuesStatement(tableName, 2))
	primaryDatabase.MustExec(generateUpdateToMaxValuesStatement(tableName, 3))

	// Verify updates on the replica
	time.Sleep(100 * time.Millisecond)
	rows, err = replicaDatabase.Queryx("select * from alltypes order by pk asc;")
	require.NoError(t, err)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["pk"])
	assertNullValues(t, row)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "2", row["pk"])
	assertMinValues(t, row)
	row = convertByteArraysToStrings(readNextRow(t, rows))
	require.Equal(t, "3", row["pk"])
	assertMaxValues(t, row)
	require.False(t, rows.Next())

	// Make deletes on the primary
	primaryDatabase.MustExec("delete from alltypes where pk=1;")
	primaryDatabase.MustExec("delete from alltypes where pk=2;")
	primaryDatabase.MustExec("delete from alltypes where pk=3;")

	// Verify deletes on the replica
	time.Sleep(100 * time.Millisecond)
	rows, err = replicaDatabase.Queryx("select * from alltypes order by pk asc;")
	require.NoError(t, err)
	require.False(t, rows.Next())
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
