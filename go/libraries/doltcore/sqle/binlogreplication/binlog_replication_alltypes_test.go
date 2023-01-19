// Copyright 2023 Dolthub, Inc.
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
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

// TestBinlogReplicationForAllTypes tests that operations (inserts, updates, and deletes) on all SQL
// data types can be successfully replicated.
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

// ---------------------
// Test Data
// ---------------------

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

// ---------------------
// Test Helper Functions
// ---------------------

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
