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
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBinlogReplicationForAllTypes tests that operations (inserts, updates, and deletes) on all SQL
// data types can be successfully replicated.
func TestBinlogReplicationForAllTypes(t *testing.T) {
	defer teardown(t)
	startSqlServersWithDoltSystemVars(t, doltReplicaSystemVars)
	startReplicationAndCreateTestDb(t, mySqlPort)

	// Set the session's timezone to UTC, to avoid TIMESTAMP test values changing
	// when they are converted to UTC for storage.
	primaryDatabase.MustExec("SET @@time_zone = '+0:00';")

	// Create the test table
	tableName := "alltypes"
	createTableStatement := generateCreateTableStatement(tableName)
	primaryDatabase.MustExec(createTableStatement)

	// Make inserts on the primary – small, large, and null values
	primaryDatabase.MustExec(generateInsertValuesStatement(tableName, 0))
	primaryDatabase.MustExec(generateInsertValuesStatement(tableName, 1))
	primaryDatabase.MustExec(generateInsertNullValuesStatement(tableName))

	// Verify inserts on replica
	waitForReplicaToCatchUp(t)
	rows, err := replicaDatabase.Queryx("select * from db01.alltypes order by pk asc;")
	require.NoError(t, err)
	row := convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["pk"])
	assertValues(t, 0, row)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "2", row["pk"])
	assertValues(t, 1, row)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "3", row["pk"])
	assertNullValues(t, row)
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())

	// Make updates on the primary
	primaryDatabase.MustExec(generateUpdateToNullValuesStatement(tableName, 1))
	primaryDatabase.MustExec(generateUpdateValuesStatement(tableName, 2, 0))
	primaryDatabase.MustExec(generateUpdateValuesStatement(tableName, 3, 1))

	// Verify updates on the replica
	waitForReplicaToCatchUp(t)
	replicaDatabase.MustExec("use db01;")
	rows, err = replicaDatabase.Queryx("select * from db01.alltypes order by pk asc;")
	require.NoError(t, err)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "1", row["pk"])
	assertNullValues(t, row)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "2", row["pk"])
	assertValues(t, 0, row)
	row = convertMapScanResultToStrings(readNextRow(t, rows))
	require.Equal(t, "3", row["pk"])
	assertValues(t, 1, row)
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())

	// Make deletes on the primary
	primaryDatabase.MustExec("delete from alltypes where pk=1;")
	primaryDatabase.MustExec("delete from alltypes where pk=2;")
	primaryDatabase.MustExec("delete from alltypes where pk=3;")

	// Verify deletes on the replica
	waitForReplicaToCatchUp(t)
	rows, err = replicaDatabase.Queryx("select * from db01.alltypes order by pk asc;")
	require.NoError(t, err)
	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
}

// ---------------------
// Test Data
// ---------------------

type typeDescriptionAssertion struct {
	Value         interface{}
	ExpectedValue interface{}
}

func newTypeDescriptionAssertion(v interface{}) typeDescriptionAssertion {
	return typeDescriptionAssertion{Value: v}
}

func newTypeDescriptionAssertionWithExpectedValue(v interface{}, x interface{}) typeDescriptionAssertion {
	return typeDescriptionAssertion{Value: v, ExpectedValue: x}
}

func (tda *typeDescriptionAssertion) getExpectedValue() interface{} {
	if tda.ExpectedValue != nil {
		return tda.ExpectedValue
	}

	if valueString, isString := tda.Value.(string); isString {
		removedPrefixes := []string{"DATE", "TIMESTAMP", "TIME"}
		lowercaseValue := strings.ToUpper(valueString)
		for _, prefix := range removedPrefixes {
			if strings.HasPrefix(lowercaseValue, prefix) {
				return valueString[len(prefix)+2 : len(valueString)-2]
			}
		}
	}

	return tda.Value
}

type typeDescription struct {
	TypeDefinition string
	Assertions     [2]typeDescriptionAssertion
}

func (td *typeDescription) ColumnName() string {
	name := "_" + strings.ReplaceAll(td.TypeDefinition, "(", "_")
	name = strings.ReplaceAll(name, ")", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, ",", "_")
	name = strings.ReplaceAll(name, "\"", "")
	name = strings.ReplaceAll(name, "'", "")
	return name
}

func (td *typeDescription) IsStringType() bool {
	def := strings.ToLower(td.TypeDefinition)
	switch {
	case strings.Contains(def, "char"),
		strings.Contains(def, "binary"),
		strings.Contains(def, "blob"),
		strings.Contains(def, "text"),
		strings.Contains(def, "enum"),
		strings.Contains(def, "set"),
		strings.Contains(def, "json"):
		return true
	default:
		return false
	}
}

// allTypes contains test data covering all SQL types.
//
// TODO: TypeWireTests contains most of the test data we need. I found it after implementing this, but we
// could simplify this test code by converting to use TypeWireTests and enhancing it with the additional
// test cases we need to cover (e.g. NULL values).
var allTypes = []typeDescription{
	// Bit types
	{
		TypeDefinition: "bit",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertionWithExpectedValue("0", []uint8{0}),
			newTypeDescriptionAssertionWithExpectedValue("1", []uint8{1}),
		},
	},
	{
		TypeDefinition: "bit(64)",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertionWithExpectedValue("0", []byte{0, 0, 0, 0, 0, 0, 0, 0}),
			newTypeDescriptionAssertionWithExpectedValue("1", []byte{0, 0, 0, 0, 0, 0, 0, 1}),
		},
	},

	// Integer types
	{
		TypeDefinition: "tinyint",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("-128"),
			newTypeDescriptionAssertion("127"),
		},
	},
	{
		TypeDefinition: "tinyint unsigned",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion("255"),
		},
	},
	{
		TypeDefinition: "bool",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion("1"),
		},
	},
	{
		TypeDefinition: "smallint",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("-32768"),
			newTypeDescriptionAssertion("32767"),
		},
	},
	{
		TypeDefinition: "smallint unsigned",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion("65535"),
		},
	},
	{
		TypeDefinition: "mediumint",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("-32768"),
			newTypeDescriptionAssertion("32767"),
		},
	},
	{
		TypeDefinition: "mediumint unsigned",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion("65535"),
		},
	},
	{
		TypeDefinition: "int",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("-32768"),
			newTypeDescriptionAssertion("32767"),
		},
	},
	{
		TypeDefinition: "int unsigned",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion("65535"),
		},
	},
	{
		TypeDefinition: "bigint",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("-32768"),
			newTypeDescriptionAssertion("32767"),
		},
	},
	{
		TypeDefinition: "bigint unsigned",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion("65535"),
		},
	},
	{
		TypeDefinition: "decimal",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion("1234567890"),
		},
	},
	{
		TypeDefinition: "decimal(10,2)",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0.00"),
			newTypeDescriptionAssertion("12345678.00"),
		},
	},
	{
		TypeDefinition: "decimal(20,8)",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("-1234567890.12345678"),
			newTypeDescriptionAssertion("999999999999.00000001"),
		},
	},

	// Floating point types
	{
		TypeDefinition: "float",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("-3.40282e+38"),
			newTypeDescriptionAssertion("-1.17549e-38"),
		},
	},
	{
		TypeDefinition: "float unsigned",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("1.17549e-38"),
			newTypeDescriptionAssertion("3.40282e+38"),
		},
	},
	{
		TypeDefinition: "double",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("-1.7976931348623157e+308"),
			newTypeDescriptionAssertion("-2.2250738585072014e-308"),
		},
	},
	{
		TypeDefinition: "double unsigned",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("2.2250738585072014e-308"),
			newTypeDescriptionAssertion("1.7976931348623157e+308"),
		},
	},

	// String types
	{
		TypeDefinition: "char(1)",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion(""),
			newTypeDescriptionAssertion("0"),
		},
	},
	{
		TypeDefinition: "char(10)",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion(""),
			newTypeDescriptionAssertion("0123456789"),
		},
	},
	{
		TypeDefinition: "varchar(255)",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion(""),
			newTypeDescriptionAssertion(generateTestDataString(255)),
		},
	},
	{
		TypeDefinition: "char(1) binary",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion("1"),
		},
	},
	{
		TypeDefinition: "binary(1)",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion("1"),
		},
	},
	{
		TypeDefinition: "binary(255)",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion(generateTestDataString(255)),
			newTypeDescriptionAssertion(generateTestDataString(255)),
		},
	},
	{
		TypeDefinition: "varbinary(1)",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion("1"),
		},
	},
	{
		TypeDefinition: "varbinary(255)",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion(generateTestDataString(0)),
			newTypeDescriptionAssertion(generateTestDataString(255)),
		},
	},

	// Blob/Text types
	{
		TypeDefinition: "tinyblob",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion(generateTestDataString(255)),
		},
	},
	{
		TypeDefinition: "blob",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion(generateTestDataString(10_000)),
		},
	},
	{
		TypeDefinition: "mediumblob",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion(generateTestDataString(15_000)),
		},
	},
	{
		TypeDefinition: "longblob",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion(generateTestDataString(20_000)),
		},
	},
	{
		TypeDefinition: "tinytext",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion(generateTestDataString(255)),
		},
	},
	{
		TypeDefinition: "text",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion(generateTestDataString(10_000)),
		},
	},
	{
		TypeDefinition: "mediumtext",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion(generateTestDataString(15_000)),
		},
	},
	{
		TypeDefinition: "longtext",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("0"),
			newTypeDescriptionAssertion(generateTestDataString(20_000)),
		},
	},

	// Enum and Set types
	{
		TypeDefinition: "ENUM(\"\",\"a\",\"b\",\"c\")",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion(""),
			newTypeDescriptionAssertion("c"),
		},
	},
	{
		TypeDefinition: "SET(\"a\",\"b\",\"c\")",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("a"),
			newTypeDescriptionAssertion("a,b,c"),
		},
	},

	// Date types
	{
		TypeDefinition: "date",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("DATE('1981-02-16')"),
			newTypeDescriptionAssertion("DATE('1981-02-16')"),
		},
	},
	{
		TypeDefinition: "time",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("TIME('01:02:03')"),
			newTypeDescriptionAssertion("TIME('01:02:03')"),
		},
	},
	{
		TypeDefinition: "datetime",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("TIMESTAMP('1981-02-16 12:13:14')"),
			newTypeDescriptionAssertion("TIMESTAMP('1981-02-16 12:13:14')"),
		},
	},
	{
		TypeDefinition: "timestamp",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("TIMESTAMP('1981-02-16 12:13:14')"),
			newTypeDescriptionAssertion("TIMESTAMP('1981-02-16 12:13:14')"),
		},
	},
	{
		TypeDefinition: "year",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("1981"),
			newTypeDescriptionAssertion("2020"),
		},
	},

	// Spatial types
	{
		TypeDefinition: "geometry",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertionWithExpectedValue("POINT(18, 23)",
				"\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x002@\x00\x00\x00\x00\x00\x007@"),
			newTypeDescriptionAssertionWithExpectedValue("LINESTRING(POINT(0,0),POINT(1,2),POINT(2,4))",
				"\x00\x00\x00\x00\x01\x02\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"+
					"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00"+
					"\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x10@"),
		},
	},

	// JSON types
	{
		TypeDefinition: "json",
		Assertions: [2]typeDescriptionAssertion{
			newTypeDescriptionAssertion("{}"),
			newTypeDescriptionAssertion("{\"os\":\"Mac\",\"name\":\"BillyBob\",\"resolution\":{\"x\":1920,\"y\":1080}}"),
		},
	},
}

// ---------------------
// Test Helper Functions
// ---------------------

func assertValues(t *testing.T, assertionIndex int, row map[string]interface{}) {
	for _, typeDesc := range allTypes {
		assertion := typeDesc.Assertions[assertionIndex]
		expectedValue := assertion.getExpectedValue()

		actualValue := ""
		if row[typeDesc.ColumnName()] != nil {
			actualValue = fmt.Sprintf("%v", row[typeDesc.ColumnName()])
		}
		if typeDesc.TypeDefinition == "json" {
			// LD_1 and DOLT storage formats return JSON strings slightly differently; DOLT removes spaces
			// while LD_1 add whitespace, so for json comparison, we sanitize by removing whitespace.
			var actual interface{}
			json.Unmarshal([]byte(actualValue), &actual)
			var expected interface{}
			json.Unmarshal([]byte(expectedValue.(string)), &expected)
			require.EqualValues(t, expected, actual,
				"Failed on assertion %d for for column %q", assertionIndex, typeDesc.ColumnName())
		} else {
			require.EqualValues(t, expectedValue, actualValue,
				"Failed on assertion %d for for column %q", assertionIndex, typeDesc.ColumnName())
		}
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

func generateInsertValuesStatement(tableName string, assertionIndex int) string {
	sb := strings.Builder{}
	sb.WriteString("insert into " + tableName)
	sb.WriteString(" values (DEFAULT")
	for _, typeDesc := range allTypes {
		assertion := typeDesc.Assertions[assertionIndex]
		value := assertion.Value
		if typeDesc.IsStringType() {
			value = fmt.Sprintf("'%s'", value)
		}
		sb.WriteString(", " + fmt.Sprintf("%v", value))
	}
	sb.WriteString(");")

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

	return sb.String()
}

func generateUpdateValuesStatement(tableName string, pk int, assertionIndex int) string {
	sb := strings.Builder{}
	sb.WriteString("update " + tableName + " set ")
	for i, typeDesc := range allTypes {
		if i > 0 {
			sb.WriteString(", ")
		}
		assertion := typeDesc.Assertions[assertionIndex]
		value := assertion.Value
		if typeDesc.IsStringType() {
			value = fmt.Sprintf("'%s'", value)
		}
		sb.WriteString(fmt.Sprintf("%s=%v", typeDesc.ColumnName(), value))
	}
	sb.WriteString(fmt.Sprintf(" where pk=%d;", pk))

	return sb.String()
}

func generateTestDataString(length uint) string {
	sb := strings.Builder{}
	for ; length > 0; length-- {
		sb.WriteRune(rune(rand.Intn(90-48) + 48))
	}

	return sb.String()
}
