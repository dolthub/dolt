// Copyright 2019 Dolthub, Inc.
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

package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"

	"github.com/dolthub/dolt/go/store/types"
)

type seedFunc func(col *SeedColumn, format string) string

// Container is used to correctly format strings enclosed in brackets
type Container struct {
	c []string
}

// NewContainer creates a new Container
func NewContainer(format string) *Container {
	c := make([]string, 3)
	switch format {
	case sqlExt:
		c[0] = "("
		c[2] = ")"
	case jsonExt:
		c[0] = "{"
		c[2] = "}"
	default:
		log.Fatalf("cannot create new container, unsupported format %s \n", format)
	}
	return &Container{c: c}
}

// InsertPayload returns the Container with the payload inserted, separated by the separator
func (sc *Container) InsertPayload(payload []string, separator string) string {
	sc.c[1] = strings.Join(payload, separator)
	return strings.Join(sc.c, "")
}

func getColValue(row []string, colIndex int, col *SeedColumn, sf seedFunc, format string) string {
	switch col.GenType {
	case increment:
		return genNomsTypeValueIncrement(row, colIndex, col, format)
	case random:
		return getNomsTypeValueRandom(col, sf, format)
	default:
		log.Fatalf("cannot get column value, unsupported gen type %s \n", col.GenType)
	}
	return ""
}

func genNomsTypeValueIncrement(row []string, colIndex int, col *SeedColumn, format string) string {
	switch col.Type {
	case types.IntKind:
		if len(row) > 0 {
			old, err := strconv.Atoi(row[colIndex])
			if err != nil {
				log.Fatalf(err.Error())
			}
			return fmt.Sprintf("%d", old+1)
		}
		return "1"
	default:
		log.Fatalf("cannot generate incremental value, unsupported noms type %s \n", col.Type.String())
	}
	return ""
}

func getNomsTypeValueRandom(col *SeedColumn, sf seedFunc, format string) string {
	return sf(col, format)
}

// seedRandom is a seedFunc that returns variably random strings for each supported
// nomsKind type
func seedRandom(col *SeedColumn, format string) string {
	switch col.Type {
	case types.IntKind:
		return fmt.Sprintf("%d", rand.Intn(1000))
	case types.StringKind:
		return getRandomString(format)
	default:
		log.Fatalf("cannot generate random value, unsupported noms type %s \n", col.Type.String())
	}
	return ""
}

func getRandomString(format string) string {
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, rand.Intn(255))
	for i := range b {
		b[i] = letters[rand.Int63()%int64(len(letters))]
	}

	switch format {
	case sqlExt:
		return fmt.Sprintf(`"%s"`, b)
	default:
		return string(b)
	}
}

func getJSONRow(strs []string, cols []*SeedColumn) string {
	if len(strs) != len(cols) {
		log.Fatalf("values should be the length of columns. values: %+v, columns: %+v \n", strs, cols)
	}

	payload := make([]string, 0)
	for i, col := range cols {
		load := fmt.Sprintf("\"%s\":\"%s\"", col.Name, strs[i])
		payload = append(payload, load)
	}

	container := NewContainer(jsonExt)
	return container.InsertPayload(payload, ",")
}

func getSQLRow(strs []string, cols []*SeedColumn, tableName string) string {
	container := NewContainer(sqlExt)
	sqlCols := make([]string, 0)

	for _, col := range cols {
		sqlCols = append(sqlCols, fmt.Sprintf("`%s`", col.Name))
	}

	fieldNames := container.InsertPayload(sqlCols, ",")
	values := container.InsertPayload(strs, ",")

	return fmt.Sprintf("INSERT INTO `%s` %s VALUES %s;", tableName, fieldNames, values)
}

func getSQLHeader(cols []*SeedColumn, tableName, format string) string {
	statement := make([]string, 0)
	statement = append(statement, fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", tableName))
	statement = append(statement, fmt.Sprintf("CREATE TABLE `%s` ", tableName))

	container := NewContainer(format)
	schema := make([]string, 0)
	pkDefs := make([]string, 0)
	for i, col := range cols {
		colStr := "`%s` %s"

		// handle pk
		if col.PrimaryKey {
			pkDefs = append(pkDefs, fmt.Sprintf("PRIMARY KEY (`%s`)", col.Name))
			colStr = "`%s` %s NOT NULL"
		}

		// handle increments
		if col.GenType == increment {
			colStr = fmt.Sprintf("%s AUTO_INCREMENT", colStr)
		}

		// append tag
		colStr = fmt.Sprintf("%s COMMENT 'tag:%d'", colStr, i)

		// translate noms type
		sqlType := typeinfo.FromKind(col.Type).ToSqlType().String()

		schema = append(schema, fmt.Sprintf(colStr, col.Name, strings.ToUpper(sqlType)))
	}

	// add pk definitions to create table statement
	for _, pkDef := range pkDefs {
		schema = append(schema, pkDef)
	}

	// create and close create table statement
	schemaStatement := container.InsertPayload(schema, ",\n")
	statement = append(statement, schemaStatement+"; \n")

	return strings.Join(statement, "")
}
