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
	"strings"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	csvExt  = ".csv"
	jsonExt = ".json"
	sqlExt  = ".sql"

	increment = GenType("increment")
	random    = GenType("random")
	supplied  = GenType("supplied")
)

var supportedFormats = []string{csvExt, jsonExt, sqlExt}

// GenType specifies how to generate subsequent row values for a given SeedColumn, for a test dataset
type GenType string

// SeedSchema contains the schema to be used to generate a test Dataset
type SeedSchema struct {
	// Rows is size of the Dataset
	Rows int

	// Columns are the schema for the columns to be used for the Dataset
	Columns []*SeedColumn

	// FileFormatExt is the file format extension that directs how to construct the Dataset
	// as a string or as bytes
	FileFormatExt string
}

// NewSeedSchema creates a new SeedSchema
func NewSeedSchema(rows int, cols []*SeedColumn, format string) *SeedSchema {
	for _, frmt := range supportedFormats {
		if format == frmt {
			return &SeedSchema{
				Rows:          rows,
				Columns:       cols,
				FileFormatExt: format,
			}
		}
	}
	log.Fatalf("cannot build seed schema with unsupported file format %s \n", format)
	return &SeedSchema{}
}

// Bytes returns a byte slice formatted according to the SeedSchema'a FileFormatExt
func (sch *SeedSchema) Bytes() []byte {
	switch sch.FileFormatExt {
	case jsonExt:
		return getColSchemaJSON(sch.Columns)
	default:
		log.Fatalf("cannot create bytes from schema, unsupported format %s \n", sch.FileFormatExt)
	}
	return []byte{}
}

// SeedColumn is used to create a column in a test dataset for benchmark testing
type SeedColumn struct {
	Name       string
	PrimaryKey bool
	Type       types.NomsKind
	GenType    GenType
}

// NewSeedColumn creates a new SeedColumn
func NewSeedColumn(name string, pk bool, t types.NomsKind, g GenType) *SeedColumn {
	if isValidGenType(t, g) {
		return &SeedColumn{
			Name:       name,
			PrimaryKey: pk,
			Type:       t,
			GenType:    g,
		}
	}
	log.Fatalf("cannot use gen type %s with noms type %s \n", g, t.String())
	return &SeedColumn{}
}

func isValidGenType(t types.NomsKind, g GenType) bool {
	var validTypes []types.NomsKind
	switch g {
	case increment:
		validTypes = []types.NomsKind{types.IntKind}
	case random:
		validTypes = []types.NomsKind{types.IntKind, types.StringKind}
	case supplied:
		validTypes = []types.NomsKind{
			types.IntKind,
			types.StringKind,
			types.TimestampKind,
		}
	default:
		log.Fatalf("unsupported gen type %s \n", g)
	}
	for _, v := range validTypes {
		if t == v {
			return true
		}
	}
	return false
}

func getColSchemaJSON(seedCols []*SeedColumn) []byte {
	prefix := "{\"Columns\":["
	suffix := "]}"

	statement := make([]string, 0)
	statement = append(statement, prefix)

	schemaStr := "{\"tag\": %d,\"name\":\"%s\",\"kind\":\"%s\",\"is_part_of_pk\":%v,\"col_constraints\":%s}"
	jsonCols := make([]string, 0)

	for i, sc := range seedCols {
		var pks []string
		if sc.PrimaryKey {
			pks = []string{"{\"constraint_type\": \"not_null\",\"params\": null}"}
		} else {
			pks = []string{}
		}
		jc := fmt.Sprintf(schemaStr, uint64(i), sc.Name, strings.ToLower(sc.Type.String()), sc.PrimaryKey, pks)
		jsonCols = append(jsonCols, jc)
	}

	statement = append(statement, strings.Join(jsonCols, ","))
	statement = append(statement, suffix)
	return []byte(strings.Join(statement, ""))
}

func genSampleCols() []*SeedColumn {
	return []*SeedColumn{
		NewSeedColumn("id", true, types.IntKind, increment),
		NewSeedColumn("int1", false, types.IntKind, random),
		NewSeedColumn("int2", false, types.IntKind, increment),
		NewSeedColumn("int3", false, types.IntKind, random),
		NewSeedColumn("int4", false, types.IntKind, increment),
		NewSeedColumn("int5", false, types.IntKind, increment),
		NewSeedColumn("str1", false, types.StringKind, random),
		NewSeedColumn("str2", false, types.StringKind, random),
		NewSeedColumn("str3", false, types.StringKind, random),
		NewSeedColumn("str4", false, types.StringKind, random),
		NewSeedColumn("str5", false, types.StringKind, random),
	}
}
