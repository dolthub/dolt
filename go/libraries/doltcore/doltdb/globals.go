// Copyright 2025 Dolthub, Inc.
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

package doltdb

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	"io"
)

// GetGlobalTablesRef is a function that reads the "ref" column from dolt_global_tables. This is used to handle the Doltgres extended string type.
var GetGlobalTablesRef = getGlobalTablesRef

// GetGlobalTablesNameColumn is a function that reads the "table_name" column from dolt_global_tables. This is used to handle the Doltgres extended string type.
var GetGlobalTablesNameColumn = getGlobalTablesNameColumn

func getGlobalTablesNameColumn(_ context.Context, keyDesc val.TupleDesc, keyTuple val.Tuple) (string, error) {
	key, ok := keyDesc.GetString(0, keyTuple)
	if !ok {
		return "", fmt.Errorf("failed to read global table")
	}
	return key, nil
}

type GlobalTablesEntry struct {
	Ref          string
	NewTableName string
	Options      string
}

func getGlobalTablesRef(_ context.Context, valDesc val.TupleDesc, valTuple val.Tuple) (result GlobalTablesEntry) {
	result.Ref, _ = valDesc.GetString(0, valTuple)
	result.NewTableName, _ = valDesc.GetString(1, valTuple)
	result.Options, _ = valDesc.GetString(2, valTuple)
	return result
}

func GetGlobalTablePatterns(ctx context.Context, root RootValue, schema string, cb func(string)) error {
	table_name := TableName{Name: GlobalTablesTableName, Schema: schema}
	table, found, err := root.GetTable(ctx, table_name)
	if err != nil {
		return err
	}
	if !found {
		// dolt_global_tables doesn't exist, so don't filter any tables.
		return nil
	}

	index, err := table.GetRowData(ctx)
	if table.Format() == types.Format_LD_1 {
		// dolt_global_tables is not supported for the legacy storage format.
		return nil
	}
	if err != nil {
		return err
	}
	ignoreTableSchema, err := table.GetSchema(ctx)
	if err != nil {
		return err
	}
	m := durable.MapFromIndex(index)
	keyDesc, _ := ignoreTableSchema.GetMapDescriptors(m.NodeStore())

	ignoreTableMap, err := m.IterAll(ctx)
	if err != nil {
		return err
	}
	for {
		keyTuple, _, err := ignoreTableMap.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		globalTableName, err := GetGlobalTablesNameColumn(ctx, keyDesc, keyTuple)
		if err != nil {
			return err
		}

		cb(globalTableName)
	}
	return nil
}
