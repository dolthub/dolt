// Copyright 2021 Dolthub, Inc.
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

package dtables

import (
	"github.com/dolthub/go-mysql-server/sql"
	sqlTypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
)

var _ sql.Table = (*DocsTable)(nil)
var _ sql.UpdatableTable = (*DocsTable)(nil)
var _ sql.DeletableTable = (*DocsTable)(nil)
var _ sql.InsertableTable = (*DocsTable)(nil)
var _ sql.ReplaceableTable = (*DocsTable)(nil)
var _ sql.IndexAddressableTable = (*DocsTable)(nil)

// DocsTable is the system table that stores Dolt docs, such as LICENSE and README.
type DocsTable struct {
	BackedSystemTable
}

// NewDocsTable creates a DocsTable
func NewDocsTable(_ *sql.Context, backingTable VersionableTable) sql.Table {
	return &DocsTable{
		BackedSystemTable: BackedSystemTable{
			backingTable: backingTable,
			tableName:    getDoltDocsTableName(),
			schema:       GetDocsSchema(),
		},
	}
}

// NewEmptyDocsTable creates a DocsTable
func NewEmptyDocsTable(_ *sql.Context) sql.Table {
	return &DocsTable{
		BackedSystemTable: BackedSystemTable{
			tableName: getDoltDocsTableName(),
			schema:    GetDocsSchema(),
		},
	}
}

const defaultStringsLen = 16383 / 16

// GetDocsSchema returns the schema of the dolt_docs system table. This is used
// by Doltgres to update the dolt_docs schema using Doltgres types.
var GetDocsSchema = getDoltDocsSchema

func getDoltDocsSchema() sql.Schema {
	return []*sql.Column{
		{Name: doltdb.DocPkColumnName, Type: sqlTypes.MustCreateString(sqltypes.VarChar, defaultStringsLen, sql.Collation_Default), Source: doltdb.GetDocTableName(), PrimaryKey: true, Nullable: false},
		{Name: doltdb.DocTextColumnName, Type: sqlTypes.LongText, Source: doltdb.GetDocTableName(), PrimaryKey: false},
	}
}

func (dt *DocsTable) LockedToRoot(ctx *sql.Context, root doltdb.RootValue) (sql.IndexAddressableTable, error) {
	if dt.backingTable == nil {
		return dt, nil
	}
	backingTableLockedToRoot, err := dt.backingTable.LockedToRoot(ctx, root)
	if err != nil {
		return nil, err
	}
	return &docsTableAsOf{backingTableLockedToRoot}, nil
}

func (dt *DocsTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	underlyingIter, err := dt.BackedSystemTable.PartitionRows(ctx, partition)
	if err != nil {
		return nil, err
	}
	return makeDoltDocRows(ctx, underlyingIter)
}

func makeDoltDocRows(ctx *sql.Context, underlyingIter sql.RowIter) (sql.RowIter, error) {
	rows, err := sql.RowIterToRows(ctx, underlyingIter)

	if err != nil {
		return nil, err
	}

	found := false
	for i := range rows {
		name, ok := rows[i][0].(string)
		if !ok {
			continue
		}

		if name == doltdb.AgentDoc {
			found = true
			break
		}
	}

	if !found {
		rows = append(rows, []interface{}{
			doltdb.AgentDoc,
			doltdb.DefaultAgentDocValue,
		})
	}

	return sql.RowsToRowIter(rows...), nil
}

func getDoltDocsTableName() doltdb.TableName {
	if resolve.UseSearchPath {
		return doltdb.TableName{Schema: doltdb.DoltNamespace, Name: doltdb.GetDocTableName()}
	}
	return doltdb.TableName{Name: doltdb.GetDocTableName()}
}

type docsTableAsOf struct {
	sql.IndexAddressableTable
}

func (dt *docsTableAsOf) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	underlyingIter, err := dt.IndexAddressableTable.PartitionRows(ctx, partition)
	if err != nil {
		return nil, err
	}
	return makeDoltDocRows(ctx, underlyingIter)
}
