// Copyright 2024 Dolthub, Inc.
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

package statspro

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func loadLowerBound(ctx *sql.Context, qual sql.StatQualifier) (sql.Row, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	roots, ok := dSess.GetRoots(ctx, qual.Database)
	if !ok {
		return nil, nil
	}

	table, ok, err := roots.Head.GetTable(ctx, qual.Table())
	if !ok {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	idx, err := table.GetIndexRowData(ctx, qual.Index())
	if err != nil {
		return nil, err
	}

	prollyMap := durable.ProllyMapFromIndex(idx)
	keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc())
	buffPool := prollyMap.NodeStore().Pool()

	firstIter, err := prollyMap.IterOrdinalRange(ctx, 0, 1)
	if err != nil {
		return nil, err
	}
	keyBytes, _, err := firstIter.Next(ctx)
	if err != nil {
		return nil, err
	}
	for i := range keyBuilder.Desc.Types {
		keyBuilder.PutRaw(i, keyBytes.GetField(i))
	}

	firstKey := keyBuilder.Build(buffPool)
	var firstRow sql.Row
	for i := 0; i < keyBuilder.Desc.Count(); i++ {
		firstRow[i], err = tree.GetField(ctx, prollyMap.KeyDesc(), i, firstKey, prollyMap.NodeStore())
		if err != nil {
			return nil, err
		}
	}
	return firstRow, nil
}

func loadFuncDeps(ctx *sql.Context, db dsess.SqlDatabase, qual sql.StatQualifier) (*sql.FuncDepSet, sql.ColSet, error) {
	tab, ok, err := db.GetTableInsensitive(ctx, qual.Table())
	if err != nil {
		return nil, sql.ColSet{}, err
	} else if !ok {
		return nil, sql.ColSet{}, fmt.Errorf("%w: table not found: '%s'", ErrFailedToLoad, qual.Table())
	}

	iat, ok := tab.(sql.IndexAddressable)
	if !ok {
		return nil, sql.ColSet{}, fmt.Errorf("%w: table does not have indexes: '%s'", ErrFailedToLoad, qual.Table())
	}

	indexes, err := iat.GetIndexes(ctx)
	if err != nil {
		return nil, sql.ColSet{}, err
	}

	var idx sql.Index
	for _, i := range indexes {
		if strings.EqualFold(i.ID(), qual.Index()) {
			idx = i
			break
		}
	}

	if idx == nil {
		return nil, sql.ColSet{}, fmt.Errorf("%w: index not found: '%s'", ErrFailedToLoad, qual.Index())
	}

	return stats.IndexFds(qual.Table(), tab.Schema(), idx)
}
