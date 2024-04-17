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

package creation

import (
	"errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/sort"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"io"
)

const (
	batchSize = 32 * 1024 * 1024 // 32MB
	fileMax   = 128
)

// BuildProllyIndexExternal builds unique and non-unique indexes with a
// single prolly tree materialization by presorting the index keys in an
// intermediate file format.
func BuildProllyIndexExternal(
	ctx *sql.Context,
	vrw types.ValueReadWriter,
	ns tree.NodeStore,
	sch schema.Schema,
	tableName string,
	idx schema.Index,
	primary prolly.Map,
	uniqCb DupEntryCb,
) (durable.Index, error) {
	empty, err := durable.NewEmptyIndex(ctx, vrw, ns, idx.Schema())
	if err != nil {
		return nil, err
	}
	secondary := durable.ProllyMapFromIndex(empty)
	if schema.IsKeyless(sch) {
		secondary = prolly.ConvertToSecondaryKeylessIndex(secondary)
	}

	iter, err := primary.IterAll(ctx)
	if err != nil {
		return nil, err
	}
	p := primary.Pool()

	prefixDesc := secondary.KeyDesc().PrefixDesc(idx.Count())
	secondaryBld, err := index.NewSecondaryKeyBuilder(ctx, tableName, sch, idx, secondary.KeyDesc(), p, secondary.NodeStore())
	if err != nil {
		return nil, err
	}

	sorter := sort.NewTupleSorter(batchSize, fileMax, func(t1, t2 val.Tuple) bool {
		return prefixDesc.Compare(t1, t2) < 0
	})

	for {
		k, v, err := iter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		idxKey, err := secondaryBld.SecondaryKeyFromRow(ctx, k, v)
		if err != nil {
			return nil, err
		}

		if uniqCb != nil && prefixDesc.HasNulls(idxKey) {
			continue
		}

		if err := sorter.Insert(ctx, idxKey); err != nil {
			return nil, err
		}
	}

	sortedKeys, err := sorter.Flush(ctx)
	if err != nil {
		return nil, err
	}

	mut := secondary.Mutate()
	it := sortedKeys.IterAll(ctx)
	defer it.Close()
	var lastKey val.Tuple
	for {
		key, err := it.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if lastKey != nil && prefixDesc.Compare(lastKey, key) == 0 {
			if uniqCb != nil {
				// register a constraint violation if |key| collides with |lastKey|
				if err := uniqCb(ctx, lastKey, key); err != nil {
					return nil, err
				}
			}
		}
		if err = mut.Put(ctx, key, val.EmptyTuple); err != nil {
			return nil, err
		}
		lastKey = key
	}

	secondary, err = mut.Map(ctx)
	if err != nil {
		return nil, err
	}

	return durable.IndexFromProllyMap(secondary), nil
}
