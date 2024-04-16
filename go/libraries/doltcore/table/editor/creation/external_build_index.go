package creation

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"io"
)

const (
	batchSize = 1024
	fileMax   = 128
)

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

	sorter := newTupleSorter(batchSize, fileMax, func(t1, t2 val.Tuple) bool {
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

		sorter.insert(ctx, idxKey)
	}

	sortedKeys := sorter.flush(ctx)

	mut := secondary.Mutate()
	it := sortedKeys.iterAll(ctx)
	defer it.close()
	var lastKey val.Tuple
	for key, ok := it.next(ctx); ok; key, ok = it.next(ctx) {
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
