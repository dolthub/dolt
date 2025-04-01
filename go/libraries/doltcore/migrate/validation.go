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

package migrate

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"time"
	"unicode"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/types"
)

func validateBranchMapping(ctx context.Context, old, new *doltdb.DoltDB) error {
	branches, err := old.GetBranches(ctx)
	if err != nil {
		return err
	}

	var ok bool
	for _, bref := range branches {
		_, ok, err = new.HasBranch(ctx, bref.GetPath())
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("failed to map branch %s", bref.GetPath())
		}
	}
	return nil
}

func validateRootValue(ctx context.Context, oldParent, old, new doltdb.RootValue) error {
	names, err := old.GetTableNames(ctx, doltdb.DefaultSchemaName)
	if err != nil {
		return err
	}
	for _, name := range names {
		o, ok, err := old.GetTable(ctx, doltdb.TableName{Name: name})
		if err != nil {
			return err
		}
		if !ok {
			h, _ := old.HashOf()
			return fmt.Errorf("expected to find table %s in root value (%s)", name, h.String())
		}

		// Skip tables that haven't changed
		op, ok, err := oldParent.GetTable(ctx, doltdb.TableName{Name: name})
		if err != nil {
			return err
		}
		if ok {
			oldHash, err := o.HashOf()
			if err != nil {
				return err
			}
			oldParentHash, err := op.HashOf()
			if err != nil {
				return err
			}
			if oldHash.Equal(oldParentHash) {
				continue
			}
		}

		n, ok, err := new.GetTable(ctx, doltdb.TableName{Name: name})
		if err != nil {
			return err
		}
		if !ok {
			h, _ := new.HashOf()
			return fmt.Errorf("expected to find table %s in root value (%s)", name, h.String())
		}

		if err = validateTableData(ctx, name, o, n); err != nil {
			return err
		}
	}
	return nil
}

func validateTableData(ctx context.Context, name string, old, new *doltdb.Table) error {
	parts, err := partitionTable(ctx, old)
	if err != nil {
		return err
	} else if len(parts) == 0 {
		return nil
	}

	eg, ctx := errgroup.WithContext(ctx)
	for i := range parts {
		start, end := parts[i][0], parts[i][1]
		eg.Go(func() error {
			return validateTableDataPartition(ctx, name, old, new, start, end)
		})
	}

	return eg.Wait()
}

func validateTableDataPartition(ctx context.Context, name string, old, new *doltdb.Table, start, end uint64) error {
	sctx := sql.NewContext(ctx)
	_, oldIter, err := sqle.DoltTablePartitionToRowIter(sctx, name, old, start, end)
	if err != nil {
		return err
	}
	newSch, newIter, err := sqle.DoltTablePartitionToRowIter(sctx, name, new, start, end)
	if err != nil {
		return err
	}

	var o, n sql.Row
	for {
		o, err = oldIter.Next(sctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		n, err = newIter.Next(sctx)
		if err != nil {
			return err
		}

		ok, err := equalRows(ctx, o, n, newSch)
		if err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("differing rows for table %s (%s != %s)",
				name, sql.FormatRow(o), sql.FormatRow(n))
		}
	}

	// validated that newIter is also exhausted
	_, err = newIter.Next(sctx)
	if err != io.EOF {
		return fmt.Errorf("differing number of rows for table %s", name)
	}
	return nil
}

func equalRows(ctx context.Context, old, new sql.Row, sch sql.Schema) (bool, error) {
	if len(new) != len(old) || len(new) != len(sch) {
		return false, nil
	}

	var cmp int
	for i := range new {

		// special case string comparisons
		s, ok, err := sql.Unwrap[string](ctx, old[i])
		if err != nil {
			return false, err
		}
		if ok {
			old[i] = strings.TrimRightFunc(s, unicode.IsSpace)
		}
		s, ok, err = sql.Unwrap[string](ctx, new[i])
		if err != nil {
			return false, err
		}
		if ok {
			new[i] = strings.TrimRightFunc(s, unicode.IsSpace)
		}

		// special case time comparison to account
		// for precision changes between formats
		if _, ok := old[i].(time.Time); ok {
			var o, n interface{}
			if o, _, err = gmstypes.Int64.Convert(old[i]); err != nil {
				return false, err
			}
			if n, _, err = gmstypes.Int64.Convert(new[i]); err != nil {
				return false, err
			}
			if cmp, err = gmstypes.Int64.Compare(ctx, o, n); err != nil {
				return false, err
			}
		} else {
			if cmp, err = sch[i].Type.Compare(ctx, old[i], new[i]); err != nil {
				return false, err
			}
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func validateSchema(existing schema.Schema) error {
	for _, c := range existing.GetAllCols().GetColumns() {
		qt := c.TypeInfo.ToSqlType().Type()
		err := assertNomsKind(c.Kind, nomsKindsFromQueryTypes(qt)...)
		if err != nil {
			return err
		}
	}
	return nil
}

func nomsKindsFromQueryTypes(qt query.Type) []types.NomsKind {
	switch qt {
	case query.Type_UINT8:
		return []types.NomsKind{types.UintKind, types.BoolKind}

	case query.Type_UINT16, query.Type_UINT24,
		query.Type_UINT32, query.Type_UINT64:
		return []types.NomsKind{types.UintKind}

	case query.Type_INT8:
		return []types.NomsKind{types.IntKind, types.BoolKind}

	case query.Type_INT16, query.Type_INT24,
		query.Type_INT32, query.Type_INT64:
		return []types.NomsKind{types.IntKind}

	case query.Type_YEAR, query.Type_TIME:
		return []types.NomsKind{types.IntKind}

	case query.Type_FLOAT32, query.Type_FLOAT64:
		return []types.NomsKind{types.FloatKind}

	case query.Type_TIMESTAMP, query.Type_DATE, query.Type_DATETIME:
		return []types.NomsKind{types.TimestampKind}

	case query.Type_DECIMAL:
		return []types.NomsKind{types.DecimalKind}

	case query.Type_TEXT, query.Type_BLOB:
		return []types.NomsKind{
			types.BlobKind,
			types.StringKind,
		}

	case query.Type_VARCHAR, query.Type_CHAR:
		return []types.NomsKind{types.StringKind}

	case query.Type_VARBINARY, query.Type_BINARY:
		return []types.NomsKind{types.InlineBlobKind}

	case query.Type_BIT, query.Type_ENUM, query.Type_SET:
		return []types.NomsKind{types.UintKind}

	case query.Type_GEOMETRY:
		return []types.NomsKind{
			types.GeometryKind,
			types.PointKind,
			types.LineStringKind,
			types.PolygonKind,
			types.MultiPointKind,
			types.MultiLineStringKind,
			types.MultiPolygonKind,
			types.GeometryCollectionKind,
		}

	case query.Type_JSON:
		return []types.NomsKind{types.JSONKind}

	default:
		panic(fmt.Sprintf("unexpect query.Type %s", qt.String()))
	}
}

func assertNomsKind(kind types.NomsKind, candidates ...types.NomsKind) error {
	for _, c := range candidates {
		if kind == c {
			return nil
		}
	}

	cs := make([]string, len(candidates))
	for i, c := range candidates {
		cs[i] = types.KindToString[c]
	}
	return fmt.Errorf("expected NomsKind to be one of (%s), got NomsKind (%s)",
		strings.Join(cs, ", "), types.KindToString[kind])
}

func partitionTable(ctx context.Context, tbl *doltdb.Table) ([][2]uint64, error) {
	idx, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	c, err := idx.Count()
	if err != nil {
		return nil, err
	}
	if c == 0 {
		return nil, nil
	}
	n := runtime.NumCPU() * 2
	szc, err := idx.Count()
	if err != nil {
		return nil, err
	}
	sz := int(szc) / n

	parts := make([][2]uint64, n)

	parts[0][0] = 0
	parts[n-1][1], err = idx.Count()
	if err != nil {
		return nil, err
	}

	for i := 1; i < len(parts); i++ {
		parts[i-1][1] = uint64(i * sz)
		parts[i][0] = uint64(i * sz)
	}

	return parts, nil
}

func assertTrue(b bool) {
	if !b {
		panic("expected true")
	}
}
