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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/proto/query"

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
		ok, err = new.HasBranch(ctx, bref.GetPath())
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("failed to map branch %s", bref.GetPath())
		}
	}
	return nil
}

func validateRootValue(ctx context.Context, old, new *doltdb.RootValue) error {
	names, err := old.GetTableNames(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		o, ok, err := old.GetTable(ctx, name)
		if err != nil {
			return err
		}
		if !ok {
			h, _ := old.HashOf()
			return fmt.Errorf("expected to find table %s in root value (%s)", name, h.String())
		}

		n, ok, err := new.GetTable(ctx, name)
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
	sctx := sql.NewContext(ctx)
	oldSch, oldIter, err := sqle.DoltTableToRowIter(sctx, name, old)
	if err != nil {
		return err
	}
	newSch, newIter, err := sqle.DoltTableToRowIter(sctx, name, old)
	if err != nil {
		return err
	}
	if !oldSch.Equals(newSch) {
		return fmt.Errorf("differing schemas for table %s", name)
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

		ok, err := o.Equals(n, newSch)
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
	case query.Type_UINT8, query.Type_UINT16, query.Type_UINT24,
		query.Type_UINT32, query.Type_UINT64:
		return []types.NomsKind{types.UintKind}

	case query.Type_INT8, query.Type_INT16, query.Type_INT24,
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

func hashRow(sctx *sql.Context, r sql.Row) (uint64, error) {
	for i := range r {
		// normalize fields
		switch x := r[i].(type) {
		case sql.JSONValue:
			s, err := x.ToString(sctx)
			if err != nil {
				return 0, err
			}
			r[i] = s
		}
	}
	return sql.HashOf(r)
}
