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

func validateRootValueChecksums(ctx context.Context, old, new *doltdb.RootValue) error {
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
		oldSch, err := o.GetSchema(ctx)
		if err != nil {
			return err
		}

		n, ok, err := new.GetTable(ctx, name)
		if err != nil {
			return err
		}
		if !ok {
			h, _ := new.HashOf()
			return fmt.Errorf("expected to find table %s in root value (%s)", name, h.String())
		}
		newSch, err := n.GetSchema(ctx)
		if err != nil {
			return err
		}

		// update |o| if schema was patched
		if !schema.SchemasAreEqual(oldSch, newSch) {
			o, err = o.UpdateSchema(ctx, newSch)
			if err != nil {
				return err
			}
		}

		oldSum, err := checksumTable(ctx, name, o)
		if err != nil {
			return err
		}

		newSum, err := checksumTable(ctx, name, n)
		if err != nil {
			return err
		}

		if oldSum != newSum {
			return fmt.Errorf("migrated table has different checksum (%d != %d)", oldSum, newSum)
		}
	}
	return nil
}

func checksumTable(ctx context.Context, name string, tbl *doltdb.Table) (uint64, error) {
	sctx := sql.NewEmptyContext()
	iter, err := sqle.DoltTableToRowIter(sctx, name, tbl)
	if err != nil {
		return 0, err
	}

	var checksum uint64
	for {
		r, err := iter.Next(sctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}

		h, err := hashRow(sctx, r)
		if err != nil {
			return 0, err
		}

		checksum ^= h
	}
	return checksum, nil
}

func validateSchema(existing schema.Schema) error {
	for _, c := range existing.GetAllCols().GetColumns() {
		qt := c.TypeInfo.ToSqlType().Type()
		nk := mapQueryTypeToNomsKind(qt)
		if qt == query.Type_GEOMETRY {
			continue // indefinite NomsKind
		}
		if c.Kind != nk {
			return fmt.Errorf("expected NomsKind %s for query.Type %s, got NomsKind %s",
				types.KindToString[nk], qt.String(), types.KindToString[c.Kind])
		}
	}
	return nil
}

func mapQueryTypeToNomsKind(qt query.Type) types.NomsKind {
	switch qt {
	case query.Type_UINT8, query.Type_UINT16, query.Type_UINT24,
		query.Type_UINT32, query.Type_UINT64:
		return types.UintKind

	case query.Type_INT8, query.Type_INT16, query.Type_INT24,
		query.Type_INT32, query.Type_INT64:
		return types.IntKind

	case query.Type_YEAR, query.Type_TIME:
		return types.IntKind

	case query.Type_FLOAT32, query.Type_FLOAT64:
		return types.FloatKind

	case query.Type_TIMESTAMP, query.Type_DATE, query.Type_DATETIME:
		return types.TimestampKind

	case query.Type_DECIMAL:
		return types.DecimalKind

	case query.Type_TEXT, query.Type_BLOB:
		return types.BlobKind

	case query.Type_VARCHAR, query.Type_CHAR:
		return types.StringKind

	case query.Type_VARBINARY, query.Type_BINARY:
		return types.InlineBlobKind

	case query.Type_BIT, query.Type_ENUM, query.Type_SET:
		return types.UintKind

	case query.Type_GEOMETRY:
		return types.GeometryKind

	case query.Type_JSON:
		return types.JSONKind
	default:
		panic(fmt.Sprintf("unexpect query.Type %s", qt.String()))
	}
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
