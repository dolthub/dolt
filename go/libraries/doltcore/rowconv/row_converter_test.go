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

package rowconv

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

var srcCols = schema.NewColCollection(
	schema.NewColumn("uuidtostr", 0, types.UUIDKind, true),
	schema.NewColumn("floattostr", 1, types.FloatKind, false),
	schema.NewColumn("uinttostr", 2, types.UintKind, false),
	schema.NewColumn("booltostr", 3, types.BoolKind, false),
	schema.NewColumn("inttostr", 4, types.IntKind, false),
	schema.NewColumn("stringtostr", 5, types.StringKind, false),
	schema.NewColumn("timestamptostr", 6, types.TimestampKind, false),
)

var srcSch = schema.MustSchemaFromCols(srcCols)

func TestRowConverter(t *testing.T) {
	mapping, err := TypedToUntypedMapping(srcSch)

	require.NoError(t, err)

	vrw := types.NewMemoryValueStore()
	rConv, err := NewRowConverter(context.Background(), vrw, mapping)

	if err != nil {
		t.Fatal("Error creating row converter")
	}

	id, _ := uuid.NewRandom()
	tt := types.Timestamp(time.Now())
	inRow, err := row.New(vrw.Format(), srcSch, row.TaggedValues{
		0: types.UUID(id),
		1: types.Float(1.25),
		2: types.Uint(12345678),
		3: types.Bool(true),
		4: types.Int(-1234),
		5: types.String("string string string"),
		6: tt,
	})

	require.NoError(t, err)
	outData, err := rConv.Convert(inRow)
	require.NoError(t, err)

	destSch := mapping.DestSch
	expected, err := row.New(vrw.Format(), destSch, row.TaggedValues{
		0: types.String(id.String()),
		1: types.String("1.25"),
		2: types.String("12345678"),
		3: types.String("1"),
		4: types.String("-1234"),
		5: types.String("string string string"),
		6: types.String(tt.String()),
	})

	require.NoError(t, err)

	if !row.AreEqual(outData, expected, destSch) {
		t.Error("\n", row.Fmt(context.Background(), expected, destSch), "!=\n", row.Fmt(context.Background(), outData, destSch))
	}
}

func TestUnneccessaryConversion(t *testing.T) {
	mapping, err := TagMapping(srcSch, srcSch)
	if err != nil {
		t.Fatal(err)
	}

	vrw := types.NewMemoryValueStore()
	rconv, err := NewRowConverter(context.Background(), vrw, mapping)
	if err != nil {
		t.Fatal(err)
	}
	if !rconv.IdentityConverter {
		t.Fatal("expected identity converter")
	}
}
