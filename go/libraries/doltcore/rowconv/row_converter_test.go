// Copyright 2019 Liquidata, Inc.
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

	"github.com/google/uuid"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

var srcCols, _ = schema.NewColCollection(
	schema.NewColumn("uuidtostr", 0, types.UUIDKind, true),
	schema.NewColumn("floattostr", 1, types.FloatKind, false),
	schema.NewColumn("uinttostr", 2, types.UintKind, false),
	schema.NewColumn("booltostr", 3, types.BoolKind, false),
	schema.NewColumn("inttostr", 4, types.IntKind, false),
	schema.NewColumn("stringtostr", 5, types.StringKind, false),
	schema.NewColumn("nulltostr", 6, types.NullKind, false),
)

var srcSch = schema.SchemaFromCols(srcCols)

func TestRowConverter(t *testing.T) {
	mapping := TypedToUntypedMapping(srcSch)

	rConv, err := NewRowConverter(mapping)

	if err != nil {
		t.Fatal("Error creating row converter")
	}

	id, _ := uuid.NewRandom()
	inRow := row.New(types.Format_7_18, srcSch, row.TaggedValues{
		0: types.UUID(id),
		1: types.Float(1.25),
		2: types.Uint(12345678),
		3: types.Bool(true),
		4: types.Int(-1234),
		5: types.String("string string string"),
		6: types.NullValue,
	})

	results, _ := GetRowConvTransformFunc(rConv)(inRow, pipeline.ImmutableProperties{})
	outData := results[0].RowData

	destSch := mapping.DestSch
	expected := row.New(types.Format_7_18, destSch, row.TaggedValues{
		0: types.String(id.String()),
		1: types.String("1.25"),
		2: types.String("12345678"),
		3: types.String("true"),
		4: types.String("-1234"),
		5: types.String("string string string"),
		6: types.NullValue,
	})

	if !row.AreEqual(outData, expected, destSch) {
		t.Error("\n", row.Fmt(context.Background(), expected, destSch), "!=\n", row.Fmt(context.Background(), outData, destSch))
	}
}

func TestUnneccessaryConversion(t *testing.T) {
	mapping, err := TagMapping(srcSch, srcSch)

	if err != nil {
		t.Error(err)
	}

	rconv, err := NewRowConverter(mapping)

	if !rconv.IdentityConverter {
		t.Error("expected identity converter")
	}
}
