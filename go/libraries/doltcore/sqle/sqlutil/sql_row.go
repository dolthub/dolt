// Copyright 2020 Dolthub, Inc.
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

package sqlutil

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	// Necessary for the empty context used by some functions to be initialized with system vars
	_ "github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// DoltRowToSqlRow constructs a go-mysql-server sql.Row from a Dolt row.Row.
func DoltRowToSqlRow(doltRow row.Row, sch schema.Schema) (sql.Row, error) {
	if doltRow == nil {
		return nil, nil
	}

	colVals := make(sql.Row, sch.GetAllCols().Size())
	i := 0

	_, err := doltRow.IterSchema(sch, func(tag uint64, val types.Value) (stop bool, err error) {
		col, _ := sch.GetAllCols().GetByTag(tag)
		colVals[i], err = col.TypeInfo.ConvertNomsValueToValue(val)
		i++

		stop = err != nil
		return
	})
	if err != nil {
		return nil, err
	}

	return sql.NewRow(colVals...), nil
}

// SqlRowToDoltRow constructs a Dolt row.Row from a go-mysql-server sql.Row.
func SqlRowToDoltRow(ctx context.Context, vrw types.ValueReadWriter, r sql.Row, doltSchema schema.Schema) (row.Row, error) {
	if schema.IsKeyless(doltSchema) {
		return keylessDoltRowFromSqlRow(ctx, vrw, r, doltSchema)
	}
	return pkDoltRowFromSqlRow(ctx, vrw, r, doltSchema)
}

// DoltKeyValueAndMappingFromSqlRow converts a sql.Row to key and value tuples and keeps a mapping from tag to value that
// can be used to speed up index key generation for foreign key checks.
func DoltKeyValueAndMappingFromSqlRow(ctx context.Context, vrw types.ValueReadWriter, r sql.Row, doltSchema schema.Schema) (types.Tuple, types.Tuple, map[uint64]types.Value, error) {
	numCols := doltSchema.GetAllCols().Size()
	vals := make([]types.Value, numCols*2)
	tagToVal := make(map[uint64]types.Value, numCols)

	nonPKCols := doltSchema.GetNonPKCols()
	numNonPKVals := nonPKCols.Size() * 2
	nonPKVals := vals[:numNonPKVals]
	pkVals := vals[numNonPKVals:]

	for i, c := range doltSchema.GetAllCols().GetColumns() {
		val := r[i]
		if val == nil {
			continue
		}

		nomsVal, err := c.TypeInfo.ConvertValueToNomsValue(ctx, vrw, val)
		if err != nil {
			return types.Tuple{}, types.Tuple{}, nil, err
		}

		tagToVal[c.Tag] = nomsVal
	}

	nonPKIdx := 0
	for _, tag := range nonPKCols.SortedTags {
		// nonPkCols sorted by ascending tag order
		if val, ok := tagToVal[tag]; ok {
			nonPKVals[nonPKIdx] = types.Uint(tag)
			nonPKVals[nonPKIdx+1] = val
			nonPKIdx += 2
		}
	}

	pkIdx := 0
	for _, tag := range doltSchema.GetPKCols().Tags {
		// pkCols are in the primary key defined order
		if val, ok := tagToVal[tag]; ok {
			pkVals[pkIdx] = types.Uint(tag)
			pkVals[pkIdx+1] = val
			pkIdx += 2
		}
	}

	nonPKVals = nonPKVals[:nonPKIdx]

	nbf := vrw.Format()
	keyTuple, err := types.NewTuple(nbf, pkVals...)

	if err != nil {
		return types.Tuple{}, types.Tuple{}, nil, err
	}

	valTuple, err := types.NewTuple(nbf, nonPKVals...)

	if err != nil {
		return types.Tuple{}, types.Tuple{}, nil, err
	}

	return keyTuple, valTuple, tagToVal, nil
}

// DoltKeyAndMappingFromSqlRow converts a sql.Row to key tuple and keeps a mapping from tag to value that
// can be used to speed up index key generation for foreign key checks.
func DoltKeyAndMappingFromSqlRow(ctx context.Context, vrw types.ValueReadWriter, r sql.Row, doltSchema schema.Schema) (types.Tuple, map[uint64]types.Value, error) {
	if r == nil {
		return types.EmptyTuple(vrw.Format()), nil, sql.ErrUnexpectedNilRow.New()
	}

	allCols := doltSchema.GetAllCols()
	pkCols := doltSchema.GetPKCols()

	numCols := allCols.Size()
	numPKCols := pkCols.Size()
	pkVals := make([]types.Value, numPKCols*2)
	tagToVal := make(map[uint64]types.Value, numCols)

	if len(r) < numCols {
		numCols = len(r)
	}

	for i := 0; i < numCols; i++ {
		schCol := allCols.GetByIndex(i)
		val := r[i]
		if val == nil {
			continue
		}

		tag := schCol.Tag
		nomsVal, err := schCol.TypeInfo.ConvertValueToNomsValue(ctx, vrw, val)

		if err != nil {
			return types.Tuple{}, nil, err
		}

		tagToVal[tag] = nomsVal
	}

	pkOrds := doltSchema.GetPkOrdinals()
	for i, pkCol := range pkCols.GetColumns() {
		ord := pkOrds[i]
		val := r[ord]
		if val == nil {
			return types.Tuple{}, nil, errors.New("not all pk columns have a value")
		}
		pkVals[i*2] = types.Uint(pkCol.Tag)
		pkVals[i*2+1] = tagToVal[pkCol.Tag]
	}

	nbf := vrw.Format()
	keyTuple, err := types.NewTuple(nbf, pkVals...)

	if err != nil {
		return types.Tuple{}, nil, err
	}

	return keyTuple, tagToVal, nil
}

func pkDoltRowFromSqlRow(ctx context.Context, vrw types.ValueReadWriter, r sql.Row, doltSchema schema.Schema) (row.Row, error) {
	taggedVals := make(row.TaggedValues)
	allCols := doltSchema.GetAllCols()
	for i, val := range r {
		tag := allCols.Tags[i]
		schCol := allCols.TagToCol[tag]
		if val != nil {
			var err error
			taggedVals[tag], err = schCol.TypeInfo.ConvertValueToNomsValue(ctx, vrw, val)
			if err != nil {
				return nil, err
			}
		}
	}
	return row.New(vrw.Format(), doltSchema, taggedVals)
}

func keylessDoltRowFromSqlRow(ctx context.Context, vrw types.ValueReadWriter, sqlRow sql.Row, sch schema.Schema) (row.Row, error) {
	j := 0
	vals := make([]types.Value, sch.GetAllCols().Size()*2)

	for idx, val := range sqlRow {
		if val != nil {
			col := sch.GetAllCols().GetByIndex(idx)
			nv, err := col.TypeInfo.ConvertValueToNomsValue(ctx, vrw, val)
			if err != nil {
				return nil, err
			}

			vals[j] = types.Uint(col.Tag)
			vals[j+1] = nv
			j += 2
		}
	}

	return row.KeylessRow(vrw.Format(), vals[:j]...)
}

// SqlColToStr is a utility function for converting a sql column of type interface{} to a string.
// NULL values are treated as empty strings. Handle nil separately if you require other behavior.
func SqlColToStr(ctx *sql.Context, sqlType sql.Type, col interface{}) (string, error) {
	if col != nil {
		switch typedCol := col.(type) {
		case bool:
			if typedCol {
				return "true", nil
			} else {
				return "false", nil
			}
		case sql.SpatialColumnType:
			res, err := sqlType.SQL(ctx, nil, col)
			hexRes := fmt.Sprintf("0x%X", res.Raw())
			if err != nil {
				return "", err
			}
			return hexRes, nil
		default:
			res, err := sqlType.SQL(ctx, nil, col)
			if err != nil {
				return "", err
			}
			return res.ToString(), nil
		}
	}

	return "", nil
}

// BinaryAsHexString is a type indicator for a binary value has been processed by the --binary-as-hex flag iterator
// into a hex string value.
type BinaryAsHexString string

// binaryHexIterator wraps a row iterator and transforms binary data to hex format
type binaryHexIterator struct {
	inner  sql.RowIter
	schema sql.Schema
}

var _ sql.RowIter = (*binaryHexIterator)(nil)

// NewBinaryHexIterator creates a new iterator that transforms binary data to hex format
func NewBinaryHexIterator(inner sql.RowIter, schema sql.Schema) sql.RowIter {
	return &binaryHexIterator{
		inner:  inner,
		schema: schema,
	}
}

// Next returns the next row with binary data transformed to hex format.
func (iter *binaryHexIterator) Next(ctx *sql.Context) (rowData sql.Row, err error) {
	rowData, err = iter.inner.Next(ctx)
	if err != nil {
		return nil, err
	}

	for i, val := range rowData {
		if bytesWrapper, ok := val.(sql.BytesWrapper); ok {
			val, err = bytesWrapper.Unwrap(ctx)
			if err != nil {
				return nil, err
			}
		}

		hexBytes, err := convertBinaryToUpperHexBytes(val)
		if err != nil {
			return nil, err
		}

		var strBuilder strings.Builder
		switch iter.schema[i].Type.Type() {
		case sqltypes.Binary, sqltypes.VarBinary, sqltypes.Blob:
			strBuilder.Grow(2 + len(hexBytes))
			strBuilder.WriteByte('0')
			strBuilder.WriteByte('x')
			strBuilder.Write(hexBytes)
			rowData[i] = BinaryAsHexString(strBuilder.String())
		case sqltypes.Bit:
			padding := 0
			if bitType, ok := iter.schema[i].Type.(gmstypes.BitType); ok {
				expectedLen := (int(bitType.NumberOfBits()) + 3) / 4
				if len(hexBytes) < expectedLen {
					padding = expectedLen - len(hexBytes)
				}
			}

			strBuilder.Grow(2 + padding + len(hexBytes))
			strBuilder.WriteByte('0')
			strBuilder.WriteByte('x')
			for repeat := 0; repeat < padding; repeat++ {
				strBuilder.WriteByte('0')
			}
			strBuilder.Write(hexBytes)
			rowData[i] = BinaryAsHexString(strBuilder.String())
		}
	}

	return rowData, nil
}

// convertBinaryToUpperHexBytes converts the input |binary| into uppercase hexadecimal bytes. This is optimized for
// large byte arrays i.e. sqltypes.Blob, reimplementing a modified version of the hex encoder from Go's standard library
// to do uppercasing in a single pass.
func convertBinaryToUpperHexBytes(val any) ([]byte, error) {
	upperHexTable := "0123456789ABCDEF"
	var valBytes []byte
	switch v := val.(type) {
	case []byte:
		valBytes = v
	case string:
		valBytes = []byte(v)
	case uint64:
		if v == 0 {
			return []byte{'0'}, nil
		}
		valBytes = make([]byte, 16)
		// uint64 contains 16 nibbles (4-bit chunks) obtained by & 0xF.
		// Map each nibble directly to the hex table above.
		// We assume big-endian and shift right to process from least to greatest bit.
		index := 15
		for v > 0 {
			valBytes[index] = upperHexTable[v&0xF]
			v >>= 4
			index--
		}

		// Index + 1 to avoid leading zeros.
		return valBytes[index+1:], nil
	default:
		return nil, fmt.Errorf("unexpected type %T (%v)", val, val)
	}

	if len(valBytes) == 0 {
		return []byte{}, nil
	}

	hexBuffer := make([]byte, hex.EncodedLen(len(valBytes)))
	for index, valByte := range valBytes {
		// Each byte contains 2 nibbles (4-bit chunks), map each to upper hex table above.
		hexBuffer[index*2] = upperHexTable[valByte>>4]
		hexBuffer[index*2+1] = upperHexTable[valByte&0x0F]
	}
	return hexBuffer, nil
}

// Close closes the wrapped iterator and releases any resources.
func (iter *binaryHexIterator) Close(ctx *sql.Context) error {
	return iter.inner.Close(ctx)
}
