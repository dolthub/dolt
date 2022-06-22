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
	"encoding/binary"
	"errors"
	"math"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type mapSqlIter struct {
	ctx context.Context
	nmr *noms.NomsMapReader
	sch schema.Schema
}

var _ sql.RowIter = (*mapSqlIter)(nil)

// Next implements the interface sql.RowIter.
func (m *mapSqlIter) Next(ctx *sql.Context) (sql.Row, error) {
	dRow, err := m.nmr.ReadRow(ctx)
	if err != nil {
		return nil, err
	}
	return DoltRowToSqlRow(dRow, m.sch)
}

// Close implements the interface sql.RowIter.
func (m *mapSqlIter) Close(ctx *sql.Context) error {
	return m.nmr.Close(ctx)
}

// MapToSqlIter returns a map reader that converts all rows to sql rows, creating a sql row iterator.
func MapToSqlIter(ctx context.Context, sch schema.Schema, data types.Map) (sql.RowIter, error) {
	mapReader, err := noms.NewNomsMapReader(ctx, data, sch)
	if err != nil {
		return nil, err
	}
	return &mapSqlIter{
		nmr: mapReader,
		sch: sch,
	}, nil
}

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
		schCol := allCols.GetAtIndex(i)
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

// WriteEWKBHeader writes the SRID, endianness, and type to the byte buffer
// This function assumes v is a valid spatial type
func WriteEWKBHeader(v interface{}, buf []byte) {
	// Write endianness byte (always little endian)
	buf[4] = 1

	// Parse data
	switch v := v.(type) {
	case sql.Point:
		// Write SRID and type
		binary.LittleEndian.PutUint32(buf[0:4], v.SRID)
		binary.LittleEndian.PutUint32(buf[5:9], 1)
	case sql.LineString:
		binary.LittleEndian.PutUint32(buf[0:4], v.SRID)
		binary.LittleEndian.PutUint32(buf[5:9], 2)
	case sql.Polygon:
		binary.LittleEndian.PutUint32(buf[0:4], v.SRID)
		binary.LittleEndian.PutUint32(buf[5:9], 3)
	}
}

// WriteEWKBPointData converts a Point into a byte array in EWKB format
// Very similar to function in GMS
func WriteEWKBPointData(p sql.Point, buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(p.X))
	binary.LittleEndian.PutUint64(buf[8:16], math.Float64bits(p.Y))
}

// WriteEWKBLineData converts a Line into a byte array in EWKB format
func WriteEWKBLineData(l sql.LineString, buf []byte) {
	// Write length of linestring
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(l.Points)))
	// Append each point
	for i, p := range l.Points {
		WriteEWKBPointData(p, buf[4+16*i:4+16*(i+1)])
	}
}

// WriteEWKBPolyData converts a Polygon into a byte array in EWKB format
func WriteEWKBPolyData(p sql.Polygon, buf []byte) {
	// Write length of polygon
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(p.Lines)))
	// Write each line
	start, stop := 0, 4
	for _, l := range p.Lines {
		start, stop = stop, stop+4+16*len(l.Points)
		WriteEWKBLineData(l, buf[start:stop])
	}
}

// SqlColToStr is a utility function for converting a sql column of type interface{} to a string
func SqlColToStr(ctx context.Context, sqlType sql.Type, col interface{}) (string, error) {
	if col != nil {
		switch typedCol := col.(type) {
		case bool:
			if typedCol {
				return "true", nil
			} else {
				return "false", nil
			}
		case sql.Point: //TODO: remove these when fixed in GMS
			buf := make([]byte, 25)
			WriteEWKBHeader(typedCol, buf)
			WriteEWKBPointData(typedCol, buf[9:])
			return string(buf), nil
		case sql.LineString:
			buf := make([]byte, 9+4+16*len(typedCol.Points))
			WriteEWKBHeader(typedCol, buf)
			WriteEWKBLineData(typedCol, buf[9:])
			return string(buf), nil
		case sql.Polygon:
			size := 0
			for _, l := range typedCol.Lines {
				size += 4 + 16*len(l.Points)
			}
			buf := make([]byte, 9+4+size)
			WriteEWKBHeader(typedCol, buf)
			WriteEWKBPolyData(typedCol, buf[9:])
			return string(buf), nil
		case *tree.ByteArray:
			s, err := typedCol.ToString(ctx)
			if err != nil {
				s = err.Error()
			}
			return s, nil
		default:
			res, err := sqlType.SQL(nil, col)
			if err != nil {
				return "", err
			}
			return res.ToString(), nil

		}
	}

	return "", nil
}
