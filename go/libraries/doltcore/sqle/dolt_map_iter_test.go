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

package sqle

import (
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/types"
)

var ExampleKeyTupleBuffer []byte = []byte{
	18, 8, 16, 165, 9, 2, 16, 77, 85, 76, 84, 73, 80, 76, 69, 95, 80, 65, 82, 84, 73,
	69, 83, 16, 171, 63, 15, 0, 16, 220, 71, 2, 12, 85, 83, 32, 80, 82, 69, 83, 73,
	68, 69, 78, 84, 16, 137, 104, 16, 22,
}

var ExampleValueTupleBuffer []byte = []byte{
	18, 16, 16, 221, 125, 16, 224, 15, 16, 204, 120, 2, 3, 103, 101, 110, 16, 241, 35,
	2, 7, 67, 76, 65, 89, 32, 52, 50, 16, 235, 117, 2, 15, 79, 78, 79, 78, 68, 65, 71,
	65, 32, 67, 79, 85, 78, 84, 89, 16, 172, 106, 2, 8, 78, 69, 87, 32, 89, 79, 82,
	75, 16, 192, 15, 2, 8, 79, 78, 79, 78, 68, 65, 71, 65, 16, 237, 3, 2, 15, 72, 73,
	76, 76, 65, 82, 89, 32, 67, 76, 73, 78, 84, 79, 78, 16, 182, 50, 2, 5, 84, 79, 84,
	65, 76,
}

var TagsToSQLColIdx map[uint64]int = map[uint64]int{
	uint64(493):   6,
	uint64(1189):  7,
	uint64(1984):  5,
	uint64(4593):  2,
	uint64(6454):  10,
	uint64(8107):  8,
	uint64(9180):  9,
	uint64(13321): 11,
	uint64(13612): 4,
	uint64(15083): 3,
	uint64(15436): 1,
	uint64(16093): 0,
}

const RowSize = 12

var Columns []schema.Column = []schema.Column{
	schema.Column{
		Name:       "election_year",
		Tag:        uint64(16093),
		Kind:       types.UintKind,
		IsPartOfPK: true,
		TypeInfo:   typeinfo.Uint32Type,
	},
	schema.Column{
		Name:       "stage",
		Tag:        uint64(15436),
		Kind:       types.StringKind,
		IsPartOfPK: true,
		TypeInfo:   typeinfo.StringDefaultType,
	},
	schema.Column{
		Name:       "precinct",
		Tag:        uint64(4593),
		Kind:       types.StringKind,
		IsPartOfPK: true,
		TypeInfo:   typeinfo.StringDefaultType,
	},
	schema.Column{
		Name:       "county",
		Tag:        uint64(15083),
		Kind:       types.StringKind,
		IsPartOfPK: true,
		TypeInfo:   typeinfo.StringDefaultType,
	},
	schema.Column{
		Name:       "state",
		Tag:        uint64(13612),
		Kind:       types.StringKind,
		IsPartOfPK: true,
		TypeInfo:   typeinfo.StringDefaultType,
	},
	schema.Column{
		Name:       "jurisdiction",
		Tag:        uint64(1984),
		Kind:       types.StringKind,
		IsPartOfPK: true,
		TypeInfo:   typeinfo.StringDefaultType,
	},
	schema.Column{
		Name:       "candidate",
		Tag:        uint64(493),
		Kind:       types.StringKind,
		IsPartOfPK: true,
		TypeInfo:   typeinfo.StringDefaultType,
	},
	schema.Column{
		Name:       "party",
		Tag:        uint64(1189),
		Kind:       types.StringKind,
		IsPartOfPK: false,
		TypeInfo:   typeinfo.StringDefaultType,
	},
	schema.Column{
		Name:       "writein",
		Tag:        uint64(8107),
		Kind:       types.IntKind,
		IsPartOfPK: false,
		TypeInfo:   typeinfo.Int8Type,
	},
	schema.Column{
		Name:       "office",
		Tag:        uint64(9180),
		Kind:       types.StringKind,
		IsPartOfPK: false,
		TypeInfo:   typeinfo.StringDefaultType,
	},
	schema.Column{
		Name:       "vote_mode",
		Tag:        uint64(6454),
		Kind:       types.StringKind,
		IsPartOfPK: false,
		TypeInfo:   typeinfo.StringDefaultType,
	},
	schema.Column{
		Name:       "votes",
		Tag:        uint64(13321),
		Kind:       types.UintKind,
		IsPartOfPK: false,
		TypeInfo:   typeinfo.Uint32Type,
	},
}

func BenchmarkKeyTupleNext8(b *testing.B) {
	ts := &chunks.TestStorage{}
	vs := types.NewValueStore(ts.NewView())
	v, err := types.DecodeValue(chunks.NewChunk(ExampleKeyTupleBuffer), vs)
	if err != nil {
		panic(err)
	}
	tuple := v.(types.Tuple)
	tupItr, err := tuple.Iterator()
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := tupItr.InitForTuple(tuple)
		if err != nil {
			panic(err)
		}
		for {
			_, t, err := tupItr.Next()
			if err != nil {
				panic(err)
			}
			if t == nil {
				break
			}
			_, _, err = tupItr.Next()
			if err != nil {
				panic(err)
			}
		}
	}
}

func BenchmarkValueTupleNext8(b *testing.B) {
	ts := &chunks.TestStorage{}
	vs := types.NewValueStore(ts.NewView())
	v, err := types.DecodeValue(chunks.NewChunk(ExampleValueTupleBuffer), vs)
	if err != nil {
		panic(err)
	}
	tuple := v.(types.Tuple)
	tupItr, err := tuple.Iterator()
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := tupItr.InitForTuple(tuple)
		if err != nil {
			panic(err)
		}
		for {
			_, t, err := tupItr.Next()
			if err != nil {
				panic(err)
			}
			if t == nil {
				break
			}
			_, _, err = tupItr.Next()
			if err != nil {
				panic(err)
			}
		}
	}
}

func BenchmarkKVConverter8(b *testing.B) {
	converter := NewKVToSqlRowConverter(TagsToSQLColIdx, Columns, RowSize)
	if converter == nil {
		panic("converter is nil")
	}
	ts := &chunks.TestStorage{}
	vs := types.NewValueStore(ts.NewView())
	k, err := types.DecodeValue(chunks.NewChunk(ExampleKeyTupleBuffer), vs)
	if err != nil {
		panic(err)
	}
	v, err := types.DecodeValue(chunks.NewChunk(ExampleValueTupleBuffer), vs)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := converter.ConvertKVToSqlRow(k, v)
		if err != nil {
			panic(err)
		}
		if r == nil {
			panic("sql.Row was nil")
		}
	}
}

func BenchmarkOldKVConverter8(b *testing.B) {
	converter := NewKVToSqlRowConverter(TagsToSQLColIdx, Columns, RowSize)
	if converter == nil {
		panic("converter is nil")
	}
	ts := &chunks.TestStorage{}
	vs := types.NewValueStore(ts.NewView())
	k, err := types.DecodeValue(chunks.NewChunk(ExampleKeyTupleBuffer), vs)
	if err != nil {
		panic(err)
	}
	v, err := types.DecodeValue(chunks.NewChunk(ExampleValueTupleBuffer), vs)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := converter.ConvertKVToSqlRow(k, v)
		if err != nil {
			panic(err)
		}
		if r == nil {
			panic("sql.Row was nil")
		}
	}
}

func BenchmarkNewKVConverter8(b *testing.B) {
	converter := NewKVToSqlRowConverter(TagsToSQLColIdx, Columns, RowSize)
	if converter == nil {
		panic("converter is nil")
	}
	ts := &chunks.TestStorage{}
	vs := types.NewValueStore(ts.NewView())
	k, err := types.DecodeValue(chunks.NewChunk(ExampleKeyTupleBuffer), vs)
	if err != nil {
		panic(err)
	}
	v, err := types.DecodeValue(chunks.NewChunk(ExampleValueTupleBuffer), vs)
	if err != nil {
		panic(err)
	}

	itr, err := v.(types.Tuple).Iterator()
	if err != nil {
		panic(err)
	}

	cols := make([]interface{}, RowSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := converter.newConvert(k, v, itr, cols)
		if err != nil {
			panic(err)
		}
		if r == nil {
			panic("sql.Row was nil")
		}
	}
}
