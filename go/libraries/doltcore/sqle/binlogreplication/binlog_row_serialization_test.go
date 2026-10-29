// Copyright 2026 Dolthub, Inc.
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

package binlogreplication

import (
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// TestBinlogFullGeneratedColumns inspects the actual emitted row events, rather
// than querying a replica, which could recompute omitted generated values.
func TestBinlogFullGeneratedColumns(t *testing.T) {
	for _, keyless := range []bool{false, true} {
		for _, virtual := range []bool{false, true} {
			name := "primary key"
			if keyless {
				name = "keyless"
			}
			if virtual {
				name += "/virtual"
			} else {
				name += "/stored"
			}
			t.Run(name, func(t *testing.T) {
				ctx := sql.NewEmptyContext()
				require.NoError(t, ctx.SetSessionVariable(ctx, "binlog_row_image", "FULL"))
				cols := []schema.Column{
					schema.NewColumn("id", 0, types.IntKind, !keyless),
					schema.NewColumn("a", 1, types.IntKind, false),
					schema.NewColumn("g", 2, types.IntKind, false),
					schema.NewColumn("z", 3, types.IntKind, false),
					schema.NewColumn("g2", 4, types.IntKind, false),
					schema.NewColumn("hidden_expr", 5, types.IntKind, false),
				}
				cols[2].Generated, cols[2].Virtual = "a + z", virtual
				cols[4].Generated, cols[4].Virtual = "g + 1", virtual
				cols[5].Generated, cols[5].Virtual, cols[5].SystemHidden = "a + 2", true, true
				sch := schema.MustSchemaFromCols(schema.NewColCollection(cols...))
				empty := binlogTestTable(t, ctx, sch, nil)
				before := binlogTestTable(t, ctx, sch, []sql.Row{{int64(1), int64(10), int64(13), int64(3), int64(14), nil}})
				after := binlogTestTable(t, ctx, sch, []sql.Row{{int64(1), int64(20), int64(23), int64(3), int64(24), nil}})
				nulls := binlogTestTable(t, ctx, sch, []sql.Row{{int64(1), nil, nil, int64(3), nil, nil}})
				format := mysql.NewMySQL56BinlogFormat()
				producer := &binlogProducer{binlogFormat: &format, binlogEventMeta: mysql.BinlogEventMetadata{ServerID: 1}}
				for _, tc := range []struct {
					name     string
					from, to *doltdb.Table
					old, new []string
				}{
					{"insert", empty, before, nil, []string{"1", "10", "13", "3", "14"}},
					{"update", before, after, []string{"1", "10", "13", "3", "14"}, []string{"1", "20", "23", "3", "24"}},
					{"delete", after, empty, []string{"1", "20", "23", "3", "24"}, nil},
					{"insert null", empty, nulls, nil, []string{"1", "NULL", "NULL", "3", "NULL"}},
					{"delete null", nulls, empty, []string{"1", "NULL", "NULL", "3", "NULL"}, nil},
				} {
					t.Run(tc.name, func(t *testing.T) {
						delta := diff.TableDelta{FromName: doltdb.TableName{Name: "t"}, ToName: doltdb.TableName{Name: "t"}, FromTable: tc.from, ToTable: tc.to, FromSch: sch, ToSch: sch}
						events, err := producer.createRowEvents(ctx, []diff.TableDelta{delta}, map[string]uint64{"t": 3})
						require.NoError(t, err)
						tm, err := createTableMapFromDoltTable(ctx, "db", "t", tc.to, false)
						require.NoError(t, err)
						require.Len(t, tm.Types, 5, "functional-index column must stay hidden")
						var gotOld, gotNew [][]string
						for _, event := range events {
							event, _, err = event.StripChecksum(format)
							require.NoError(t, err)
							rows, err := event.Rows(format, tm)
							require.NoError(t, err)
							require.Len(t, rows.Rows, 1)
							if event.IsWriteRows() || event.IsUpdateRows() {
								require.Equal(t, 5, rows.DataColumns.BitCount())
								values, err := rows.StringValuesForTests(tm, 0)
								require.NoError(t, err)
								gotNew = append(gotNew, values)
							}
							if event.IsDeleteRows() || event.IsUpdateRows() {
								require.Equal(t, 5, rows.IdentifyColumns.BitCount())
								values, err := rows.StringIdentifiesForTests(tm, 0)
								require.NoError(t, err)
								gotOld = append(gotOld, values)
							}
						}
						if tc.old == nil {
							require.Empty(t, gotOld)
						} else {
							require.Equal(t, [][]string{tc.old}, gotOld)
						}
						if tc.new == nil {
							require.Empty(t, gotNew)
						} else {
							require.Equal(t, [][]string{tc.new}, gotNew)
						}
					})
				}
			})
		}
	}
}

func binlogTestTable(t *testing.T, ctx *sql.Context, sch schema.Schema, rows []sql.Row) *doltdb.Table {
	t.Helper()
	ns := tree.NewTestNodeStore()
	vrw := types.NewMemoryValueStore()
	t.Cleanup(func() { require.NoError(t, vrw.Close()) })
	kd, vd := sch.GetKeyDescriptor(ns), sch.GetValueDescriptor(ns)
	var tuples []val.Tuple
	for _, row := range rows {
		kb, vb := val.NewTupleBuilder(kd, ns), val.NewTupleBuilder(vd, ns)
		if schema.IsKeyless(sch) {
			vb.PutUint64(0, 1)
		}
		for i, col := range sch.GetAllCols().GetColumns() {
			if col.Virtual {
				continue
			}
			builder := vb
			idx, _ := sch.GetNonPKCols().StoredIndexByTag(col.Tag)
			if schema.IsKeyless(sch) {
				idx++
			}
			if col.IsPartOfPK {
				builder = kb
				idx, _ = sch.GetPKCols().StoredIndexByTag(col.Tag)
			}
			require.NoError(t, tree.PutField(ctx, ns, builder, idx, row[i]))
		}
		v, err := vb.Build(ctx, ns.Pool())
		require.NoError(t, err)
		var k val.Tuple
		if schema.IsKeyless(sch) {
			k = val.HashTupleFromValue(ns.Pool(), v)
		} else {
			k, err = kb.Build(ctx, ns.Pool())
			require.NoError(t, err)
		}
		tuples = append(tuples, k, v)
	}
	m, err := prolly.NewMapFromTuples(ctx, ns, kd, vd, tuples...)
	require.NoError(t, err)
	table, err := doltdb.NewTable(ctx, vrw, ns, sch, durable.IndexFromProllyMap(m), nil, nil)
	require.NoError(t, err)
	return table
}

func TestBinlogFullGeneratedColumnTypes(t *testing.T) {
	ctx := sql.NewEmptyContext()
	cols := make([]schema.Column, 10)
	names := []string{"id", "txt", "txt_len", "hidden_expr", "b", "b_len", "j", "j_num", "copy_num", "null_value"}
	for i, name := range names {
		cols[i] = schema.NewColumn(name, uint64(i), types.IntKind, i == 0)
	}
	cols[1].TypeInfo, cols[1].Kind = typeinfo.TextType, types.BlobKind
	blobType, err := typeinfo.FromSqlType(gmstypes.Blob)
	require.NoError(t, err)
	cols[4].TypeInfo, cols[4].Kind = blobType, types.BlobKind
	cols[6].TypeInfo, cols[6].Kind = typeinfo.JSONType, types.JSONKind
	cols[2].Generated, cols[2].Virtual = "length(txt)", true
	cols[3].Generated, cols[3].Virtual, cols[3].SystemHidden = "id + 1", true, true
	cols[5].Generated, cols[5].Virtual = "length(b)", true
	cols[7].Generated, cols[7].Virtual = "json_extract(j, '$.n') + 1", true
	cols[8].Generated, cols[8].Virtual = "j_num + 1", true
	cols[9].Generated, cols[9].Virtual = "nullif(id, 1)", true
	sch := schema.MustSchemaFromCols(schema.NewColCollection(cols...))
	empty := binlogTestTable(t, ctx, sch, nil)
	table := binlogTestTable(t, ctx, sch, []sql.Row{{int64(1), strings.Repeat("x", 4000), nil, nil, []byte(strings.Repeat("y", 4000)), nil, gmstypes.JSONDocument{Val: map[string]interface{}{"n": float64(41)}}, nil, nil, nil}})
	format := mysql.NewMySQL56BinlogFormat()
	producer := &binlogProducer{binlogFormat: &format, binlogEventMeta: mysql.BinlogEventMetadata{ServerID: 1}}
	events, err := producer.createRowEvents(ctx, []diff.TableDelta{{FromName: doltdb.TableName{Name: "t"}, ToName: doltdb.TableName{Name: "t"}, FromTable: empty, ToTable: table, FromSch: sch, ToSch: sch}}, map[string]uint64{"t": 3})
	require.NoError(t, err)
	require.Len(t, events, 1)
	tm, err := createTableMapFromDoltTable(ctx, "db", "t", table, false)
	require.NoError(t, err)
	event, _, err := events[0].StripChecksum(format)
	require.NoError(t, err)
	rows, err := event.Rows(format, tm)
	require.NoError(t, err)
	require.Equal(t, 9, rows.DataColumns.BitCount())
	require.True(t, rows.Rows[0].NullColumns.Bit(8), "NULL bitmap must span all visible columns, including virtual columns")
	values, err := rows.StringValuesForTests(tm, 0)
	require.NoError(t, err)
	require.Len(t, values, 9)
	require.Equal(t, "4000", values[2])
	require.Equal(t, "4000", values[4])
	require.Equal(t, "42", values[6])
	require.Equal(t, "43", values[7])
	require.Equal(t, "NULL", values[8])
}

func TestBinlogFullGeneratedColumnTime(t *testing.T) {
	ctx := sql.NewEmptyContext()
	cols := []schema.Column{
		schema.NewColumn("id", 0, types.IntKind, true),
		schema.NewColumn("t", 1, types.IntKind, false),
		schema.NewColumn("g", 2, types.IntKind, false),
		schema.NewColumn("hours", 3, types.IntKind, false),
	}
	timeType, err := typeinfo.FromSqlType(gmstypes.TimeMaxPrecision)
	require.NoError(t, err)
	cols[1].TypeInfo, cols[2].TypeInfo = timeType, timeType
	cols[2].Generated, cols[2].Virtual = "t", true
	cols[3].Generated, cols[3].Virtual = "hour(t)", true
	sch := schema.MustSchemaFromCols(schema.NewColCollection(cols...))
	for _, duration := range []string{"27:01:02.123456", "-27:01:02.123456"} {
		t.Run(duration, func(t *testing.T) {
			v, _, err := gmstypes.TimeMaxPrecision.Convert(ctx, duration)
			require.NoError(t, err)
			empty := binlogTestTable(t, ctx, sch, nil)
			table := binlogTestTable(t, ctx, sch, []sql.Row{{int64(1), v, nil, nil}})
			format := mysql.NewMySQL56BinlogFormat()
			producer := &binlogProducer{binlogFormat: &format, binlogEventMeta: mysql.BinlogEventMetadata{ServerID: 1}}
			events, err := producer.createRowEvents(ctx, []diff.TableDelta{{FromName: doltdb.TableName{Name: "t"}, ToName: doltdb.TableName{Name: "t"}, FromTable: empty, ToTable: table, FromSch: sch, ToSch: sch}}, map[string]uint64{"t": 3})
			require.NoError(t, err)
			require.Len(t, events, 1)
			tm, err := createTableMapFromDoltTable(ctx, "db", "t", table, false)
			require.NoError(t, err)
			event, _, err := events[0].StripChecksum(format)
			require.NoError(t, err)
			rows, err := event.Rows(format, tm)
			require.NoError(t, err)
			values, err := rows.StringValuesForTests(tm, 0)
			require.NoError(t, err)
			require.Equal(t, []string{"1", duration, duration, "27"}, values)
		})
	}
}
