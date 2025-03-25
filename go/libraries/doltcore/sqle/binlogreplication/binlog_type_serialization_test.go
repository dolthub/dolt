// Copyright 2024 Dolthub, Inc.
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
	bytes2 "bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

var varchar20 = gmstypes.MustCreateString(sqltypes.VarChar, 5, sql.Collation_Default)
var varchar255 = gmstypes.MustCreateString(sqltypes.VarChar, 255, sql.Collation_Default)
var buffPool = pool.NewBuffPool()

func TestStringSerializer(t *testing.T) {
	ns := tree.NewTestNodeStore()
	s := stringSerializer{}

	tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.StringEnc})
	tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)

	t.Run("VARCHAR 1 byte length encoding", func(t *testing.T) {
		tupleBuilder.PutString(0, "abc")
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, varchar20, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{3, 'a', 'b', 'c'}, bytes)
		typeId, metadata := s.metadata(nil, varchar255)
		require.EqualValues(t, mysql.TypeVarchar, typeId)
		require.EqualValues(t, 255*4, metadata)
	})
	t.Run("VARCHAR 2 byte length encoding", func(t *testing.T) {
		tupleBuilder.PutString(0, "abc")
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, varchar255, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{3, 0, 'a', 'b', 'c'}, bytes)
		typeId, metadata := s.metadata(nil, varchar255)
		require.EqualValues(t, mysql.TypeVarchar, typeId)
		require.EqualValues(t, 255*4, metadata)
	})
	t.Run("CHAR 1 byte length encoding", func(t *testing.T) {
		typ := gmstypes.MustCreateString(sqltypes.Char, 25, sql.Collation_Default)
		tupleBuilder.PutString(0, "abc")
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x03, 'a', 'b', 'c'}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeString, typeId)
		require.EqualValues(t, ((mysql.TypeString<<8)^0x00)|0x64, metadata)
	})
	t.Run("CHAR 2 byte length encoding", func(t *testing.T) {
		typ := gmstypes.MustCreateString(sqltypes.Char, 100, sql.Collation_Default)
		tupleBuilder.PutString(0, "abc")
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x03, 0x00, 'a', 'b', 'c'}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeString, typeId)
		require.EqualValues(t, ((mysql.TypeString<<8)^(0x01<<12))|0x90, metadata)
	})
	t.Run("VARBINARY 1 byte length encoding", func(t *testing.T) {
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.ByteStringEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		typ := gmstypes.MustCreateString(sqltypes.VarBinary, 50, sql.Collation_binary)
		tupleBuilder.PutByteString(0, []byte{'a', 'b', 'c'})
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x03, 'a', 'b', 'c'}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeVarchar, typeId)
		require.EqualValues(t, 50, metadata)
	})
	t.Run("VARBINARY 2 byte length encoding", func(t *testing.T) {
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.ByteStringEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		typ := gmstypes.MustCreateString(sqltypes.VarBinary, 420, sql.Collation_binary)
		tupleBuilder.PutByteString(0, []byte{'a', 'b', 'c'})
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x03, 0x00, 'a', 'b', 'c'}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeVarchar, typeId)
		require.EqualValues(t, 420, metadata)
	})
	t.Run("BINARY 1 byte length encoding", func(t *testing.T) {
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.ByteStringEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		typ := gmstypes.MustCreateString(sqltypes.Binary, 25, sql.Collation_binary)
		tupleBuilder.PutByteString(0, []byte{'a', 'b', 'c'})
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x03, 'a', 'b', 'c'}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeString, typeId)
		require.EqualValues(t, ((mysql.TypeString<<8)^0x00)|0x19, metadata)
	})
	// NOTE: There is no 2 byte encoding for BINARY, since the max size of a BINARY
	//       field is 255 bytes, and that's not large enough to need 2 bytes.
}

func TestFloatSerializer_Float32(t *testing.T) {
	ns := tree.NewTestNodeStore()
	s := floatSerializer{}

	// 3.1415927E+00 = 0x40490fdb
	tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.Float32Enc})
	tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
	tupleBuilder.PutFloat32(0, 3.1415927)
	tuple, err := tupleBuilder.Build(buffPool)
	require.NoError(t, err)
	bytes, err := s.serialize(nil, gmstypes.Float32, tupleDesc, tuple, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []byte{0xdb, 0x0f, 0x49, 0x40}, bytes)
	typeId, metadata := s.metadata(nil, gmstypes.Float32)
	require.EqualValues(t, mysql.TypeFloat, typeId)
	require.EqualValues(t, 4, metadata)
}

func TestFloatSerializer_Float64(t *testing.T) {
	ns := tree.NewTestNodeStore()
	s := floatSerializer{}

	// 3.1415926535E+00 = 0x400921fb54411744
	tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.Float64Enc})
	tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
	tupleBuilder.PutFloat64(0, 3.1415926535)
	tuple, err := tupleBuilder.Build(buffPool)
	require.NoError(t, err)
	bytes, err := s.serialize(nil, gmstypes.Float64, tupleDesc, tuple, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []byte{0x44, 0x17, 0x41, 0x54, 0xfb, 0x21, 0x09, 0x40}, bytes)
	typeId, metadata := s.metadata(nil, gmstypes.Float64)
	require.EqualValues(t, mysql.TypeDouble, typeId)
	require.EqualValues(t, 8, metadata)
}

func TestYearSerializer(t *testing.T) {
	ns := tree.NewTestNodeStore()
	s := yearSerializer{}

	tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.YearEnc})
	tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
	tupleBuilder.PutYear(0, 2030)
	tuple, err := tupleBuilder.Build(buffPool)
	require.NoError(t, err)
	bytes, err := s.serialize(nil, gmstypes.Year, tupleDesc, tuple, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []byte{0x82}, bytes)
	typeId, metadata := s.metadata(nil, gmstypes.Year)
	require.EqualValues(t, mysql.TypeYear, typeId)
	require.EqualValues(t, 0, metadata)
}

func TestDatetimeSerializer(t *testing.T) {
	ns := tree.NewTestNodeStore()
	s := datetimeSerializer{}

	t.Run("No Precision", func(t *testing.T) {
		// 2012-06-21 15:45:17 (precision 0)
		datetimeType := gmstypes.Datetime
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DatetimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutDatetime(0,
			time.Date(2012, 6, 21, 15, 45, 17, 0, time.UTC))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, datetimeType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x99, 0x8c, 0xaa, 0xfb, 0x51}, bytes)
		typeId, metadata := s.metadata(nil, datetimeType)
		require.EqualValues(t, mysql.TypeDateTime2, typeId)
		require.EqualValues(t, 0, metadata)
	})
	t.Run("1 Digit Precision", func(t *testing.T) {
		// 2012-06-21 15:45:17.7 (precision 1)
		datetimeType := gmstypes.MustCreateDatetimeType(sqltypes.Datetime, 1)
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DatetimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutDatetime(0,
			time.Date(2012, 6, 21, 15, 45, 17, .7*1_000_000_000, time.UTC))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, datetimeType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x99, 0x8c, 0xaa, 0xfb, 0x51, 70}, bytes)
		typeId, metadata := s.metadata(nil, datetimeType)
		require.EqualValues(t, mysql.TypeDateTime2, typeId)
		require.EqualValues(t, 1, metadata)
	})
	t.Run("2 Digit Precision", func(t *testing.T) {
		// 2012-06-21 15:45:17.76 (precision 2)
		datetimeType := gmstypes.MustCreateDatetimeType(sqltypes.Datetime, 2)
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DatetimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutDatetime(0,
			time.Date(2012, 6, 21, 15, 45, 17, .76*1_000_000_000, time.UTC))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, datetimeType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x99, 0x8c, 0xaa, 0xfb, 0x51, 76}, bytes)
		typeId, metadata := s.metadata(nil, datetimeType)
		require.EqualValues(t, mysql.TypeDateTime2, typeId)
		require.EqualValues(t, 2, metadata)
	})
	t.Run("3 Digit Precision", func(t *testing.T) {
		// 2012-06-21 15:45:17.765 (precision 3)
		datetimeType := gmstypes.MustCreateDatetimeType(sqltypes.Datetime, 3)
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DatetimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutDatetime(0,
			time.Date(2012, 6, 21, 15, 45, 17, .765*1_000_000_000, time.UTC))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, datetimeType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x99, 0x8c, 0xaa, 0xfb, 0x51, 0x1d, 0xe2}, bytes)
		typeId, metadata := s.metadata(nil, datetimeType)
		require.EqualValues(t, mysql.TypeDateTime2, typeId)
		require.EqualValues(t, 3, metadata)
	})
	t.Run("4 Digit Precision", func(t *testing.T) {
		// 2012-06-21 15:45:17.7654 (precision 4)
		datetimeType := gmstypes.MustCreateDatetimeType(sqltypes.Datetime, 4)
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DatetimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutDatetime(0,
			time.Date(2012, 6, 21, 15, 45, 17, .7654*1_000_000_000, time.UTC))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, datetimeType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x99, 0x8c, 0xaa, 0xfb, 0x51, 0x1d, 0xe6}, bytes)
		typeId, metadata := s.metadata(nil, datetimeType)
		require.EqualValues(t, mysql.TypeDateTime2, typeId)
		require.EqualValues(t, 4, metadata)
	})
	t.Run("5 Digit Precision", func(t *testing.T) {
		// 2012-06-21 15:45:17.76543 (precision 5)
		datetimeType := gmstypes.MustCreateDatetimeType(sqltypes.Datetime, 5)
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DatetimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutDatetime(0,
			time.Date(2012, 6, 21, 15, 45, 17, .76543*1_000_000_000, time.UTC))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, datetimeType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x99, 0x8c, 0xaa, 0xfb, 0x51, 0x0b, 0xad, 0xf6}, bytes)
		typeId, metadata := s.metadata(nil, datetimeType)
		require.EqualValues(t, mysql.TypeDateTime2, typeId)
		require.EqualValues(t, 5, metadata)
	})
	t.Run("6 Digit Precision", func(t *testing.T) {
		// 2012-06-21 15:45:17.765432 (precision 6)
		datetimeType := gmstypes.MustCreateDatetimeType(sqltypes.Datetime, 6)
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DatetimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutDatetime(0,
			time.Date(2012, 6, 21, 15, 45, 17, .765432*1_000_000_000, time.UTC))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, datetimeType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x99, 0x8c, 0xaa, 0xfb, 0x51, 0x0b, 0xad, 0xf8}, bytes)
		typeId, metadata := s.metadata(nil, datetimeType)
		require.EqualValues(t, mysql.TypeDateTime2, typeId)
		require.EqualValues(t, 6, metadata)
	})
}

func TestTimestampSerializer(t *testing.T) {
	ns := tree.NewTestNodeStore()
	s := timestampSerializer{}

	t.Run("No Precision", func(t *testing.T) {
		// 2017-03-21 14:25:09
		timestampType := gmstypes.MustCreateDatetimeType(sqltypes.Timestamp, 0)
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DatetimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutDatetime(0,
			time.Date(2017, 03, 21, 14, 25, 9, 0.0*1_000_000_000, time.UTC))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, timestampType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x58, 0xd1, 0x37, 0xc5}, bytes)
		typeId, metadata := s.metadata(nil, timestampType)
		require.EqualValues(t, mysql.TypeTimestamp2, typeId)
		require.EqualValues(t, 0, metadata)
	})
	t.Run("1 Digit Precision", func(t *testing.T) {
		// 2017-03-21 14:25:09.7
		timestampType := gmstypes.MustCreateDatetimeType(sqltypes.Timestamp, 1)
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DatetimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutDatetime(0,
			time.Date(2017, 03, 21, 14, 25, 9, 0.7*1_000_000_000, time.UTC))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, timestampType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x58, 0xd1, 0x37, 0xc5, 70}, bytes)
		typeId, metadata := s.metadata(nil, timestampType)
		require.EqualValues(t, mysql.TypeTimestamp2, typeId)
		require.EqualValues(t, 1, metadata)
	})
	t.Run("2 Digit Precision", func(t *testing.T) {
		// 2017-03-21 14:25:09.76
		timestampType := gmstypes.MustCreateDatetimeType(sqltypes.Timestamp, 2)
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DatetimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutDatetime(0,
			time.Date(2017, 03, 21, 14, 25, 9, 0.76*1_000_000_000, time.UTC))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, timestampType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x58, 0xd1, 0x37, 0xc5, 76}, bytes)
		typeId, metadata := s.metadata(nil, timestampType)
		require.EqualValues(t, mysql.TypeTimestamp2, typeId)
		require.EqualValues(t, 2, metadata)
	})
	t.Run("3 Digit Precision", func(t *testing.T) {
		// 2017-03-21 14:25:09.765
		timestampType := gmstypes.MustCreateDatetimeType(sqltypes.Timestamp, 3)
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DatetimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutDatetime(0,
			time.Date(2017, 03, 21, 14, 25, 9, 0.765*1_000_000_000, time.UTC))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, timestampType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x58, 0xd1, 0x37, 0xc5, 0x1d, 0xe2}, bytes)
		typeId, metadata := s.metadata(nil, timestampType)
		require.EqualValues(t, mysql.TypeTimestamp2, typeId)
		require.EqualValues(t, 3, metadata)
	})
	t.Run("4 Digit Precision", func(t *testing.T) {
		// 2017-03-21 14:25:09.7654
		timestampType := gmstypes.MustCreateDatetimeType(sqltypes.Timestamp, 4)
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DatetimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutDatetime(0,
			time.Date(2017, 03, 21, 14, 25, 9, 0.7654*1_000_000_000, time.UTC))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, timestampType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x58, 0xd1, 0x37, 0xc5, 0x1d, 0xe6}, bytes)
		typeId, metadata := s.metadata(nil, timestampType)
		require.EqualValues(t, mysql.TypeTimestamp2, typeId)
		require.EqualValues(t, 4, metadata)
	})
	t.Run("5 Digit Precision", func(t *testing.T) {
		// 2017-03-21 14:25:09.76543
		timestampType := gmstypes.MustCreateDatetimeType(sqltypes.Timestamp, 5)
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DatetimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutDatetime(0,
			time.Date(2017, 03, 21, 14, 25, 9, 0.76543*1_000_000_000, time.UTC))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, timestampType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x58, 0xd1, 0x37, 0xc5, 0x0b, 0xad, 0xf6}, bytes)
		typeId, metadata := s.metadata(nil, timestampType)
		require.EqualValues(t, mysql.TypeTimestamp2, typeId)
		require.EqualValues(t, 5, metadata)
	})
	t.Run("6 Digit Precision", func(t *testing.T) {
		// 2017-03-21 14:25:09.765432
		timestampType := gmstypes.MustCreateDatetimeType(sqltypes.Timestamp, 6)
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DatetimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutDatetime(0,
			time.Date(2017, 03, 21, 14, 25, 9, 0.765432*1_000_000_000, time.UTC))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, timestampType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x58, 0xd1, 0x37, 0xc5, 0x0b, 0xad, 0xf8}, bytes)
		typeId, metadata := s.metadata(nil, timestampType)
		require.EqualValues(t, mysql.TypeTimestamp2, typeId)
		require.EqualValues(t, 6, metadata)
	})
}

func TestDateSerializer(t *testing.T) {
	ns := tree.NewTestNodeStore()
	s := dateSerializer{}

	// 2010-10-03
	tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.DateEnc})
	tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
	tupleBuilder.PutDate(0,
		time.Date(2010, 10, 03, 0, 0, 0, 0.0*1_000_000_000, time.UTC))
	tuple, err := tupleBuilder.Build(buffPool)
	require.NoError(t, err)
	bytes, err := s.serialize(nil, gmstypes.Date, tupleDesc, tuple, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []byte{0x43, 0xb5, 0x0f}, bytes)
	typeId, metadata := s.metadata(nil, gmstypes.Date)
	require.EqualValues(t, mysql.TypeDate, typeId)
	require.EqualValues(t, 0, metadata)
}

func TestTimeSerializer(t *testing.T) {
	ns := tree.NewTestNodeStore()
	s := timeSerializer{}

	t.Run("6 Digit Precision: 00:00:00", func(t *testing.T) {
		typ := gmstypes.Time
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.TimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutSqlTime(0, (0 * time.Second).Microseconds())
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeTime2, typeId)
		require.EqualValues(t, 6, metadata)
	})
	t.Run("6 Digit Precision: -00:00:00.000001", func(t *testing.T) {
		typ := gmstypes.Time
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.TimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutSqlTime(0, (-1 * time.Microsecond).Microseconds())
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeTime2, typeId)
		require.EqualValues(t, 6, metadata)
	})
	t.Run("6 Digit Precision: -00:00:00.000099", func(t *testing.T) {
		typ := gmstypes.Time
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.TimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutSqlTime(0, (-99 * time.Microsecond).Microseconds())
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0x9d}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeTime2, typeId)
		require.EqualValues(t, 6, metadata)
	})
	t.Run("6 Digit Precision: -00:00:01.000000", func(t *testing.T) {
		typ := gmstypes.Time
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.TimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutSqlTime(0, -1*(time.Second).Microseconds())
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x7f, 0xff, 0xff, 0x00, 0x00, 0x00}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeTime2, typeId)
		require.EqualValues(t, 6, metadata)
	})
	t.Run("6 Digit Precision: -00:00:01.000001", func(t *testing.T) {
		typ := gmstypes.Time
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.TimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutSqlTime(0, -1*(time.Second+time.Microsecond).Microseconds())
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x7f, 0xff, 0xfe, 0xff, 0xff, 0xff}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeTime2, typeId)
		require.EqualValues(t, 6, metadata)
	})
	t.Run("6 Digit Precision: -00:00:01.000010", func(t *testing.T) {
		typ := gmstypes.Time
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.TimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutSqlTime(0, -1*(time.Second+10*time.Microsecond).Microseconds())
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x7f, 0xff, 0xfe, 0xff, 0xff, 0xf6}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeTime2, typeId)
		require.EqualValues(t, 6, metadata)
	})
	t.Run("6 Digit Precision: 15:34:54.000000", func(t *testing.T) {
		typ := gmstypes.Time
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.TimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutSqlTime(0, (15*time.Hour + 34*time.Minute + 54*time.Second).Microseconds())
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x80, 0xf8, 0xb6, 0x00, 0x00, 0x00}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeTime2, typeId)
		require.EqualValues(t, 6, metadata)
	})
	t.Run("6 Digit Precision: 00:00:01.100000", func(t *testing.T) {
		typ := gmstypes.Time
		tupleDesc := val.NewTupleDescriptor(val.Type{Enc: val.TimeEnc})
		tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
		tupleBuilder.PutSqlTime(0, (time.Second + 100*time.Millisecond).Microseconds())
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x80, 0x0, 0x1, 0x1, 0x86, 0xa0}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeTime2, typeId)
		require.EqualValues(t, 6, metadata)
	})
}

func TestIntegerSerializer(t *testing.T) {
	s := integerSerializer{}

	t.Run("INT8", func(t *testing.T) {
		typ := gmstypes.Int8
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.Int8Enc)
		tupleBuilder.PutInt8(0, -2)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0xfe}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeTiny, typeId)
		require.EqualValues(t, 0, metadata)
	})
	t.Run("UINT8", func(t *testing.T) {
		typ := gmstypes.Uint8
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.Uint8Enc)
		tupleBuilder.PutUint8(0, 130)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x82}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeTiny, typeId)
		require.EqualValues(t, 0, metadata)
	})
	t.Run("INT16", func(t *testing.T) {
		typ := gmstypes.Int16
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.Int16Enc)
		tupleBuilder.PutInt16(0, int16(-2))
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0xfe, 0xff}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeShort, typeId)
		require.EqualValues(t, 0, metadata)
	})
	t.Run("UINT16", func(t *testing.T) {
		typ := gmstypes.Uint16
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.Uint16Enc)
		tupleBuilder.PutUint16(0, 0x8182)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x82, 0x81}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeShort, typeId)
		require.EqualValues(t, 0, metadata)
	})
	t.Run("INT24", func(t *testing.T) {
		typ := gmstypes.Int24
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.Int32Enc)
		tupleBuilder.PutInt32(0, -259)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0xfd, 0xfe, 0xff}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeInt24, typeId)
		require.EqualValues(t, 0, metadata)
	})
	t.Run("UINT24", func(t *testing.T) {
		typ := gmstypes.Uint24
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.Uint32Enc)
		tupleBuilder.PutUint32(0, 0x818283)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x83, 0x82, 0x81}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeInt24, typeId)
		require.EqualValues(t, 0, metadata)
	})
	t.Run("INT32", func(t *testing.T) {
		typ := gmstypes.Int32
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.Int32Enc)
		tupleBuilder.PutInt32(0, -66052)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0xfc, 0xfd, 0xfe, 0xff}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeLong, typeId)
		require.EqualValues(t, 0, metadata)
	})
	t.Run("UINT32", func(t *testing.T) {
		typ := gmstypes.Uint32
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.Uint32Enc)
		tupleBuilder.PutUint32(0, 0x81828384)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x84, 0x83, 0x82, 0x81}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeLong, typeId)
		require.EqualValues(t, 0, metadata)
	})
	t.Run("INT64", func(t *testing.T) {
		typ := gmstypes.Int64
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.Int64Enc)
		tupleBuilder.PutInt64(0, -283686952306184)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeLongLong, typeId)
		require.EqualValues(t, 0, metadata)
	})
	t.Run("UINT64", func(t *testing.T) {
		typ := gmstypes.Uint64
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.Uint64Enc)
		tupleBuilder.PutUint64(0, 0x8182838485868788)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x88, 0x87, 0x86, 0x85, 0x84, 0x83, 0x82, 0x81}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeLongLong, typeId)
		require.EqualValues(t, 0, metadata)
	})
}

func TestDecimalSerializer(t *testing.T) {
	s := decimalSerializer{}

	t.Run("0", func(t *testing.T) {
		typ := gmstypes.MustCreateDecimalType(14, 4)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.DecimalEnc)
		dec, err := decimal.NewFromString("0")
		require.NoError(t, err)
		tupleBuilder.PutDecimal(0, dec)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeNewDecimal, typeId)
		require.EqualValues(t, 14<<8|4, metadata)
	})
	t.Run("100", func(t *testing.T) {
		typ := gmstypes.MustCreateDecimalType(14, 4)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.DecimalEnc)
		dec, err := decimal.NewFromString("100")
		require.NoError(t, err)
		tupleBuilder.PutDecimal(0, dec)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x80, 0x0, 0x0, 0x0, 0x64, 0x0, 0x0}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeNewDecimal, typeId)
		require.EqualValues(t, 14<<8|4, metadata)
	})
	t.Run("1.1", func(t *testing.T) {
		typ := gmstypes.MustCreateDecimalType(14, 4)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.DecimalEnc)
		dec, err := decimal.NewFromString("1.1")
		require.NoError(t, err)
		tupleBuilder.PutDecimal(0, dec)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x80, 0x0, 0x0, 0x0, 0x1, 0x3, 0xe8}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeNewDecimal, typeId)
		require.EqualValues(t, 14<<8|4, metadata)
	})
	t.Run("10", func(t *testing.T) {
		typ := gmstypes.MustCreateDecimalType(19, 0)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.DecimalEnc)
		dec, err := decimal.NewFromString("100")
		require.NoError(t, err)
		tupleBuilder.PutDecimal(0, dec)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeNewDecimal, typeId)
		require.EqualValues(t, 19<<8|0, metadata)
	})
	t.Run("1234567890.1234", func(t *testing.T) {
		typ := gmstypes.MustCreateDecimalType(14, 4)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.DecimalEnc)
		dec, err := decimal.NewFromString("1234567890.1234")
		require.NoError(t, err)
		tupleBuilder.PutDecimal(0, dec)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x81, 0x0D, 0xFB, 0x38, 0xD2, 0x04, 0xD2}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeNewDecimal, typeId)
		require.EqualValues(t, 14<<8|4, metadata)
	})
	t.Run("-1234567890.1234", func(t *testing.T) {
		typ := gmstypes.MustCreateDecimalType(14, 4)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.DecimalEnc)
		dec, err := decimal.NewFromString("-1234567890.1234")
		require.NoError(t, err)
		tupleBuilder.PutDecimal(0, dec)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x7E, 0xF2, 0x04, 0xC7, 0x2D, 0xFB, 0x2D}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeNewDecimal, typeId)
		require.EqualValues(t, 14<<8|4, metadata)
	})
	t.Run("1234567890.0001", func(t *testing.T) {
		typ := gmstypes.MustCreateDecimalType(14, 4)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.DecimalEnc)
		dec, err := decimal.NewFromString("1234567890.0001")
		require.NoError(t, err)
		tupleBuilder.PutDecimal(0, dec)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x81, 0x0D, 0xFB, 0x38, 0xD2, 0x00, 0x01}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeNewDecimal, typeId)
		require.EqualValues(t, 14<<8|4, metadata)
	})
}

func TestBitSerializer(t *testing.T) {
	s := bitSerializer{}

	typ := gmstypes.MustCreateBitType(15)
	tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.Uint64Enc)
	tupleBuilder.PutUint64(0, 0x0301)
	tuple, err := tupleBuilder.Build(buffPool)
	require.NoError(t, err)
	bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []byte{0x03, 0x01}, bytes)
	typeId, metadata := s.metadata(nil, typ)
	require.EqualValues(t, mysql.TypeBit, typeId)
	require.EqualValues(t, 0x0107, metadata)
}

func TestEnumSerializer(t *testing.T) {
	s := enumSerializer{}

	t.Run("Less than 255 members", func(t *testing.T) {
		typ := gmstypes.MustCreateEnumType([]string{"red", "green", "blue"}, sql.Collation_Default)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.EnumEnc)
		tupleBuilder.PutEnum(0, 0x03)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x03}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeString, typeId)
		require.EqualValues(t, mysql.TypeEnum<<8|0x01, metadata)
	})
	t.Run("More than 255 members", func(t *testing.T) {
		typ := gmstypes.MustCreateEnumType(createTestStringSlice(267), sql.Collation_Default)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.EnumEnc)
		tupleBuilder.PutEnum(0, 0x0102)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x02, 0x01}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeString, typeId)
		require.EqualValues(t, mysql.TypeEnum<<8|0x02, metadata)
	})
}

func TestSetSerializer(t *testing.T) {
	s := setSerializer{}

	typ := gmstypes.MustCreateSetType(createTestStringSlice(12), sql.Collation_Default)
	tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.SetEnc)
	tupleBuilder.PutSet(0, 0x0102)
	tuple, err := tupleBuilder.Build(buffPool)
	require.NoError(t, err)
	bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []byte{0x02, 0x01}, bytes)
	typeId, metadata := s.metadata(nil, typ)
	require.EqualValues(t, mysql.TypeString, typeId)
	require.EqualValues(t, mysql.TypeSet<<8|0x02, metadata)
}

func TestBlobSerializer(t *testing.T) {
	s := blobSerializer{}

	t.Run("TINYBLOB", func(t *testing.T) {
		typ := gmstypes.TinyBlob
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.BytesAddrEnc)
		ns, addr := createTestBlob(t, []byte(`abc`))
		tupleBuilder.PutBytesAddr(0, addr)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{0x03, 'a', 'b', 'c'}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeBlob, typeId)
		require.EqualValues(t, 0x01, metadata)
	})
	t.Run("BLOB", func(t *testing.T) {
		typ := gmstypes.Blob
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.BytesAddrEnc)
		ns, addr := createTestBlob(t, []byte(`abc`))
		tupleBuilder.PutBytesAddr(0, addr)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{0x03, 0x00, 'a', 'b', 'c'}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeBlob, typeId)
		require.EqualValues(t, 0x02, metadata)
	})
	t.Run("MEDIUMBLOB", func(t *testing.T) {
		typ := gmstypes.MediumBlob
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.BytesAddrEnc)
		ns, addr := createTestBlob(t, []byte(`abc`))
		tupleBuilder.PutBytesAddr(0, addr)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{0x03, 0x00, 0x00, 'a', 'b', 'c'}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeBlob, typeId)
		require.EqualValues(t, 0x03, metadata)
	})
	t.Run("LONGBLOB", func(t *testing.T) {
		typ := gmstypes.LongBlob
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.BytesAddrEnc)
		ns, addr := createTestBlob(t, []byte(`abc`))
		tupleBuilder.PutBytesAddr(0, addr)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{0x03, 0x00, 0x00, 0x00, 'a', 'b', 'c'}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeBlob, typeId)
		require.EqualValues(t, 0x04, metadata)
	})
}

func TestJsonSerializer(t *testing.T) {
	s := jsonSerializer{}

	typ := gmstypes.JSON
	tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.JSONAddrEnc)
	ns, addr := createTestBlob(t, []byte(`{"a":"b"}`))
	tupleBuilder.PutJSONAddr(0, addr)
	tuple, err := tupleBuilder.Build(buffPool)
	require.NoError(t, err)
	bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, ns)
	require.NoError(t, err)
	require.Equal(t, []byte{0x0f, 0x00, 0x00, 0x00,
		0, 1, 0, 14, 0, 11, 0, 1, 0, 12, 12, 0, 97, 1, 98}, bytes)
	typeId, metadata := s.metadata(nil, typ)
	require.EqualValues(t, mysql.TypeJSON, typeId)
	require.EqualValues(t, 0x04, metadata)
}

func TestTextSerializer(t *testing.T) {
	s := textSerializer{}

	t.Run("TINYTEXT", func(t *testing.T) {
		typ := gmstypes.TinyText
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.StringAddrEnc)
		ns, addr := createTestBlob(t, []byte("abcde"))
		tupleBuilder.PutStringAddr(0, addr)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{0x05, 'a', 'b', 'c', 'd', 'e'}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeBlob, typeId)
		require.EqualValues(t, 0x01, metadata)
	})
	t.Run("TEXT", func(t *testing.T) {
		typ := gmstypes.Text
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.StringAddrEnc)
		ns, addr := createTestBlob(t, []byte("abcde"))
		tupleBuilder.PutStringAddr(0, addr)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{0x05, 0x00, 'a', 'b', 'c', 'd', 'e'}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeBlob, typeId)
		require.EqualValues(t, 0x02, metadata)
	})
	t.Run("MEDIUMTEXT", func(t *testing.T) {
		typ := gmstypes.MediumText
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.StringAddrEnc)
		ns, addr := createTestBlob(t, []byte("abcde"))
		tupleBuilder.PutStringAddr(0, addr)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{0x05, 0x00, 0x00, 'a', 'b', 'c', 'd', 'e'}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeBlob, typeId)
		require.EqualValues(t, 0x03, metadata)
	})
	t.Run("LONGTEXT", func(t *testing.T) {
		typ := gmstypes.LongText
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.StringAddrEnc)
		ns, addr := createTestBlob(t, []byte("abcde"))
		tupleBuilder.PutStringAddr(0, addr)
		tuple, err := tupleBuilder.Build(buffPool)
		require.NoError(t, err)
		bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{0x05, 0x00, 0x00, 0x00, 'a', 'b', 'c', 'd', 'e'}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeBlob, typeId)
		require.EqualValues(t, 0x04, metadata)
	})
}

func TestGeometrySerializer(t *testing.T) {
	s := geometrySerializer{}

	typ := typeinfo.GeometryType.ToSqlType()
	tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.GeomAddrEnc)
	ns, addr := createTestBlob(t, []byte{
		0x00, 0x00, 0x00, 0x00, // SRID
		0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xBF})
	tupleBuilder.PutGeometryAddr(0, addr)
	tuple, err := tupleBuilder.Build(buffPool)
	require.NoError(t, err)
	bytes, err := s.serialize(nil, typ, tupleDesc, tuple, 0, ns)
	require.NoError(t, err)
	require.Equal(t, []byte{
		0x19, 0x0, 0x0, 0x0, // Length
		0x0, 0x0, 0x0, 0x0, // SRID
		0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0xbf}, bytes)
	typeId, metadata := s.metadata(nil, typ)
	require.EqualValues(t, mysql.TypeGeometry, typeId)
	require.EqualValues(t, 0x04, metadata)
}

func newTupleBuilderForEncoding(encoding val.Encoding) (val.TupleDesc, *val.TupleBuilder) {
	ns := tree.NewTestNodeStore()
	tupleDesc := val.NewTupleDescriptor(val.Type{Enc: encoding})
	tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
	return tupleDesc, tupleBuilder
}

func createTestStringSlice(length int) []string {
	result := make([]string, length)
	for i := 0; i < length; i++ {
		result[i] = fmt.Sprintf("%d", i)
	}
	return result
}

func createTestBlob(t *testing.T, bytes []byte) (tree.NodeStore, hash.Hash) {
	storage := &chunks.MemoryStorage{}
	cs := storage.NewViewWithFormat("__DOLT__")
	ns := tree.NewNodeStore(cs)
	blobBuilder := ns.BlobBuilder()
	blobBuilder.Init(len(bytes))
	_, addr, err := blobBuilder.Chunk(context.Background(), bytes2.NewReader(bytes))
	require.NoError(t, err)
	return ns, addr
}
