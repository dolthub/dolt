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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), varchar20, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, varchar20, value, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{3, 'a', 'b', 'c'}, bytes)
		typeId, metadata := s.metadata(nil, varchar255)
		require.EqualValues(t, mysql.TypeVarchar, typeId)
		require.EqualValues(t, 255*4, metadata)
	})
	t.Run("VARCHAR 2 byte length encoding", func(t *testing.T) {
		tupleBuilder.PutString(0, "abc")
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), varchar255, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, varchar255, value, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{3, 0, 'a', 'b', 'c'}, bytes)
		typeId, metadata := s.metadata(nil, varchar255)
		require.EqualValues(t, mysql.TypeVarchar, typeId)
		require.EqualValues(t, 255*4, metadata)
	})
	t.Run("CHAR 1 byte length encoding", func(t *testing.T) {
		typ := gmstypes.MustCreateString(sqltypes.Char, 25, sql.Collation_Default)
		tupleBuilder.PutString(0, "abc")
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{0x03, 'a', 'b', 'c'}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeString, typeId)
		require.EqualValues(t, ((mysql.TypeString<<8)^0x00)|0x64, metadata)
	})
	t.Run("CHAR 2 byte length encoding", func(t *testing.T) {
		typ := gmstypes.MustCreateString(sqltypes.Char, 100, sql.Collation_Default)
		tupleBuilder.PutString(0, "abc")
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, ns)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, ns)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		// VARBINARY representation is NOT right padded to the full length
		bytes, err := s.serialize(nil, typ, value, ns)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		// BINARY representation is NOT right padded to the full length
		bytes, err := s.serialize(nil, typ, value, ns)
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
	tuple, err := tupleBuilder.Build(context.Background(), buffPool)
	require.NoError(t, err)
	value, err := s.deserialize(nil, gmstypes.Float32, tupleDesc, tuple, 0, nil)
	require.NoError(t, err)
	bytes, err := s.serialize(nil, gmstypes.Float32, value, nil)

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
	tuple, err := tupleBuilder.Build(context.Background(), buffPool)
	require.NoError(t, err)

	value, err := s.deserialize(nil, gmstypes.Float64, tupleDesc, tuple, 0, nil)
	require.NotNil(t, value)
	require.Nil(t, err)

	bytes, err := s.serialize(nil, gmstypes.Float64, value, nil)
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
	tuple, err := tupleBuilder.Build(context.Background(), buffPool)
	require.NoError(t, err)

	value, err := s.deserialize(nil, gmstypes.Year, tupleDesc, tuple, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, value)

	bytes, err := s.serialize(nil, gmstypes.Year, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, datetimeType, tupleDesc, tuple, 0,
			nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, datetimeType, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, datetimeType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, datetimeType, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, datetimeType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, datetimeType, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, datetimeType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, datetimeType, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, datetimeType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, datetimeType, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, datetimeType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, datetimeType, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, datetimeType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, datetimeType, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, timestampType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, timestampType, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, timestampType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, timestampType, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, timestampType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, timestampType, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, timestampType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, timestampType, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, timestampType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, timestampType, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, timestampType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, timestampType, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, timestampType, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, timestampType, value, nil)
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
	tuple, err := tupleBuilder.Build(context.Background(), buffPool)
	require.NoError(t, err)

	value, err := s.deserialize(nil, gmstypes.Date, tupleDesc, tuple, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, value)

	bytes, err := s.serialize(nil, gmstypes.Date, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		dec, _, err := apd.NewFromString("0")
		require.NoError(t, err)
		tupleBuilder.PutDecimal(0, dec)
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeNewDecimal, typeId)
		require.EqualValues(t, 14<<8|4, metadata)
	})
	t.Run("100", func(t *testing.T) {
		typ := gmstypes.MustCreateDecimalType(14, 4)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.DecimalEnc)
		dec, _, err := apd.NewFromString("100")
		require.NoError(t, err)
		tupleBuilder.PutDecimal(0, dec)
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x80, 0x0, 0x0, 0x0, 0x64, 0x0, 0x0}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeNewDecimal, typeId)
		require.EqualValues(t, 14<<8|4, metadata)
	})
	t.Run("1.1", func(t *testing.T) {
		typ := gmstypes.MustCreateDecimalType(14, 4)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.DecimalEnc)
		dec, _, err := apd.NewFromString("1.1")
		require.NoError(t, err)
		tupleBuilder.PutDecimal(0, dec)
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x80, 0x0, 0x0, 0x0, 0x1, 0x3, 0xe8}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeNewDecimal, typeId)
		require.EqualValues(t, 14<<8|4, metadata)
	})
	t.Run("10", func(t *testing.T) {
		typ := gmstypes.MustCreateDecimalType(19, 0)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.DecimalEnc)
		dec, _, err := apd.NewFromString("100")
		require.NoError(t, err)
		tupleBuilder.PutDecimal(0, dec)
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeNewDecimal, typeId)
		require.EqualValues(t, 19<<8|0, metadata)
	})
	t.Run("1234567890.1234", func(t *testing.T) {
		typ := gmstypes.MustCreateDecimalType(14, 4)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.DecimalEnc)
		dec, _, err := apd.NewFromString("1234567890.1234")
		require.NoError(t, err)
		tupleBuilder.PutDecimal(0, dec)
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x81, 0x0D, 0xFB, 0x38, 0xD2, 0x04, 0xD2}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeNewDecimal, typeId)
		require.EqualValues(t, 14<<8|4, metadata)
	})
	t.Run("-1234567890.1234", func(t *testing.T) {
		typ := gmstypes.MustCreateDecimalType(14, 4)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.DecimalEnc)
		dec, _, err := apd.NewFromString("-1234567890.1234")
		require.NoError(t, err)
		tupleBuilder.PutDecimal(0, dec)
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x7E, 0xF2, 0x04, 0xC7, 0x2D, 0xFB, 0x2D}, bytes)
		typeId, metadata := s.metadata(nil, typ)
		require.EqualValues(t, mysql.TypeNewDecimal, typeId)
		require.EqualValues(t, 14<<8|4, metadata)
	})
	t.Run("1234567890.0001", func(t *testing.T) {
		typ := gmstypes.MustCreateDecimalType(14, 4)
		tupleDesc, tupleBuilder := newTupleBuilderForEncoding(val.DecimalEnc)
		dec, _, err := apd.NewFromString("1234567890.0001")
		require.NoError(t, err)
		tupleBuilder.PutDecimal(0, dec)
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
	tuple, err := tupleBuilder.Build(context.Background(), buffPool)
	require.NoError(t, err)

	value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, value)

	bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(nil, typ, value, nil)
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
	tuple, err := tupleBuilder.Build(context.Background(), buffPool)
	require.NoError(t, err)

	value, err := s.deserialize(nil, typ, tupleDesc, tuple, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, value)

	bytes, err := s.serialize(nil, typ, value, nil)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(context.Background(), typ, value, ns)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(context.Background(), typ, value, ns)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(context.Background(), typ, value, ns)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(context.Background(), typ, value, ns)
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
	tuple, err := tupleBuilder.Build(context.Background(), buffPool)
	require.NoError(t, err)

	value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, ns)
	require.NoError(t, err)
	require.NotNil(t, value)

	bytes, err := s.serialize(context.Background(), typ, value, ns)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(context.Background(), typ, value, ns)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(context.Background(), typ, value, ns)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(context.Background(), typ, value, ns)
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
		tuple, err := tupleBuilder.Build(context.Background(), buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(context.Background(), typ, value, ns)
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
	tuple, err := tupleBuilder.Build(context.Background(), buffPool)
	require.NoError(t, err)

	value, err := s.deserialize(context.Background(), typ, tupleDesc, tuple, 0, ns)
	require.NoError(t, err)
	require.NotNil(t, value)

	bytes, err := s.serialize(nil, typ, value, ns)
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

func newTupleBuilderForEncoding(encoding val.Encoding) (*val.TupleDesc, *val.TupleBuilder) {
	ns := tree.NewTestNodeStore()
	tupleDesc := val.NewTupleDescriptor(val.Type{Enc: encoding})
	tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
	return tupleDesc, tupleBuilder
}

// newAdaptiveBuilder returns a TupleDesc, TupleBuilder, and the NodeStore they share, so
// callers can reuse the same NodeStore when later deserializing the tuple via tree.GetField.
func newAdaptiveBuilder(encoding val.Encoding) (*val.TupleDesc, *val.TupleBuilder, tree.NodeStore) {
	storage := &chunks.MemoryStorage{}
	cs := storage.NewViewWithFormat("__DOLT__")
	ns := tree.NewNodeStore(cs)
	tupleDesc := val.NewTupleDescriptor(val.Type{Enc: encoding})
	tupleBuilder := val.NewTupleBuilder(tupleDesc, ns)
	return tupleDesc, tupleBuilder, ns
}

func TestTextSerializer_AdaptiveEncoding(t *testing.T) {
	s := textSerializer{}
	typ := gmstypes.Text
	ctx := context.Background()

	t.Run("inline", func(t *testing.T) {
		tupleDesc, tupleBuilder, ns := newAdaptiveBuilder(val.StringAdaptiveEnc)
		require.NoError(t, tupleBuilder.PutAdaptiveStringFromInline(ctx, 0, "abcde"))
		tuple, err := tupleBuilder.Build(ctx, buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(ctx, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(ctx, typ, value, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{0x05, 0x00, 'a', 'b', 'c', 'd', 'e'}, bytes)
	})

	t.Run("out-of-band", func(t *testing.T) {
		tupleDesc, tupleBuilder, ns := newAdaptiveBuilder(val.StringAdaptiveEnc)
		payload := []byte("abcde")
		addr, err := ns.WriteBytes(ctx, payload)
		require.NoError(t, err)
		ts := val.NewTextStorage(addr, ns).WithMaxByteLength(int64(len(payload)))
		tupleBuilder.PutAdaptiveStringFromOutline(0, ts)
		tuple, err := tupleBuilder.Build(ctx, buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(ctx, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(ctx, typ, value, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{0x05, 0x00, 'a', 'b', 'c', 'd', 'e'}, bytes)
	})
}

func TestBlobSerializer_AdaptiveEncoding(t *testing.T) {
	s := blobSerializer{}
	typ := gmstypes.Blob
	ctx := context.Background()

	t.Run("inline", func(t *testing.T) {
		tupleDesc, tupleBuilder, ns := newAdaptiveBuilder(val.BytesAdaptiveEnc)
		require.NoError(t, tupleBuilder.PutAdaptiveBytesFromInline(ctx, 0, []byte("abc")))
		tuple, err := tupleBuilder.Build(ctx, buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(ctx, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(ctx, typ, value, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{0x03, 0x00, 'a', 'b', 'c'}, bytes)
	})

	t.Run("out-of-band", func(t *testing.T) {
		tupleDesc, tupleBuilder, ns := newAdaptiveBuilder(val.BytesAdaptiveEnc)
		payload := []byte("abc")
		addr, err := ns.WriteBytes(ctx, payload)
		require.NoError(t, err)
		ba := val.NewByteArray(addr, ns).WithMaxByteLength(int64(len(payload)))
		tupleBuilder.PutAdaptiveBytesFromOutline(0, ba)
		tuple, err := tupleBuilder.Build(ctx, buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(ctx, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(ctx, typ, value, ns)
		require.NoError(t, err)
		require.Equal(t, []byte{0x03, 0x00, 'a', 'b', 'c'}, bytes)
	})
}

func TestJsonSerializer_AdaptiveEncoding(t *testing.T) {
	s := jsonSerializer{}
	typ := gmstypes.JSON
	ctx := context.Background()
	jsonBytes := []byte(`{"a":"b"}`)
	expectedWire := []byte{0x0f, 0x00, 0x00, 0x00,
		0, 1, 0, 14, 0, 11, 0, 1, 0, 12, 12, 0, 97, 1, 98}

	t.Run("inline", func(t *testing.T) {
		tupleDesc, tupleBuilder, ns := newAdaptiveBuilder(val.JsonAdaptiveEnc)
		require.NoError(t, tupleBuilder.PutAdaptiveJsonFromInline(ctx, 0, jsonBytes))
		tuple, err := tupleBuilder.Build(ctx, buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(ctx, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(ctx, typ, value, ns)
		require.NoError(t, err)
		require.Equal(t, expectedWire, bytes)
	})

	t.Run("out-of-band", func(t *testing.T) {
		tupleDesc, tupleBuilder, ns := newAdaptiveBuilder(val.JsonAdaptiveEnc)
		addr, err := ns.WriteBytes(ctx, jsonBytes)
		require.NoError(t, err)
		js := val.NewJsonStorageOutOfBand(addr, ns, int64(len(jsonBytes)))
		tupleBuilder.PutAdaptiveJsonFromOutline(0, js)
		tuple, err := tupleBuilder.Build(ctx, buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(ctx, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(ctx, typ, value, ns)
		require.NoError(t, err)
		require.Equal(t, expectedWire, bytes)
	})
}

// TestUnwrapToBytes_JSONWrappers checks that unwrapToBytes — used by the TEXT and BLOB
// serializers — correctly handles JSON wrapper values that come out of tree.GetField.
// This matters for schema-change replication, where a column that was JSON in the source
// schema may be serialized through a TEXT/BLOB target serializer, producing a JSON wrapper
// value where the serializer expects byte-shaped data.
func TestUnwrapToBytes_JSONWrappers(t *testing.T) {
	ctx := context.Background()
	jsonBytes := []byte(`{"a":"b"}`)

	t.Run("LazyJSONDocument (inline JSON)", func(t *testing.T) {
		// Build an inline-stored JSON tuple, deserialize through tree.GetField directly to
		// get back a LazyJSONDocument, and pass that to unwrapToBytes for a TEXT type.
		_, tupleBuilder, ns := newAdaptiveBuilder(val.JsonAdaptiveEnc)
		require.NoError(t, tupleBuilder.PutAdaptiveJsonFromInline(ctx, 0, jsonBytes))
		tuple, err := tupleBuilder.Build(ctx, buffPool)
		require.NoError(t, err)

		td := val.NewTupleDescriptor(val.Type{Enc: val.JsonAdaptiveEnc})
		value, err := tree.GetField(ctx, td, 0, tuple, ns)
		require.NoError(t, err)
		_, isJsonWrapper := value.(sql.JSONWrapper)
		require.True(t, isJsonWrapper, "expected sql.JSONWrapper, got %T", value)

		out, err := unwrapToBytes(ctx, value, gmstypes.LongText, ns)
		require.NoError(t, err)
		require.Equal(t, jsonBytes, out)
	})

	t.Run("IndexedJsonDocument (out-of-band JSON)", func(t *testing.T) {
		// Force out-of-band storage by writing a JSON blob and putting it as an outline,
		// so tree.GetField returns an IndexedJsonDocument.
		_, tupleBuilder, ns := newAdaptiveBuilder(val.JsonAdaptiveEnc)
		addr, err := ns.WriteBytes(ctx, jsonBytes)
		require.NoError(t, err)
		js := val.NewJsonStorageOutOfBand(addr, ns, int64(len(jsonBytes)))
		tupleBuilder.PutAdaptiveJsonFromOutline(0, js)
		tuple, err := tupleBuilder.Build(ctx, buffPool)
		require.NoError(t, err)

		td := val.NewTupleDescriptor(val.Type{Enc: val.JsonAdaptiveEnc})
		value, err := tree.GetField(ctx, td, 0, tuple, ns)
		require.NoError(t, err)
		_, isJsonBytes := value.(gmstypes.JSONBytes)
		require.True(t, isJsonBytes, "expected gmstypes.JSONBytes, got %T", value)

		out, err := unwrapToBytes(ctx, value, gmstypes.LongBlob, ns)
		require.NoError(t, err)
		require.Equal(t, jsonBytes, out)
	})
}

// TestJsonSerializer_AdaptiveEncoding_DocumentShapes round-trips a variety of JSON
// document shapes — objects with many keys, arrays with many elements, nested mixtures,
// and documents spanning the inline/out-of-band Dolt storage boundary (~2 KiB) plus the
// MySQL small-format/large-format wire boundary (~64 KiB encoded). Each shape produces
// many small leaf values so the resulting MySQL wire encoding actually has a long
// value-entries section with non-trivial offsets, rather than collapsing to a single
// huge inline string. For every case we compare the serializer's wire bytes against an
// independent encodeJsonDoc reference run on a freshly-parsed copy of the source bytes.
func TestJsonSerializer_AdaptiveEncoding_DocumentShapes(t *testing.T) {
	s := jsonSerializer{}
	typ := gmstypes.JSON
	ctx := context.Background()

	// Per-element wire overhead estimate (key entry + value entry + ~3-byte key + 8-byte
	// double): ~17 bytes/element in small object encoding. So 5000 keys ≈ 85 KiB encoded,
	// comfortably past the 64 KiB small/large boundary.
	cases := []struct {
		name     string
		buildDoc func() string
	}{
		{
			name: "object_few_keys_inline_storage",
			buildDoc: func() string {
				return buildJsonObject(t, 8)
			},
		},
		{
			name: "object_many_keys_inline_storage_under_2k",
			buildDoc: func() string {
				return buildJsonObject(t, 50)
			},
		},
		{
			name: "object_many_keys_out_of_band_small_wire",
			buildDoc: func() string {
				// Out-of-band in Dolt (>2 KiB raw), still small-format wire (<64 KiB encoded).
				return buildJsonObject(t, 1000)
			},
		},
		{
			name: "object_many_keys_crosses_64k_wire_boundary",
			buildDoc: func() string {
				// Forces the wire encoding from small (16-bit offsets) to large (32-bit).
				return buildJsonObject(t, 5000)
			},
		},
		{
			name: "array_few_elements",
			buildDoc: func() string {
				return buildJsonArray(t, 8)
			},
		},
		{
			name: "array_many_elements_out_of_band",
			buildDoc: func() string {
				return buildJsonArray(t, 2000)
			},
		},
		{
			name: "array_many_elements_crosses_64k_wire_boundary",
			buildDoc: func() string {
				// Per-element wire overhead in a small array is ~3 bytes value-entry plus
				// ~5 bytes value, so ~9000 mixed-leaf elements is needed to exceed 64 KiB.
				return buildJsonArray(t, 12000)
			},
		},
		{
			name: "mixed_nested_object_with_arrays",
			buildDoc: func() string {
				return buildNestedJson(t, 200, 30)
			},
		},
		{
			name: "mixed_nested_crosses_64k_wire_boundary",
			buildDoc: func() string {
				return buildNestedJson(t, 1500, 50)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			docStr := tc.buildDoc()
			jsonBytes := []byte(docStr)

			// Reference: parse and encode independently — this is the wire output the
			// production serializer must produce regardless of storage layout.
			var refDoc interface{}
			require.NoError(t, json.Unmarshal(jsonBytes, &refDoc))
			refJsonBuf, err := encodeJsonDoc(ctx, gmstypes.JSONDocument{Val: refDoc})
			require.NoError(t, err)
			refLength := make([]byte, 4)
			binary.LittleEndian.PutUint32(refLength, uint32(len(refJsonBuf)))
			expectedWire := append(append([]byte{}, refLength...), refJsonBuf...)

			tupleDesc, tupleBuilder, ns := newAdaptiveBuilder(val.JsonAdaptiveEnc)
			// Routes inline or out-of-band automatically based on the raw byte length.
			require.NoError(t, tupleBuilder.PutAdaptiveJsonFromInline(ctx, 0, jsonBytes))
			tuple, err := tupleBuilder.Build(ctx, buffPool)
			require.NoError(t, err)

			value, err := s.deserialize(ctx, typ, tupleDesc, tuple, 0, ns)
			require.NoError(t, err)
			require.NotNil(t, value)

			gotWire, err := s.serialize(ctx, typ, value, ns)
			require.NoError(t, err)
			require.Equal(t, expectedWire, gotWire,
				"wire format mismatch for %s (raw json bytes=%d, encoded body bytes=%d)",
				tc.name, len(jsonBytes), len(refJsonBuf))

			// Sanity-check that the boundary-crossing cases really do cross. The wire
			// body's first byte after the 4-byte length prefix is the JSON type id.
			// Small object/array ids are 0x00/0x02; large ones are 0x01/0x03.
			rootTypeId := gotWire[4]
			switch tc.name {
			case "object_many_keys_crosses_64k_wire_boundary":
				require.Equalf(t, byte(0x01), rootTypeId,
					"expected large object type id (0x01) for %s, got 0x%02x; encoded body size=%d",
					tc.name, rootTypeId, len(refJsonBuf))
			case "array_many_elements_crosses_64k_wire_boundary":
				require.Equalf(t, byte(0x03), rootTypeId,
					"expected large array type id (0x03) for %s, got 0x%02x; encoded body size=%d",
					tc.name, rootTypeId, len(refJsonBuf))
			case "object_many_keys_out_of_band_small_wire":
				require.Equalf(t, byte(0x00), rootTypeId,
					"expected small object type id (0x00) for %s, got 0x%02x; encoded body size=%d",
					tc.name, rootTypeId, len(refJsonBuf))
			}
		})
	}
}

// buildJsonObject returns the JSON text of an object with |n| keys. Each value is a
// distinct small leaf type so the encoded value-entries section is genuinely populated
// with mixed type IDs (numbers, strings, booleans, null).
func buildJsonObject(t *testing.T, n int) string {
	t.Helper()
	obj := make(map[string]interface{}, n)
	for i := 0; i < n; i++ {
		obj[fmt.Sprintf("k%06d", i)] = jsonLeafValue(i)
	}
	buf, err := json.Marshal(obj)
	require.NoError(t, err)
	return string(buf)
}

// buildJsonArray returns the JSON text of an array with |n| elements, again mixing leaf
// types so every value-entry slot carries non-trivial data.
func buildJsonArray(t *testing.T, n int) string {
	t.Helper()
	arr := make([]interface{}, n)
	for i := 0; i < n; i++ {
		arr[i] = jsonLeafValue(i)
	}
	buf, err := json.Marshal(arr)
	require.NoError(t, err)
	return string(buf)
}

// buildNestedJson returns a JSON object that contains both a long array and a sub-object
// with many keys, so the encoder has to handle nested small/large decisions and the
// outer object's offsets point at non-trivial sub-encodings.
func buildNestedJson(t *testing.T, arrayLen, subObjectKeys int) string {
	t.Helper()
	subObj := make(map[string]interface{}, subObjectKeys)
	for i := 0; i < subObjectKeys; i++ {
		subObj[fmt.Sprintf("nested_key_%04d", i)] = jsonLeafValue(i * 7)
	}
	arr := make([]interface{}, arrayLen)
	for i := 0; i < arrayLen; i++ {
		arr[i] = jsonLeafValue(i)
	}
	root := map[string]interface{}{
		"meta": map[string]interface{}{
			"version": 3,
			"label":   "test",
			"flags":   []interface{}{true, false, nil},
		},
		"items":  arr,
		"detail": subObj,
	}
	buf, err := json.Marshal(root)
	require.NoError(t, err)
	return string(buf)
}

// jsonLeafValue returns a small leaf value that varies by index so encoded value-entries
// span MySQL's distinct JSON type ids: numbers (double), strings, booleans, and null.
func jsonLeafValue(i int) interface{} {
	switch i % 5 {
	case 0:
		return float64(i)
	case 1:
		return fmt.Sprintf("v_%d", i)
	case 2:
		return i%2 == 0
	case 3:
		return nil
	default:
		return float64(i) * 0.5
	}
}

func TestGeometrySerializer_AdaptiveEncoding(t *testing.T) {
	s := geometrySerializer{}
	typ := typeinfo.GeometryType.ToSqlType()
	ctx := context.Background()
	geoBytes := []byte{
		0x00, 0x00, 0x00, 0x00, // SRID
		0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xBF}
	expectedWire := []byte{
		0x19, 0x0, 0x0, 0x0, // Length
		0x0, 0x0, 0x0, 0x0, // SRID
		0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0xbf}

	t.Run("inline", func(t *testing.T) {
		tupleDesc, tupleBuilder, ns := newAdaptiveBuilder(val.GeomAdaptiveEnc)
		require.NoError(t, tupleBuilder.PutAdaptiveGeomFromInline(ctx, 0, geoBytes))
		tuple, err := tupleBuilder.Build(ctx, buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(ctx, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(ctx, typ, value, ns)
		require.NoError(t, err)
		require.Equal(t, expectedWire, bytes)
	})

	t.Run("out-of-band", func(t *testing.T) {
		tupleDesc, tupleBuilder, ns := newAdaptiveBuilder(val.GeomAdaptiveEnc)
		addr, err := ns.WriteBytes(ctx, geoBytes)
		require.NoError(t, err)
		tupleBuilder.PutAdaptiveGeomFromOutOfBand(0, int64(len(geoBytes)), addr)
		tuple, err := tupleBuilder.Build(ctx, buffPool)
		require.NoError(t, err)

		value, err := s.deserialize(ctx, typ, tupleDesc, tuple, 0, ns)
		require.NoError(t, err)
		require.NotNil(t, value)

		bytes, err := s.serialize(ctx, typ, value, ns)
		require.NoError(t, err)
		require.Equal(t, expectedWire, bytes)
	})
}

// TestGeometrySerializer_AdaptiveEncoding_LargeValues round-trips real GeometryValue
// objects (LineStrings with many points, Polygons with many rings) through the natural
// PutAdaptiveGeomFromInline routing. Small values land inline; values larger than the
// adaptive tuple-length target are automatically promoted to out-of-band storage,
// returning *val.GeometryStorage from tree.GetField. The serializer must handle both
// shapes; this test exercises both, as well as multi-chunk blob storage for very large
// values (>~chunk size, so the underlying blob spans multiple prolly-tree leaves).
func TestGeometrySerializer_AdaptiveEncoding_LargeValues(t *testing.T) {
	s := geometrySerializer{}
	typ := typeinfo.GeometryType.ToSqlType()
	ctx := context.Background()

	cases := []struct {
		name              string
		buildGeo          func() gmstypes.GeometryValue
		expectOutOfBand   bool
		expectStorageType interface{} // *val.GeometryStorage when out-of-band, else types.GeometryValue
	}{
		{
			// Single Point → 25 bytes serialized → comfortably inline. Smallest geometry
			// shape; sanity-checks that small values keep working through the new path.
			name:              "small_point_inline",
			buildGeo:          func() gmstypes.GeometryValue { return gmstypes.Point{SRID: 0, X: 1.0, Y: -1.0} },
			expectOutOfBand:   false,
			expectStorageType: gmstypes.Point{},
		},
		{
			// 50 points → 813 bytes serialized → still inline (under 2 KiB target).
			name:              "medium_linestring_inline",
			buildGeo:          func() gmstypes.GeometryValue { return makeLineString(t, 50) },
			expectOutOfBand:   false,
			expectStorageType: gmstypes.LineString{},
		},
		{
			// 200 points → 3213 bytes serialized → just over the inline threshold,
			// promoted to out-of-band single-chunk storage. Exercises GeometryStorage
			// round-trip on the smallest possible out-of-band geometry.
			name:              "linestring_just_over_inline",
			buildGeo:          func() gmstypes.GeometryValue { return makeLineString(t, 200) },
			expectOutOfBand:   true,
			expectStorageType: &val.GeometryStorage{},
		},
		{
			// 10000 points → ~160 KiB serialized → out-of-band, stored as a multi-chunk
			// blob in the prolly tree (the BlobBuilder chunks at ~64 KiB).
			name:              "linestring_large_multi_chunk",
			buildGeo:          func() gmstypes.GeometryValue { return makeLineString(t, 10000) },
			expectOutOfBand:   true,
			expectStorageType: &val.GeometryStorage{},
		},
		{
			// Polygon with one outer ring and many inner rings (holes), each a small
			// rectangle. 500 rings → ~40 KiB serialized → out-of-band, single chunk.
			// Confirms the serializer handles non-LineString shapes through the
			// out-of-band path too.
			name:              "polygon_many_rings_out_of_band",
			buildGeo:          func() gmstypes.GeometryValue { return makePolygon(t, 500) },
			expectOutOfBand:   true,
			expectStorageType: &val.GeometryStorage{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			geo := tc.buildGeo()
			rawSerialized := geo.Serialize()

			expectedWireLength := make([]byte, 4)
			binary.LittleEndian.PutUint32(expectedWireLength, uint32(len(rawSerialized)))
			expectedWire := append(append([]byte{}, expectedWireLength...), rawSerialized...)

			// Use tree.PutField — the production path — which calls
			// PutAdaptiveGeomFromInline and lets the tuple builder decide inline
			// vs. out-of-band based on size. This is what real writes go through.
			tupleDesc, tupleBuilder, ns := newAdaptiveBuilder(val.GeomAdaptiveEnc)
			require.NoError(t, tree.PutField(ctx, ns, tupleBuilder, 0, geo))
			tuple, err := tupleBuilder.Build(ctx, buffPool)
			require.NoError(t, err)

			// Confirm the value actually landed where the test expects: out-of-band
			// values come back from tree.GetField as *val.GeometryStorage; inline ones
			// come back already deserialized into a concrete GeometryValue.
			deserialized, err := tree.GetField(ctx, tupleDesc, 0, tuple, ns)
			require.NoError(t, err)
			require.NotNil(t, deserialized)
			if tc.expectOutOfBand {
				_, isStorage := deserialized.(*val.GeometryStorage)
				require.Truef(t, isStorage,
					"%s: expected *val.GeometryStorage from out-of-band path, got %T (serialized geo bytes=%d)",
					tc.name, deserialized, len(rawSerialized))
			} else {
				_, isStorage := deserialized.(*val.GeometryStorage)
				require.Falsef(t, isStorage,
					"%s: expected inline GeometryValue, got *val.GeometryStorage (serialized geo bytes=%d)",
					tc.name, len(rawSerialized))
			}

			// Now drive the same path the binlog producer does: serializer.deserialize
			// → serializer.serialize → wire bytes.
			value, err := s.deserialize(ctx, typ, tupleDesc, tuple, 0, ns)
			require.NoError(t, err)
			require.NotNil(t, value)

			gotWire, err := s.serialize(ctx, typ, value, ns)
			require.NoError(t, err)
			require.Equal(t, expectedWire, gotWire,
				"%s: wire format mismatch (serialized geo bytes=%d)", tc.name, len(rawSerialized))
		})
	}
}

// makeLineString builds a LineString with |n| points lying on a simple curve so each
// point's bytes differ; this avoids any chance the storage layer or test framework
// short-circuits identical inputs.
func makeLineString(t *testing.T, n int) gmstypes.LineString {
	t.Helper()
	require.Greater(t, n, 0)
	points := make([]gmstypes.Point, n)
	for i := 0; i < n; i++ {
		points[i] = gmstypes.Point{
			SRID: 0,
			X:    float64(i) * 0.001,
			Y:    float64(i) * -0.002,
		}
	}
	return gmstypes.LineString{SRID: 0, Points: points}
}

// makePolygon builds a Polygon with one outer ring (square) and |innerRings| inner
// rings (square holes), each a closed ring of 5 points. innerRings of 0 still yields
// a valid Polygon with just the outer ring.
func makePolygon(t *testing.T, innerRings int) gmstypes.Polygon {
	t.Helper()
	closedRing := func(originX, originY, side float64) gmstypes.LineString {
		return gmstypes.LineString{
			SRID: 0,
			Points: []gmstypes.Point{
				{X: originX, Y: originY},
				{X: originX + side, Y: originY},
				{X: originX + side, Y: originY + side},
				{X: originX, Y: originY + side},
				{X: originX, Y: originY}, // close
			},
		}
	}
	rings := make([]gmstypes.LineString, 0, innerRings+1)
	rings = append(rings, closedRing(0, 0, 1000)) // big outer square
	for i := 0; i < innerRings; i++ {
		// non-overlapping unit squares scattered inside the outer square
		rings = append(rings, closedRing(float64(i%30), float64(i/30), 0.1))
	}
	return gmstypes.Polygon{SRID: 0, Lines: rings}
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
