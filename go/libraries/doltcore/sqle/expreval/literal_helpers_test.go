// Copyright 2019-2020 Dolthub, Inc.
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

package expreval

import (
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql/expression"
	types2 "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/types"
)

func TestLiteralAsInt64(t *testing.T) {
	tests := []struct {
		name      string
		l         *expression.Literal
		expected  int64
		expectErr bool
	}{
		{
			"int8 literal",
			expression.NewLiteral(int8(5), types2.Int8),
			5,
			false,
		},
		{
			"int16 literal",
			expression.NewLiteral(int16(5), types2.Int16),
			5,
			false,
		},
		{
			"int32 literal",
			expression.NewLiteral(int32(5), types2.Int32),
			5,
			false,
		},
		{
			"int literal",
			expression.NewLiteral(int(5), types2.Int32),
			5,
			false,
		},
		{
			"int64 literal",
			expression.NewLiteral(int64(5), types2.Int64),
			5,
			false,
		},

		{
			"uint8 literal",
			expression.NewLiteral(uint8(5), types2.Uint8),
			5,
			false,
		},
		{
			"uint16 literal",
			expression.NewLiteral(uint16(5), types2.Uint16),
			5,
			false,
		},
		{
			"uint32 literal",
			expression.NewLiteral(uint32(5), types2.Uint32),
			5,
			false,
		},
		{
			"uint literal",
			expression.NewLiteral(uint(5), types2.Uint32),
			5,
			false,
		},
		{
			"uint64 literal",
			expression.NewLiteral(uint64(5), types2.Uint64),
			5,
			false,
		},
		{
			"true literal",
			expression.NewLiteral(true, types2.Boolean),
			1,
			false,
		},
		{
			"false literal",
			expression.NewLiteral(false, types2.Boolean),
			0,
			false,
		},
		{
			"float32 literal",
			expression.NewLiteral(float32(32.0), types2.Float32),
			32,
			false,
		},
		{
			"float64 literal",
			expression.NewLiteral(float64(32.0), types2.Float64),
			32,
			false,
		},
		{
			"string literal",
			expression.NewLiteral("54321", types2.Text),
			54321,
			false,
		},
		{
			"uint literal too big",
			expression.NewLiteral(uint64(0xFFFFFFFFFFFFFFFF), types2.Uint32),
			0,
			true,
		},
		{
			"float64 with fractional portion",
			expression.NewLiteral(float64(5.0005), types2.Float64),
			0,
			true,
		},
		{
			"float32 with fractional portion",
			expression.NewLiteral(float32(5.0005), types2.Float32),
			0,
			true,
		},
		{
			"string not a number",
			expression.NewLiteral("not a number", types2.Text),
			0,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := literalAsInt64(test.l)
			assertOnUnexpectedErr(t, test.expectErr, err)

			if err == nil {
				assert.Equal(t, test.expected, res)
			}
		})
	}
}

func TestLiteralAsUint64(t *testing.T) {
	tests := []struct {
		name      string
		l         *expression.Literal
		expected  uint64
		expectErr bool
	}{
		{
			"int8 literal",
			expression.NewLiteral(int8(5), types2.Int8),
			5,
			false,
		},
		{
			"int16 literal",
			expression.NewLiteral(int16(5), types2.Int16),
			5,
			false,
		},
		{
			"int32 literal",
			expression.NewLiteral(int32(5), types2.Int32),
			5,
			false,
		},
		{
			"int literal",
			expression.NewLiteral(int(5), types2.Int32),
			5,
			false,
		},
		{
			"int64 literal",
			expression.NewLiteral(int64(5), types2.Int64),
			5,
			false,
		},
		{
			"uint8 literal",
			expression.NewLiteral(uint8(5), types2.Uint8),
			5,
			false,
		},
		{
			"uint16 literal",
			expression.NewLiteral(uint16(5), types2.Uint16),
			5,
			false,
		},
		{
			"uint32 literal",
			expression.NewLiteral(uint32(5), types2.Uint32),
			5,
			false,
		},
		{
			"uint literal",
			expression.NewLiteral(uint(5), types2.Uint32),
			5,
			false,
		},
		{
			"uint64 literal",
			expression.NewLiteral(uint64(5), types2.Uint64),
			5,
			false,
		},
		{
			"true literal",
			expression.NewLiteral(true, types2.Boolean),
			1,
			false,
		},
		{
			"false literal",
			expression.NewLiteral(false, types2.Boolean),
			0,
			false,
		},
		{
			"float32 literal",
			expression.NewLiteral(float32(32.0), types2.Float32),
			32,
			false,
		},
		{
			"float64 literal",
			expression.NewLiteral(float64(32.0), types2.Float64),
			32,
			false,
		},
		{
			"string literal",
			expression.NewLiteral("54321", types2.Text),
			54321,
			false,
		},
		{
			"negative int8 literal",
			expression.NewLiteral(int8(-1), types2.Int8),
			0,
			true,
		},
		{
			"negative int16 literal",
			expression.NewLiteral(int16(-1), types2.Int16),
			0,
			true,
		},
		{
			"negative int32 literal",
			expression.NewLiteral(int32(-1), types2.Int32),
			0,
			true,
		},
		{
			"negative int literal",
			expression.NewLiteral(int(-1), types2.Int32),
			0,
			true,
		},
		{
			"negative int64 literal",
			expression.NewLiteral(int64(-1), types2.Int64),
			0,
			true,
		},
		{
			"float32 with fractional portion",
			expression.NewLiteral(float32(5.0005), types2.Float32),
			0,
			true,
		},
		{
			"float64 with fractional portion",
			expression.NewLiteral(float64(5.0005), types2.Float64),
			0,
			true,
		},
		{
			"string not a number",
			expression.NewLiteral("not a number", types2.Text),
			0,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := literalAsUint64(test.l)
			assertOnUnexpectedErr(t, test.expectErr, err)

			if err == nil {
				assert.Equal(t, test.expected, res)
			}
		})
	}
}

func TestLiteralAsFloat64(t *testing.T) {
	tests := []struct {
		name      string
		l         *expression.Literal
		expected  float64
		expectErr bool
	}{
		{
			"int8 literal",
			expression.NewLiteral(int8(-5), types2.Int8),
			-5.0,
			false,
		},
		{
			"int16 literal",
			expression.NewLiteral(int16(-5), types2.Int16),
			-5.0,
			false,
		},
		{
			"int32 literal",
			expression.NewLiteral(int32(-5), types2.Int32),
			-5.0,
			false,
		},
		{
			"int literal",
			expression.NewLiteral(int(-5), types2.Int32),
			-5.0,
			false,
		},
		{
			"int64 literal",
			expression.NewLiteral(int64(-5), types2.Int64),
			-5.0,
			false,
		},
		{
			"uint8 literal",
			expression.NewLiteral(uint8(5), types2.Uint8),
			5.0,
			false,
		},
		{
			"uint16 literal",
			expression.NewLiteral(uint16(5), types2.Uint16),
			5.0,
			false,
		},
		{
			"uint32 literal",
			expression.NewLiteral(uint32(5), types2.Uint32),
			5.0,
			false,
		},
		{
			"uint literal",
			expression.NewLiteral(uint(5), types2.Uint32),
			5.0,
			false,
		},
		{
			"uint64 literal",
			expression.NewLiteral(uint64(5), types2.Uint64),
			5.0,
			false,
		},
		{
			"bool literal",
			expression.NewLiteral(true, types2.Boolean),
			0.0,
			true,
		},
		{
			"float32 literal",
			expression.NewLiteral(float32(32.0), types2.Float32),
			32.0,
			false,
		},
		{
			"float64 literal",
			expression.NewLiteral(float64(32.0), types2.Float64),
			32.0,
			false,
		},
		{
			"string literal",
			expression.NewLiteral("-54.321", types2.Text),
			-54.321,
			false,
		},
		{
			"non numeric string",
			expression.NewLiteral("test", types2.Text),
			0,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := literalAsFloat64(test.l)
			assertOnUnexpectedErr(t, test.expectErr, err)

			if err == nil {
				assert.Equal(t, test.expected, res)
			}
		})
	}
}

func TestLiteralAsBool(t *testing.T) {
	tests := []struct {
		name      string
		l         *expression.Literal
		expected  bool
		expectErr bool
	}{
		{
			"int8 literal",
			expression.NewLiteral(int8(0), types2.Int8),
			false,
			false,
		},
		{
			"int16 literal",
			expression.NewLiteral(int16(1), types2.Int16),
			true,
			false,
		},
		{
			"int32 literal",
			expression.NewLiteral(int32(0), types2.Int32),
			false,
			false,
		},
		{
			"int literal",
			expression.NewLiteral(int(1), types2.Int32),
			true,
			false,
		},
		{
			"int64 literal",
			expression.NewLiteral(int64(0), types2.Int64),
			false,
			false,
		},

		{
			"uint8 literal",
			expression.NewLiteral(uint8(1), types2.Uint8),
			true,
			false,
		},
		{
			"uint16 literal",
			expression.NewLiteral(uint16(0), types2.Uint16),
			false,
			false,
		},
		{
			"uint32 literal",
			expression.NewLiteral(uint32(1), types2.Uint32),
			true,
			false,
		},
		{
			"uint literal",
			expression.NewLiteral(uint(0), types2.Uint32),
			false,
			false,
		},
		{
			"uint64 literal",
			expression.NewLiteral(uint64(1), types2.Uint64),
			true,
			false,
		},
		{
			"bool literal",
			expression.NewLiteral(true, types2.Boolean),
			true,
			false,
		},
		{
			"float literal not supported",
			expression.NewLiteral(float32(32.0), types2.Float32),
			false,
			true,
		},
		{
			"string literal false",
			expression.NewLiteral("false", types2.Text),
			false,
			false,
		},
		{
			"string literal 1",
			expression.NewLiteral("1", types2.Text),
			true,
			false,
		},
		{
			"non numeric non bool string",
			expression.NewLiteral("test", types2.Text),
			false,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := literalAsBool(test.l)
			assertOnUnexpectedErr(t, test.expectErr, err)

			if err == nil {
				assert.Equal(t, test.expected, res)
			}
		})
	}
}

func TestLiteralAsString(t *testing.T) {
	tests := []struct {
		name      string
		l         *expression.Literal
		expected  string
		expectErr bool
	}{
		{
			"int literal",
			expression.NewLiteral(5, types2.Int16),
			"5",
			false,
		},
		{
			"uint literal",
			expression.NewLiteral(uint32(5), types2.Uint32),
			"5",
			false,
		},
		{
			"bool literal",
			expression.NewLiteral(true, types2.Boolean),
			"true",
			false,
		},
		{
			"float literal",
			expression.NewLiteral(float32(-2.5), types2.Float32),
			"-2.5",
			false,
		},
		{
			"string literal",
			expression.NewLiteral("test", types2.Text),
			"test",
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := literalAsString(test.l)
			assertOnUnexpectedErr(t, test.expectErr, err)

			if err == nil {
				assert.Equal(t, test.expected, res)
			}
		})
	}
}

func TestParseDate(t *testing.T) {
	tests := []struct {
		name      string
		str       string
		expected  time.Time
		expectErr bool
	}{
		{
			"YYYY-MM-DD",
			"2006-01-02",
			time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC),
			false,
		},
		{
			"YYYY-MM-DD HH:MM:SS",
			"2006-01-02 15:04:05",
			time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC),
			false,
		},
		{
			"Invalid format",
			"not a date",
			time.Time{},
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := parseDate(test.str)
			assertOnUnexpectedErr(t, test.expectErr, err)

			if err == nil {
				assert.Equal(t, test.expected, res)
			}
		})
	}
}

func TestLiteralAsTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		l         *expression.Literal
		expected  time.Time
		expectErr bool
	}{
		{
			"YYYY-MM-DD",
			expression.NewLiteral("2006-01-02", types2.Text),
			time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC),
			false,
		},
		{
			"YYYY-MM-DD HH:MM:SS",
			expression.NewLiteral("2006-01-02 15:04:05", types2.Text),
			time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC),
			false,
		},
		{
			"Invalid format",
			expression.NewLiteral("not a date", types2.Text),
			time.Time{},
			true,
		},
		{
			"int literal",
			expression.NewLiteral(5, types2.Int8),
			time.Time{},
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := literalAsTimestamp(test.l)
			assertOnUnexpectedErr(t, test.expectErr, err)

			if err == nil {
				assert.Equal(t, test.expected, res)
			}
		})
	}
}

func TestLiteralToNomsValue(t *testing.T) {
	tests := []struct {
		name      string
		l         *expression.Literal
		expected  types.Value
		expectErr bool
	}{
		{"int", expression.NewLiteral(-1, types2.Int32), types.Int(-1), false},
		{"int err", expression.NewLiteral(uint64(0xFFFFFFFFFFFFFFFF), types2.Uint64), types.Int(0), true},
		{"uint", expression.NewLiteral(1, types2.Uint32), types.Uint(1), false},
		{"uint err", expression.NewLiteral(-1, types2.Int16), types.Uint(1), true},
		{"float", expression.NewLiteral(1.5, types2.Float32), types.Float(1.5), false},
		{"float err", expression.NewLiteral("not a valid float", types2.Text), types.Float(1.5), true},
		{"bool", expression.NewLiteral(true, types2.Boolean), types.Bool(true), false},
		{"bool err", expression.NewLiteral("not a valid bool", types2.Text), types.Bool(true), true},
		{"string", expression.NewLiteral("this is a test", types2.Text), types.String("this is a test"), false},
		{
			"date",
			expression.NewLiteral("1900-01-01", types2.Text),
			types.Timestamp(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)),
			false,
		},
		{
			"date err",
			expression.NewLiteral("not a valid date", types2.Text),
			types.Timestamp(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)),
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := LiteralToNomsValue(test.expected.Kind(), test.l)
			assertOnUnexpectedErr(t, test.expectErr, err)

			if err == nil {
				assert.Equal(t, test.expected, res)
			}
		})
	}
}
