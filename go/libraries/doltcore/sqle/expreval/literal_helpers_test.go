// Copyright 2019-2020 Liquidata, Inc.
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

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/store/types"
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
			expression.NewLiteral(int8(5), sql.Int8),
			5,
			false,
		},
		{
			"int16 literal",
			expression.NewLiteral(int16(5), sql.Int16),
			5,
			false,
		},
		{
			"int32 literal",
			expression.NewLiteral(int32(5), sql.Int32),
			5,
			false,
		},
		{
			"int literal",
			expression.NewLiteral(int(5), sql.Int32),
			5,
			false,
		},
		{
			"int64 literal",
			expression.NewLiteral(int64(5), sql.Int64),
			5,
			false,
		},

		{
			"uint8 literal",
			expression.NewLiteral(uint8(5), sql.Uint8),
			5,
			false,
		},
		{
			"uint16 literal",
			expression.NewLiteral(uint16(5), sql.Uint16),
			5,
			false,
		},
		{
			"uint32 literal",
			expression.NewLiteral(uint32(5), sql.Uint32),
			5,
			false,
		},
		{
			"uint literal",
			expression.NewLiteral(uint(5), sql.Uint32),
			5,
			false,
		},
		{
			"uint64 literal",
			expression.NewLiteral(uint64(5), sql.Uint64),
			5,
			false,
		},
		{
			"true literal",
			expression.NewLiteral(true, sql.Boolean),
			1,
			false,
		},
		{
			"false literal",
			expression.NewLiteral(false, sql.Boolean),
			0,
			false,
		},
		{
			"float32 literal",
			expression.NewLiteral(float32(32.0), sql.Float32),
			32,
			false,
		},
		{
			"float64 literal",
			expression.NewLiteral(float64(32.0), sql.Float64),
			32,
			false,
		},
		{
			"string literal",
			expression.NewLiteral("54321", sql.Text),
			54321,
			false,
		},
		{
			"uint literal too big",
			expression.NewLiteral(uint64(0xFFFFFFFFFFFFFFFF), sql.Uint32),
			0,
			true,
		},
		{
			"float64 with fractional portion",
			expression.NewLiteral(float64(5.0005), sql.Float64),
			0,
			true,
		},
		{
			"float32 with fractional portion",
			expression.NewLiteral(float32(5.0005), sql.Float32),
			0,
			true,
		},
		{
			"string not a number",
			expression.NewLiteral("not a number", sql.Text),
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
			expression.NewLiteral(int8(5), sql.Int8),
			5,
			false,
		},
		{
			"int16 literal",
			expression.NewLiteral(int16(5), sql.Int16),
			5,
			false,
		},
		{
			"int32 literal",
			expression.NewLiteral(int32(5), sql.Int32),
			5,
			false,
		},
		{
			"int literal",
			expression.NewLiteral(int(5), sql.Int32),
			5,
			false,
		},
		{
			"int64 literal",
			expression.NewLiteral(int64(5), sql.Int64),
			5,
			false,
		},
		{
			"uint8 literal",
			expression.NewLiteral(uint8(5), sql.Uint8),
			5,
			false,
		},
		{
			"uint16 literal",
			expression.NewLiteral(uint16(5), sql.Uint16),
			5,
			false,
		},
		{
			"uint32 literal",
			expression.NewLiteral(uint32(5), sql.Uint32),
			5,
			false,
		},
		{
			"uint literal",
			expression.NewLiteral(uint(5), sql.Uint32),
			5,
			false,
		},
		{
			"uint64 literal",
			expression.NewLiteral(uint64(5), sql.Uint64),
			5,
			false,
		},
		{
			"true literal",
			expression.NewLiteral(true, sql.Boolean),
			1,
			false,
		},
		{
			"false literal",
			expression.NewLiteral(false, sql.Boolean),
			0,
			false,
		},
		{
			"float32 literal",
			expression.NewLiteral(float32(32.0), sql.Float32),
			32,
			false,
		},
		{
			"float64 literal",
			expression.NewLiteral(float64(32.0), sql.Float64),
			32,
			false,
		},
		{
			"string literal",
			expression.NewLiteral("54321", sql.Text),
			54321,
			false,
		},
		{
			"negative int8 literal",
			expression.NewLiteral(int8(-1), sql.Int8),
			0,
			true,
		},
		{
			"negative int16 literal",
			expression.NewLiteral(int16(-1), sql.Int16),
			0,
			true,
		},
		{
			"negative int32 literal",
			expression.NewLiteral(int32(-1), sql.Int32),
			0,
			true,
		},
		{
			"negative int literal",
			expression.NewLiteral(int(-1), sql.Int32),
			0,
			true,
		},
		{
			"negative int64 literal",
			expression.NewLiteral(int64(-1), sql.Int64),
			0,
			true,
		},
		{
			"float32 with fractional portion",
			expression.NewLiteral(float32(5.0005), sql.Float32),
			0,
			true,
		},
		{
			"float64 with fractional portion",
			expression.NewLiteral(float64(5.0005), sql.Float64),
			0,
			true,
		},
		{
			"string not a number",
			expression.NewLiteral("not a number", sql.Text),
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
			expression.NewLiteral(int8(-5), sql.Int8),
			-5.0,
			false,
		},
		{
			"int16 literal",
			expression.NewLiteral(int16(-5), sql.Int16),
			-5.0,
			false,
		},
		{
			"int32 literal",
			expression.NewLiteral(int32(-5), sql.Int32),
			-5.0,
			false,
		},
		{
			"int literal",
			expression.NewLiteral(int(-5), sql.Int32),
			-5.0,
			false,
		},
		{
			"int64 literal",
			expression.NewLiteral(int64(-5), sql.Int64),
			-5.0,
			false,
		},
		{
			"uint8 literal",
			expression.NewLiteral(uint8(5), sql.Uint8),
			5.0,
			false,
		},
		{
			"uint16 literal",
			expression.NewLiteral(uint16(5), sql.Uint16),
			5.0,
			false,
		},
		{
			"uint32 literal",
			expression.NewLiteral(uint32(5), sql.Uint32),
			5.0,
			false,
		},
		{
			"uint literal",
			expression.NewLiteral(uint(5), sql.Uint32),
			5.0,
			false,
		},
		{
			"uint64 literal",
			expression.NewLiteral(uint64(5), sql.Uint64),
			5.0,
			false,
		},
		{
			"bool literal",
			expression.NewLiteral(true, sql.Boolean),
			0.0,
			true,
		},
		{
			"float32 literal",
			expression.NewLiteral(float32(32.0), sql.Float32),
			32.0,
			false,
		},
		{
			"float64 literal",
			expression.NewLiteral(float64(32.0), sql.Float64),
			32.0,
			false,
		},
		{
			"string literal",
			expression.NewLiteral("-54.321", sql.Text),
			-54.321,
			false,
		},
		{
			"non numeric string",
			expression.NewLiteral("test", sql.Text),
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
			expression.NewLiteral(int8(0), sql.Int8),
			false,
			false,
		},
		{
			"int16 literal",
			expression.NewLiteral(int16(1), sql.Int16),
			true,
			false,
		},
		{
			"int32 literal",
			expression.NewLiteral(int32(0), sql.Int32),
			false,
			false,
		},
		{
			"int literal",
			expression.NewLiteral(int(1), sql.Int32),
			true,
			false,
		},
		{
			"int64 literal",
			expression.NewLiteral(int64(0), sql.Int64),
			false,
			false,
		},

		{
			"uint8 literal",
			expression.NewLiteral(uint8(1), sql.Uint8),
			true,
			false,
		},
		{
			"uint16 literal",
			expression.NewLiteral(uint16(0), sql.Uint16),
			false,
			false,
		},
		{
			"uint32 literal",
			expression.NewLiteral(uint32(1), sql.Uint32),
			true,
			false,
		},
		{
			"uint literal",
			expression.NewLiteral(uint(0), sql.Uint32),
			false,
			false,
		},
		{
			"uint64 literal",
			expression.NewLiteral(uint64(1), sql.Uint64),
			true,
			false,
		},
		{
			"bool literal",
			expression.NewLiteral(true, sql.Boolean),
			true,
			false,
		},
		{
			"float literal not supported",
			expression.NewLiteral(float32(32.0), sql.Float32),
			false,
			true,
		},
		{
			"string literal false",
			expression.NewLiteral("false", sql.Text),
			false,
			false,
		},
		{
			"string literal 1",
			expression.NewLiteral("1", sql.Text),
			true,
			false,
		},
		{
			"non numeric non bool string",
			expression.NewLiteral("test", sql.Text),
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
			expression.NewLiteral(5, sql.Int16),
			"5",
			false,
		},
		{
			"uint literal",
			expression.NewLiteral(uint32(5), sql.Uint32),
			"5",
			false,
		},
		{
			"bool literal",
			expression.NewLiteral(true, sql.Boolean),
			"true",
			false,
		},
		{
			"float literal",
			expression.NewLiteral(float32(-2.5), sql.Float32),
			"-2.5",
			false,
		},
		{
			"string literal",
			expression.NewLiteral("test", sql.Text),
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
			expression.NewLiteral("2006-01-02", sql.Text),
			time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC),
			false,
		},
		{
			"YYYY-MM-DD HH:MM:SS",
			expression.NewLiteral("2006-01-02 15:04:05", sql.Text),
			time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC),
			false,
		},
		{
			"Invalid format",
			expression.NewLiteral("not a date", sql.Text),
			time.Time{},
			true,
		},
		{
			"int literal",
			expression.NewLiteral(5, sql.Int8),
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
		{"int", expression.NewLiteral(-1, sql.Int32), types.Int(-1), false},
		{"int err", expression.NewLiteral(uint64(0xFFFFFFFFFFFFFFFF), sql.Uint64), types.Int(0), true},
		{"uint", expression.NewLiteral(1, sql.Uint32), types.Uint(1), false},
		{"uint err", expression.NewLiteral(-1, sql.Int16), types.Uint(1), true},
		{"float", expression.NewLiteral(1.5, sql.Float32), types.Float(1.5), false},
		{"float err", expression.NewLiteral("not a valid float", sql.Text), types.Float(1.5), true},
		{"bool", expression.NewLiteral(true, sql.Boolean), types.Bool(true), false},
		{"bool err", expression.NewLiteral("not a valid bool", sql.Text), types.Bool(true), true},
		{"string", expression.NewLiteral("this is a test", sql.Text), types.String("this is a test"), false},
		{
			"date",
			expression.NewLiteral("1900-01-01", sql.Text),
			types.Timestamp(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)),
			false,
		},
		{
			"date err",
			expression.NewLiteral("not a valid date", sql.Text),
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
