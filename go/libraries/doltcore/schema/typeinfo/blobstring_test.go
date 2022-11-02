// Copyright 2021 Dolthub, Inc.
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

package typeinfo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestBlobStringConvertNomsValueToValue(t *testing.T) {
	tests := []struct {
		typ         *blobStringType
		input       types.Blob
		output      string
		expectedErr bool
	}{
		{
			generateBlobStringType(t, 10),
			mustBlobString(t, "0  "),
			"0  ",
			false,
		},
		{
			generateBlobStringType(t, 80),
			mustBlobString(t, "this is some text that will be returned"),
			"this is some text that will be returned",
			false,
		},
		{
			&blobStringType{sql.CreateLongText(sql.Collation_Default)},
			mustBlobString(t, "  This is a sentence.  "),
			"  This is a sentence.  ",
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, test.typ.String(), test.input), func(t *testing.T) {
			output, err := test.typ.ConvertNomsValueToValue(test.input)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, output)
			}
		})
	}
}

func TestBlobStringConvertValueToNomsValue(t *testing.T) {
	tests := []struct {
		typ         *blobStringType
		input       interface{}
		output      types.Blob
		expectedErr bool
	}{
		{
			generateBlobStringType(t, 10),
			"0  ",
			mustBlobString(t, "0  "),
			false,
		},
		{
			generateBlobStringType(t, 80),
			int64(28354),
			mustBlobString(t, "28354"),
			false,
		},
		{
			&blobStringType{sql.CreateLongText(sql.Collation_Default)},
			float32(3724.75),
			mustBlobString(t, "3724.75"),
			false,
		},
		{
			generateBlobStringType(t, 80),
			time.Date(2030, 1, 2, 4, 6, 3, 472382485, time.UTC),
			mustBlobString(t, "2030-01-02 04:06:03.472382"),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, test.typ.String(), test.input), func(t *testing.T) {
			vrw := types.NewMemoryValueStore()
			output, err := test.typ.ConvertValueToNomsValue(context.Background(), vrw, test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestBlobStringFormatValue(t *testing.T) {
	tests := []struct {
		typ         *blobStringType
		input       types.Blob
		output      string
		expectedErr bool
	}{
		{
			generateBlobStringType(t, 10),
			mustBlobString(t, "0  "),
			"0  ",
			false,
		},
		{
			generateBlobStringType(t, 80),
			mustBlobString(t, "this is some text that will be returned"),
			"this is some text that will be returned",
			false,
		},
		{
			&blobStringType{sql.CreateLongText(sql.Collation_Default)},
			mustBlobString(t, "  This is a sentence.  "),
			"  This is a sentence.  ",
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, test.typ.String(), test.input), func(t *testing.T) {
			output, err := test.typ.FormatValue(test.input)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, *output)
			}
		})
	}
}

func TestBlobStringParseValue(t *testing.T) {
	tests := []struct {
		typ         *blobStringType
		input       string
		output      types.Blob
		expectedErr bool
	}{
		{
			generateBlobStringType(t, 10),
			"0  ",
			mustBlobString(t, "0  "),
			false,
		},
		{
			generateBlobStringType(t, 80),
			"this is some text that will be returned",
			mustBlobString(t, "this is some text that will be returned"),
			false,
		},
		{
			&blobStringType{sql.CreateLongText(sql.Collation_Default)},
			"  This is a sentence.  ",
			mustBlobString(t, "  This is a sentence.  "),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, test.typ.String(), test.input), func(t *testing.T) {
			vrw := types.NewMemoryValueStore()
			output, err := StringDefaultType.ConvertToType(context.Background(), vrw, test.typ, types.String(test.input))
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
