// Copyright 2020 Liquidata, Inc.
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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestInlineBlobConvertNomsValueToValue(t *testing.T) {
	tests := []struct {
		input  types.InlineBlob
		output string
	}{
		{
			[]byte("hi"),
			"hi",
		},
		{
			[]byte("hello there"),
			"hello there",
		},
		{
			[]byte("هذا هو بعض نماذج النص التي أستخدمها لاختبار عناصر"),
			"هذا هو بعض نماذج النص التي أستخدمها لاختبار عناصر",
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, InlineBlobType.String(), test.input), func(t *testing.T) {
			output, err := InlineBlobType.ConvertNomsValueToValue(test.input)
			require.NoError(t, err)
			require.Equal(t, test.output, output)
		})
	}
}

func TestInlineBlobConvertValueToNomsValue(t *testing.T) {
	tests := []struct {
		input       interface{}
		output      types.InlineBlob
		expectedErr bool
	}{
		{
			int16(25),
			[]byte("25"),
			false,
		},
		{
			uint64(287946293486),
			[]byte("287946293486"),
			false,
		},
		{
			float32(78.25),
			[]byte("78.25"),
			false,
		},
		{
			"something",
			[]byte("something"),
			false,
		},
		{
			time.Date(1880, 1, 2, 3, 4, 5, 0, time.UTC),
			[]byte("1880-01-02 03:04:05"),
			false,
		},
		{
			complex128(32),
			[]byte{},
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, InlineBlobType.String(), test.input), func(t *testing.T) {
			output, err := InlineBlobType.ConvertValueToNomsValue(test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestInlineBlobFormatValue(t *testing.T) {
	tests := []struct {
		input  types.InlineBlob
		output string
	}{
		{
			[]byte("hi"),
			"hi",
		},
		{
			[]byte("hello there"),
			"hello there",
		},
		{
			[]byte("هذا هو بعض نماذج النص التي أستخدمها لاختبار عناصر"),
			"هذا هو بعض نماذج النص التي أستخدمها لاختبار عناصر",
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, InlineBlobType.String(), test.input), func(t *testing.T) {
			output, err := InlineBlobType.FormatValue(test.input)
			require.NoError(t, err)
			require.Equal(t, test.output, *output)
		})
	}
}

func TestInlineBlobParseValue(t *testing.T) {
	tests := []struct {
		input  string
		output types.InlineBlob
	}{
		{
			"hi",
			[]byte("hi"),
		},
		{
			"hello there",
			[]byte("hello there"),
		},
		{
			"هذا هو بعض نماذج النص التي أستخدمها لاختبار عناصر",
			[]byte("هذا هو بعض نماذج النص التي أستخدمها لاختبار عناصر"),
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, InlineBlobType.String(), test.input), func(t *testing.T) {
			output, err := InlineBlobType.ParseValue(&test.input)
			require.NoError(t, err)
			assert.Equal(t, test.output, output)
		})
	}
}
