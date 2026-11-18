// Copyright 2023 Dolthub, Inc.
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

package schema

import (
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

func TestCompareCollatedStrings(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/12342
	tests := []struct {
		collation sql.CollationID
		left      []byte
		right     []byte
		exp       int
	}{
		{
			collation: sql.Collation_utf8mb4_0900_ai_ci,
			left:      []byte("Hello, 人"),
			right:     []byte("Hello, 亻"),
			exp:       -1,
		},
		{
			collation: sql.Collation_utf8mb4_0900_ai_ci,
			left:      []byte("woÒ"),
			right:     []byte("woÓ"),
			exp:       0,
		},
		{
			collation: sql.Collation_utf8mb4_0900_ai_ci,
			left:      []byte("\u07FB"),
			right:     []byte("\u07FC"),
			exp:       -1,
		},
		{
			collation: sql.Collation_utf8mb4_0900_ai_ci,
			left:      []byte("˧"),
			right:     []byte("˦"),
			exp:       1,
		},
		{
			collation: sql.Collation_utf8mb4_0900_ai_ci,
			left:      []byte("ƵƶzƸ"),
			right:     []byte("ƵƶzƷ"),
			exp:       1,
		},
		{
			collation: sql.Collation_utf8mb4_unicode_ci,
			left:      []byte("b\u200bob"),
			right:     []byte("bob"),
			exp:       0,
		},
		{
			collation: sql.Collation_utf8mb4_unicode_ci,
			left:      []byte("bob"),
			right:     []byte("bob\u200b"),
			exp:       0,
		},
		{
			collation: sql.Collation_utf8mb4_unicode_ci,
			left:      []byte("\u200ba"),
			right:     []byte("\u200ca"),
			exp:       0,
		},
		{
			collation: sql.Collation_utf8mb4_unicode_ci,
			left:      []byte("\u200b"),
			right:     []byte(""),
			exp:       0,
		},
		{
			collation: sql.Collation_utf8mb4_0900_as_cs,
			left:      []byte("b\u200bob"),
			right:     []byte("bob"),
			exp:       0,
		},
		{
			collation: sql.Collation_utf8mb4_general_ci,
			left:      []byte("b\u200bob"),
			right:     []byte("bob"),
			exp:       1,
		},
		{
			collation: sql.Collation_utf8mb4_general_ci,
			left:      []byte("b\x00ob"),
			right:     []byte("bob"),
			exp:       -1,
		},
		{
			collation: sql.Collation_utf8mb4_unicode_ci,
			left:      []byte{0xe2, 0x80},
			right:     []byte("\u200b"),
			exp:       1,
		},
		{
			collation: sql.Collation_utf8mb4_unicode_ci,
			left:      []byte("b\xff\u200bob"),
			right:     []byte("b\ufffdob"),
			exp:       0,
		},
		{
			collation: sql.Collation_utf8mb4_unicode_ci,
			left:      []byte("a\xffz"),
			right:     []byte("a\xfeb"),
			exp:       1,
		},
		{
			collation: sql.Collation_utf8mb4_unicode_ci,
			left:      []byte("a\xffz"),
			right:     []byte("a\xffb"),
			exp:       1,
		},
		{
			collation: sql.Collation_utf8mb4_unicode_ci,
			left:      []byte("a\xfeb"),
			right:     []byte("a\xffb"),
			exp:       0,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s:%x vs %x", tt.collation, tt.left, tt.right), func(t *testing.T) {
			cmp := val.CompareCollatedStrings(tt.collation, tt.left, tt.right)
			require.Equal(t, tt.exp, cmp)
			rev := val.CompareCollatedStrings(tt.collation, tt.right, tt.left)
			require.Equal(t, -tt.exp, rev)
		})
	}

	orderedSamples := [][]byte{
		[]byte("\t"),
		[]byte("\n"),
		[]byte(" "),
		[]byte("!"),
		[]byte("0"),
		[]byte("a"),
		[]byte("o"),
		[]byte("z"),
		[]byte("人"),
		[]byte("\U00020000"),
	}
	for _, coll := range []sql.CollationID{
		sql.Collation_utf8mb4_unicode_ci,
		sql.Collation_utf8mb4_unicode_520_ci,
		sql.Collation_utf8mb4_persian_ci,
		sql.Collation_utf8mb4_sinhala_ci,
		sql.Collation_utf8mb4_0900_ai_ci,
	} {
		for i := 0; i < len(orderedSamples); i++ {
			require.Zero(t, val.CompareCollatedStrings(coll, orderedSamples[i], orderedSamples[i]), "%s:%q", coll, orderedSamples[i])
			for j := i + 1; j < len(orderedSamples); j++ {
				require.Equal(t, -1, val.CompareCollatedStrings(coll, orderedSamples[i], orderedSamples[j]), "%s:%q < %q", coll, orderedSamples[i], orderedSamples[j])
				require.Equal(t, 1, val.CompareCollatedStrings(coll, orderedSamples[j], orderedSamples[i]), "%s:%q > %q", coll, orderedSamples[j], orderedSamples[i])
			}
		}
	}
}
