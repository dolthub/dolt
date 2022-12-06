// Copyright 2022 Dolthub, Inc.
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

package branch_control

import (
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestSingleMatch(t *testing.T) {
	tests := []struct {
		expression string
		testStr    string
		matches    bool
		collation  sql.CollationID
	}{
		{"a__", "abc", true, sql.Collation_utf8mb4_0900_bin},
		{"a__", "abcd", false, sql.Collation_utf8mb4_0900_bin},
		{"a%b", "acb", true, sql.Collation_utf8mb4_0900_bin},
		{"a%b", "acdkeflskjfdklb", true, sql.Collation_utf8mb4_0900_bin},
		{"a%b", "ab", true, sql.Collation_utf8mb4_0900_bin},
		{"a%b", "a", false, sql.Collation_utf8mb4_0900_bin},
		{"a_b", "ab", false, sql.Collation_utf8mb4_0900_bin},
		{"aa:%", "aa:bb:cc:dd:ee:ff", true, sql.Collation_utf8mb4_0900_bin},
		{"aa:%", "AA:BB:CC:DD:EE:FF", false, sql.Collation_utf8mb4_0900_bin},
		{"aa:%", "AA:BB:CC:DD:EE:FF", true, sql.Collation_utf8mb4_0900_ai_ci},
		{"a_%_b%_%c", "AaAbCc", true, sql.Collation_utf8mb4_0900_ai_ci},
		{"a_%_b%_%c", "AaAbBcCbCc", true, sql.Collation_utf8mb4_0900_ai_ci},
		{"a_%_b%_%c", "AbbbbC", true, sql.Collation_utf8mb4_0900_ai_ci},
		{"a_%_n%_%z", "aBcDeFgHiJkLmNoPqRsTuVwXyZ", true, sql.Collation_utf8mb4_0900_ai_ci},
		{`a\%b`, "acb", false, sql.Collation_utf8mb4_0900_bin},
		{`a\%b`, "a%b", true, sql.Collation_utf8mb4_0900_bin},
		{`a\%b`, "A%B", false, sql.Collation_utf8mb4_0900_bin},
		{`a\%b`, "A%B", true, sql.Collation_utf8mb4_0900_ai_ci},
		{`a`, "a", true, sql.Collation_utf8mb4_0900_bin},
		{`ab`, "a", false, sql.Collation_utf8mb4_0900_bin},
		{`a\b`, "a", false, sql.Collation_utf8mb4_0900_bin},
		{`a\\b`, "a", false, sql.Collation_utf8mb4_0900_bin},
		{`a\\\b`, "a", false, sql.Collation_utf8mb4_0900_bin},
		{`a`, "a", true, sql.Collation_utf8mb4_0900_ai_ci},
		{`ab`, "a", false, sql.Collation_utf8mb4_0900_ai_ci},
		{`a\b`, "a", false, sql.Collation_utf8mb4_0900_ai_ci},
		{`a\\b`, "a", false, sql.Collation_utf8mb4_0900_ai_ci},
		{`a\\\b`, "a", false, sql.Collation_utf8mb4_0900_ai_ci},
		{`A%%%%`, "abc", true, sql.Collation_utf8mb4_0900_ai_ci},
		{`A%%%%bc`, "abc", true, sql.Collation_utf8mb4_0900_ai_ci},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%q matches %q", test.testStr, test.expression), func(t *testing.T) {
			parsedExpression := ParseExpression(FoldExpression(test.expression), test.collation)
			matchCount := Match([]MatchExpression{{0, parsedExpression}}, test.testStr, test.collation)
			if test.matches {
				require.Len(t, matchCount, 1)
			} else {
				require.Len(t, matchCount, 0)
			}
		})
	}
}

func TestMultipleMatch(t *testing.T) {
	collation := sql.Collation_utf8mb4_0900_ai_ci
	matchExprs := []MatchExpression{
		{0, ParseExpression(FoldExpression("a__"), collation)},
		{1, ParseExpression(FoldExpression("a%b"), collation)},
		{2, ParseExpression(FoldExpression("a_b"), collation)},
		{3, ParseExpression(FoldExpression("aa:%"), collation)},
		{4, ParseExpression(FoldExpression("a_%_b%_%c"), collation)},
		{5, ParseExpression(FoldExpression(`a\%b`), collation)},
		{6, ParseExpression(FoldExpression(`a`), collation)},
		{7, ParseExpression(FoldExpression(`ab`), collation)},
		{8, ParseExpression(FoldExpression(`a\\b`), collation)},
		{9, ParseExpression(FoldExpression(`A%%%%`), collation)},
		{10, ParseExpression(FoldExpression(`A%%%%bc`), collation)},
		{11, ParseExpression(FoldExpression("a_%_b%_%c%"), collation)},
		{12, ParseExpression(FoldExpression("a_%_n%_%z"), collation)},
	}
	tests := []struct {
		testStr string
		indexes []uint32
	}{
		{"a", []uint32{6, 9}},
		{"ab", []uint32{1, 7, 9}},
		{"abc", []uint32{0, 9, 10}},
		{"acb", []uint32{0, 1, 2, 9}},
		{"abcd", []uint32{9}},
		{"acdkeflskjfdklb", []uint32{1, 9}},
		{"acdkeflskjfdklbc", []uint32{9, 10}},
		{"aa:bb:cc:dd:ee:ff", []uint32{3, 9, 11}},
		{"AA:BB:CC:DD:EE:FF", []uint32{3, 9, 11}},
		{"AaAbCc", []uint32{4, 9, 11}},
		{"AaAbBcCbCc", []uint32{4, 9, 11}},
		{"AbbbbC", []uint32{4, 9, 10, 11}},
		{"aBcDeFgHiJkLmNoPqRsTuVwXyZ", []uint32{9, 12}},
		{"a%b", []uint32{0, 1, 2, 5, 9}},
		{"A%B", []uint32{0, 1, 2, 5, 9}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%q", test.testStr), func(t *testing.T) {
			actualMatches := Match(matchExprs, test.testStr, collation)
			require.ElementsMatch(t, test.indexes, actualMatches)
		})
	}
}

func BenchmarkSimpleCase(b *testing.B) {
	collation := sql.Collation_utf8mb4_0900_ai_ci
	matchExprs := []MatchExpression{
		{0, ParseExpression(FoldExpression("a__"), collation)},
		{1, ParseExpression(FoldExpression("a%b"), collation)},
		{2, ParseExpression(FoldExpression("a_b"), collation)},
		{3, ParseExpression(FoldExpression("aa:%"), collation)},
		{4, ParseExpression(FoldExpression("a_%_b%_%c"), collation)},
		{5, ParseExpression(FoldExpression(`a\%b`), collation)},
		{6, ParseExpression(FoldExpression(`a`), collation)},
		{7, ParseExpression(FoldExpression(`ab`), collation)},
		{8, ParseExpression(FoldExpression(`a\\b`), collation)},
		{9, ParseExpression(FoldExpression(`A%%%%`), collation)},
		{10, ParseExpression(FoldExpression(`A%%%%bc`), collation)},
		{11, ParseExpression(FoldExpression("a_%_b%_%c%"), collation)},
		{12, ParseExpression(FoldExpression("a_%_n%_%z"), collation)},
	}
	tests := []struct {
		testStr string
		matches []uint32
	}{
		{"a", []uint32{6, 9}},
		{"ab", []uint32{1, 7, 9}},
		{"abc", []uint32{0, 9, 10}},
		{"acb", []uint32{0, 1, 2, 9}},
		{"abcd", []uint32{9}},
		{"acdkeflskjfdklb", []uint32{1, 9}},
		{"acdkeflskjfdklbc", []uint32{9, 10}},
		{"aa:bb:cc:dd:ee:ff", []uint32{3, 9, 11}},
		{"AA:BB:CC:DD:EE:FF", []uint32{3, 9, 11}},
		{"AaAbCc", []uint32{4, 9, 11}},
		{"AaAbBcCbCc", []uint32{4, 9, 11}},
		{"AbbbbC", []uint32{4, 9, 10, 11}},
		{"aBcDeFgHiJkLmNoPqRsTuVwXyZ", []uint32{9, 12}},
		{"a%b", []uint32{0, 1, 2, 5, 9}},
		{"A%B", []uint32{0, 1, 2, 5, 9}},
	}
	testLen := len(tests)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		test := tests[i%testLen]
		indexes := Match(matchExprs, test.testStr, collation)
		indexPool.Put(indexes)
	}
	b.ReportAllocs()
}
