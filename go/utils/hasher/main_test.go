// Copyright 2025 Dolthub, Inc.
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

package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrintHashToBytes(t *testing.T) {
	check := func(in string, out string) {
		var buf bytes.Buffer
		PrintHashToBytes(in, &buf)
		assert.Equal(t, out, buf.String())
	}
	for _, testCase := range [][2]string{
		{"128", "Invalid hash\n"},
		{"72ef2eng5q79mi394elksvrr86mk20rb", "[56 156 241 58 240 46 142 155 72 105 35 171 78 127 123 65 173 65 3 107]\n"},
		{"72ef2eng5q79mi394elksvrr86mk20r", "Invalid hash\n"},
		{"72ef2ezg5q79mi394elksvrr86mk20rb", "Invalid hash\n"},
		{"72ef2eng5q79mi394elksvrr86mk20rb4", "Invalid hash\n"},
	} {
		check(testCase[0], testCase[1])
	}
}

func TestPrintBytesToHash(t *testing.T) {
	check := func(out string, in []string) {
		var buf bytes.Buffer
		PrintBytesToHash(in, &buf)
		assert.Equal(t, out, buf.String())
	}
	for _, testCase := range [][]string{
		{"Too many bytes given.\n", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32"},
		{"40g2081040g2081040g2081040g2087v\n", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "255"},
		{"40g2081040g2081040g2081040g21vo0\n", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "255"},
		{"40g2081040g2081040g2081040gfu000\n", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "255"},
		{"vvvg0000000000000000000000000000\n", "255", "255"},
		{"strconv.ParseUint: parsing \"-1\": invalid syntax\n", "32", "32", "32", "32", "32", "-1", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "255"},
		{"strconv.ParseUint: parsing \"256\": value out of range\n", "32", "32", "32", "32", "32", "256", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "255"},
		{"strconv.ParseUint: parsing \"wat\": invalid syntax\n", "32", "32", "32", "32", "32", "wat", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "32", "255"},
	} {
		check(testCase[0], testCase[1:])
	}
}
