// Copyright 2019 Liquidata, Inc.
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

package set

import "testing"

const (
	asciiDecimalNumbers = "0123456789"
)

func TestByteSet(t *testing.T) {
	asciiDecimalBS := NewByteSet([]byte(asciiDecimalNumbers))

	if !asciiDecimalBS.Contains('0') {
		t.Error("Should have 0.")
	}

	if asciiDecimalBS.Contains('a') {
		t.Error("Should not have 'a'")
	}

	if !asciiDecimalBS.ContainsAll([]byte("8675309")) {
		t.Error("Should contain all of this number string.")
	}

	if asciiDecimalBS.ContainsAll([]byte("0x1234")) {
		t.Error("Should not contain the x from this string")
	}
}
