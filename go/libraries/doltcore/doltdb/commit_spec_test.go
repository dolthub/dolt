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

package doltdb

import (
	"testing"

	"github.com/liquidata-inc/dolt/go/libraries/utils/test"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

func TestCommitRegex(t *testing.T) {
	for i := 0; i < 32; i++ {
		data := test.RandomData(hash.ByteLen)

		var hashVal hash.Hash
		copy(hashVal[:], data)

		hashStr := hashVal.String()

		if !hashRegex.MatchString(hashStr) {
			t.Error(hashStr, ": random hash failed to match hash regex.")
		}
	}
}

func TestNewCommitSpec(t *testing.T) {
	tests := []struct {
		inputStr        string
		cwbName         string
		expectedRefStr  string
		expecteASpecStr string
		expectErr       bool
	}{
		{"master", "", "refs/heads/master", "", false},
		{"refs/heads/master", "", "refs/heads/master", "", false},
		{"head", "refs/heads/master", "head", "", false},
		{"head", "refs/heads/master", "head", "", false},
		{"head^~2", "master", "head", "^~2", false},
		{"00000000000000000000000000000000", "", "00000000000000000000000000000000", "", false},
		{"head", "", "head", "", true},
	}

	for _, test := range tests {
		cs, err := NewCommitSpec(test.inputStr, test.cwbName)

		if err != nil {
			if !test.expectErr {
				t.Error(test.inputStr, "Error didn't match expected.  Errored: ", err != nil)
			}
		} else if cs.CommitStringer.String() != test.expectedRefStr {
			t.Error(test.inputStr, "expected name:", test.expectedRefStr, "actual name:", cs.CommitStringer.String())
		} else if cs.ASpec.SpecStr != test.expecteASpecStr {
			t.Error(test.inputStr, "expected ancestor spec:", test.expecteASpecStr, "actual ancestor spec:", cs.ASpec.SpecStr)
		}
	}
}
