package doltdb

import (
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/test"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
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
		{"head", "refs/heads/master", "refs/heads/master", "", false},
		{"head", "refs/heads/master", "refs/heads/master", "", false},
		{"head^~2", "master", "refs/heads/master", "^~2", false},
		{"00000000000000000000000000000000", "", "00000000000000000000000000000000", "", false},
		{"head", "", "", "", true},
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
