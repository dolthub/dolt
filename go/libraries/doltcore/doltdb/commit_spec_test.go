package doltdb

import (
	"github.com/attic-labs/noms/go/hash"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/test"
	"testing"
)

func TestBranchRegex(t *testing.T) {
	tests := map[string]bool{
		"branch":                     true,
		"my-branch-with-dashes":      true,
		"my-branch-with_underscores": true,
		"ab":                         true,
		"a":                          false,
		"__create__":                 false,
		"__bad-start":                false,
		"bad-end__":                  false,
		"invalid+char":               false,
		"":                           false,
	}

	for k, v := range tests {
		if userBranchRegex.MatchString(k) != v {
			t.Error(k)
		}
	}
}

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
		{"00000000000000000000000000000000", "", "00000000000000000000000000000000", "", false},
		{"head^~2", "master", "refs/heads/master", "^~2", false},
		{"__invalid__^~2", "", "", "", true},
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
