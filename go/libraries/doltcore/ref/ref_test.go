package ref

import (
	"encoding/json"
	"testing"
)

type TestMarshalStruct struct {
	Test DoltRef `json:"test"`
}

func TestJsonMarshalAndUnmarshal(t *testing.T) {
	tests := []struct {
		dr  DoltRef
		str string
	}{
		{
			NewBranchRef("master"),
			`{"test":"refs/heads/master"}`,
		},
		{
			NewRemoteRef("origin", "master"),
			`{"test":"refs/remotes/origin/master"}`,
		},
		{
			NewInternalRef("create"),
			`{"test":"refs/internal/create"}`,
		},
	}

	for _, test := range tests {
		tms := TestMarshalStruct{test.dr}
		data, err := json.Marshal(tms)
		actual := string(data)

		if err != nil {
			t.Error(err)
		} else if test.str != actual {
			t.Error(actual, "!=", test.str)
		}
	}

	for _, test := range tests {
		var tms TestMarshalStruct
		err := json.Unmarshal([]byte(test.str), &tms)

		if err != nil {
			t.Error(err)
		} else if !test.dr.Equals(tms.Test) {
			t.Error(tms.Test, "!=", test.dr)
		}
	}
}

func TestEqualsStr(t *testing.T) {
	tests := []struct {
		dr       DoltRef
		cmp      string
		expected bool
	}{
		{
			NewBranchRef("master"),
			"refs/heads/master",
			true,
		},
		{
			NewBranchRef("master"),
			"refs/heads/notmaster",
			false,
		},
		{
			NewBranchRef("master"),
			"refs/remotes/origin/master",
			false,
		},
		{
			NewRemoteRef("origin", "master"),
			"refs/remotes/origin/master",
			true,
		},
		{
			NewRemoteRefFromPathStr("origin/master"),
			"refs/remotes/origin/master",
			true,
		},
		{
			NewRemoteRef("origin", "master"),
			"refs/remotes/borigin/master",
			false,
		},
		{
			NewRemoteRef("origin", "master"),
			"refs/remotes/origin/notmaster",
			false,
		},
		{
			NewRemoteRef("origin", "master"),
			"refs/notavalidtype/origin/notmaster",
			false,
		},
	}

	for _, test := range tests {
		actual := test.dr.EqualsStr(test.cmp)

		if actual != test.expected {
			t.Error("error comparing", test.dr, "to", test.cmp)
		}
	}
}
