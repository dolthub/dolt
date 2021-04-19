// Copyright 2019 Dolthub, Inc.
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

package ref

import (
	"encoding/json"
	"testing"
)

type TestMarshalStruct struct {
	Test MarshalableRef `json:"test"`
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
		{
			NewWorkspaceRef("newworkspace"),
			`{"test":"refs/workspaces/newworkspace"}`,
		},
	}

	for _, test := range tests {
		tms := TestMarshalStruct{MarshalableRef{test.dr}}
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
		} else if !Equals(test.dr, tms.Test.Ref) {
			t.Error(tms.Test, "!=", test.dr)
		}
	}
}

func TestEqualsStr(t *testing.T) {
	om, _ := NewRemoteRefFromPathStr("origin/master")
	rom, _ := NewRemoteRefFromPathStr("refs/remotes/origin/master")
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
			NewBranchRef("refs/heads/master"),
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
			om,
			"refs/remotes/origin/master",
			true,
		},
		{
			rom,
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
		{
			NewInternalRef("create"),
			"refs/internal/create",
			true,
		},
		{
			NewInternalRef("refs/internal/create"),
			"refs/internal/create",
			true,
		},
		{
			NewWorkspaceRef("newworkspace"),
			"refs/workspaces/newworkspace",
			true,
		},
		{
			NewWorkspaceRef("refs/workspaces/newworkspace"),
			"refs/workspaces/newworkspace",
			true,
		},
		{
			NewWorkspaceRef("newworkspace"),
			"refs/workspaces/notnewworkspace",
			false,
		},
		{
			NewWorkspaceRef("newworkspace"),
			"refs/remotes/origin/newworkspace",
			false,
		},
	}

	for _, test := range tests {
		actual := EqualsStr(test.dr, test.cmp)

		if actual != test.expected {
			t.Error("for input:", test.cmp, "error comparing", test.dr, "to", test.cmp)
		}
	}
}
