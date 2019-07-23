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
	"reflect"
	"testing"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func TestCommitMetaToAndFromNomsStruct(t *testing.T) {
	cm, _ := NewCommitMeta("Bill Billerson", "bigbillieb@fake.horse", "This is a test commit")
	cmSt := cm.toNomsStruct(types.Format_7_18)
	result, err := commitMetaFromNomsSt(cmSt)

	if err != nil {
		t.Fatal("Failed to convert from types.Struct to CommitMeta")
	} else if !reflect.DeepEqual(cm, result) {
		t.Error("CommitMeta was not converted without error.")
	}

	t.Log(cm.String())
}
