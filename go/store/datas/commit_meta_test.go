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

package datas

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/types"
)

func TestCommitMetaToAndFromNomsStruct(t *testing.T) {
	cm, _ := NewCommitMeta("Bill Billerson", "bigbillieb@fake.horse", "This is a test commit")
	ts := uint64(0)
	uts := int64(0)
	cm.Timestamp = &ts
	cm.UserTimestamp = &uts
	cmSt, err := cm.toNomsStruct(types.Format_Default)
	assert.NoError(t, err)
	result, err := CommitMetaFromNomsSt(cmSt)

	if err != nil {
		t.Fatal("Failed to convert from types.Struct to CommitMeta")
	}

	normalizedCm := *cm
	if normalizedCm.CommitterName == "" {
		normalizedCm.CommitterName = normalizedCm.Name
	}
	if normalizedCm.CommitterEmail == "" {
		normalizedCm.CommitterEmail = normalizedCm.Email
	}

	if !reflect.DeepEqual(&normalizedCm, result) {
		t.Error("CommitMeta was not converted without error.")
	}

	t.Log(cm.String())
}
