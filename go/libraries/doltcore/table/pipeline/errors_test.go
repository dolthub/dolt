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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/store/types"
)

func TestTransformRowFailure(t *testing.T) {
	_, sch := untyped.NewUntypedSchema("a", "b", "c")
	r, err := untyped.NewRowFromStrings(types.Format_Default, sch, []string{"1", "2", "3"})

	assert.NoError(t, err)

	err = &TransformRowFailure{r, nil, "transform_name", "details"}

	if !IsTransformFailure(err) {
		t.Error("should be transform failure")
	}

	tn := GetTransFailureTransName(err)
	if tn != "transform_name" {
		t.Error("Unexpected transform name:" + tn)
	}

	fr := GetTransFailureRow(err)

	if !row.AreEqual(r, fr, sch) {
		t.Error("unexpected row")
	}

	dets := GetTransFailureDetails(err)

	if dets != "details" {
		t.Error("unexpected details:" + dets)
	}
}
