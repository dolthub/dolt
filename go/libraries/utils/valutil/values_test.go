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

package valutil

import (
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"testing"
)

func TestNilSafeEqCheck(t *testing.T) {
	tests := []struct {
		v1         types.Value
		v2         types.Value
		expectedEq bool
	}{
		{nil, nil, true},
		{nil, types.NullValue, true},
		{nil, types.String("blah"), false},
		{types.NullValue, types.String("blah"), false},
		{types.String("blah"), types.String("blah"), true},
	}

	for i, test := range tests {
		actual := NilSafeEqCheck(test.v1, test.v2)

		if actual != test.expectedEq {
			t.Error("test", i, "failed")
		}
	}
}
