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

package row

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFmt(t *testing.T) {
	r, err := newTestRow()
	require.NoError(t, err)

	expected := `first:"rick" | last:"astley" | age:53 | address:"123 Fake St" | title:null_value | `
	actual := Fmt(context.Background(), r, sch)
	if expected != actual {
		t.Fatalf("expected: '%s', actual: '%s'", expected, actual)
	}
}
