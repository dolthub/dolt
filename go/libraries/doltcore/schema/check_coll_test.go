// Copyright 2025 Dolthub, Inc.
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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopy(t *testing.T) {
	var original = &checkCollection{
		checks: []check{{"check1", "expr1", true},
			{"check2", "expr2", false}},
	}
	var copy = original.Copy()

	// Assert copy doesn't reuse the same check instances
	original.checks[0].name = "XXX"
	original.checks[0].expression = "XXX"
	original.checks[0].enforced = false
	original.checks[1].name = "XXX"
	original.checks[1].expression = "XXX"
	original.checks[1].enforced = true

	assert.Equal(t, "check1", copy.AllChecks()[0].Name())
	assert.Equal(t, "expr1", copy.AllChecks()[0].Expression())
	assert.Equal(t, true, copy.AllChecks()[0].Enforced())
	assert.Equal(t, "check2", copy.AllChecks()[1].Name())
	assert.Equal(t, "expr2", copy.AllChecks()[1].Expression())
	assert.Equal(t, false, copy.AllChecks()[1].Enforced())
}
