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

package dynassert

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAssertsAreEnabled(t *testing.T) {
	assert.True(t, enabled)
	Assert(true, "does not panic")
	assert.Panics(t, func() {
		Assert(false, "does panic")
	})
}

func TestInitDynamicAsserts(t *testing.T) {
	t.Run("WithoutEnvVarSet", func(t *testing.T) {
		enabled = true
		t.Setenv("DOLT_ENABLE_DYNAMIC_ASSERTS", "")
		InitDyanmicAsserts()
		assert.False(t, enabled)
		Assert(true, "does not panic")
		Assert(false, "does not panic")
	})
	t.Run("WithEnvVarSet", func(t *testing.T) {
		enabled = true
		t.Setenv("DOLT_ENABLE_DYNAMIC_ASSERTS", "true")
		InitDyanmicAsserts()
		assert.True(t, enabled)
	})
}
