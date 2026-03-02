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

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefTargetHash(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	tup, err := NewTuple(vs.Format(), Float(42), String("hello"))
	require.NoError(t, err)
	r, err := NewRef(tup, vs.Format())
	require.NoError(t, err)
	h, err := tup.Hash(vs.Format())
	require.NoError(t, err)
	assert.Equal(h, r.TargetHash())
}
