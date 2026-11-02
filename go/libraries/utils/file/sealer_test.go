// Copyright 2026 Dolthub, Inc.
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

package file

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nopSealer struct{ PlainSealer }

func TestSealerOf(t *testing.T) {
	assert.Equal(t, PlainSealer{}, SealerOf(nil))
	assert.Equal(t, PlainSealer{}, SealerOf([]Sealer{nil}))
	assert.Equal(t, nopSealer{}, SealerOf([]Sealer{nopSealer{}}))

	ctx := context.Background()
	sealed, err := PlainSealer{}.Seal(ctx, []byte("x"))
	require.NoError(t, err)
	assert.Equal(t, []byte("x"), sealed)
	opened, err := PlainSealer{}.Open(ctx, "path", []byte("x"), os.ModePerm)
	require.NoError(t, err)
	assert.Equal(t, []byte("x"), opened)
}
