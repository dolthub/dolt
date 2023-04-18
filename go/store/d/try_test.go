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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package d

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	te  = testError{"te"}
	te2 = testError2{"te2"}
)

type testError struct {
	s string
}

func (e testError) Error() string { return e.s }

type testError2 struct {
	s string
}

func (e testError2) Error() string { return e.s }

func TestPanicIfTrue(t *testing.T) {
	assert := assert.New(t)

	assert.Panics(func() {
		PanicIfTrue(true)
	})

	assert.Panics(func() {
		PanicIfTrue(true)
	})

	assert.NotPanics(func() {
		PanicIfTrue(false)
	})
}

func TestPanicIfFalse(t *testing.T) {
	assert := assert.New(t)

	assert.Panics(func() {
		PanicIfFalse(false)
	})

	assert.Panics(func() {
		PanicIfFalse(false)
	})

	assert.NotPanics(func() {
		PanicIfFalse(true)
	})
}
