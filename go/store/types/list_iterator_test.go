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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListIterator(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	numbers := append(generateNumbersAsValues(10), Float(20), Float(25))
	l := NewList(context.Background(), vrw, numbers...)
	i := l.Iterator(context.Background())
	vs := iterToSlice(i)
	assert.True(vs.Equals(numbers), "Expected: %v != actual: %v", numbers, vs)

	i = l.IteratorAt(context.Background(), 3)
	vs = iterToSlice(i)
	assert.True(vs.Equals(numbers[3:]), "Expected: %v != actual: %v", numbers, vs)

	i = l.IteratorAt(context.Background(), l.Len())
	assert.Nil(i.Next(context.Background()))
}
