// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListIterator(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	numbers := append(generateNumbersAsValues(10), Number(20), Number(25))
	l := NewList(vrw, numbers...)
	i := l.Iterator()
	vs := iterToSlice(i)
	assert.True(vs.Equals(numbers), "Expected: %v != actual: %v", numbers, vs)

	i = l.IteratorAt(3)
	vs = iterToSlice(i)
	assert.True(vs.Equals(numbers[3:]), "Expected: %v != actual: %v", numbers, vs)

	i = l.IteratorAt(l.Len())
	assert.Nil(i.Next())
}
