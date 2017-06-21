// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestMapIterator(t *testing.T) {
	assert := assert.New(t)

	me := NewMap().Edit()
	for i := 0; i < 5; i++ {
		me.Set(String(string(byte(65+i))), Number(i))
	}

	m := me.Map(nil)
	test := func(it MapIterator, start int, msg string) {
		for i := start; i < 5; i++ {
			k, v := it.Next()
			assert.True(k.Equals(k), msg)
			assert.True(v.Equals(v), msg)
			assert.True(String(string(byte(65+i))).Equals(k), msg)
			assert.True(Number(i).Equals(v), msg)
		}
		k, v := it.Next()
		assert.Nil(k, msg)
		assert.Nil(v, msg)
	}

	test(m.Iterator(), 0, "Iterator()")
	test(m.IteratorAt(0), 0, "IteratorAt(0)")
	test(m.IteratorAt(5), 5, "IteratorAt(5)")
	test(m.IteratorAt(6), 5, "IteratorAt(6)")
	test(m.IteratorFrom(String("?")), 0, "IteratorFrom(?)")
	test(m.IteratorFrom(String("A")), 0, "IteratorFrom(A)")
	test(m.IteratorFrom(String("C")), 2, "IteratorFrom(C)")
	test(m.IteratorFrom(String("E")), 4, "IteratorFrom(E)")
	test(m.IteratorFrom(String("F")), 5, "IteratorFrom(F)")
	test(m.IteratorFrom(String("G")), 5, "IteratorFrom(G)")
}
