// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"bytes"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestPrefixWriterEmpty(t *testing.T) {
	assert := assert.New(t)
	b := &bytes.Buffer{}
	newPrefixWriter(b, ADD)
	assert.Equal("", b.String())
}

func TestPrefixWriterNoLineBreak(t *testing.T) {
	assert := assert.New(t)

	test := func(op prefixOp, prefix string) {
		b := &bytes.Buffer{}
		w := newPrefixWriter(b, op)
		s := "hello world"

		n, err := w.Write([]byte(s))
		assert.Equal(len(s), n)
		assert.NoError(err)

		n, err = w.Write([]byte(s))
		assert.Equal(len(s), n)
		assert.NoError(err)

		assert.Equal(prefix+s+s, b.String())
	}

	test(ADD, "+   ")
	test(DEL, "-   ")
}

func TestPrefixWriterMultipleLines(t *testing.T) {
	assert := assert.New(t)

	test := func(op prefixOp, prefix string) {
		b := &bytes.Buffer{}
		w := newPrefixWriter(b, op)
		s := "hello\nworld\n"

		n, err := w.Write([]byte(s))
		assert.Equal(len(s), n)
		assert.NoError(err)

		n, err = w.Write([]byte(s))
		assert.Equal(len(s), n)
		assert.NoError(err)

		assert.Equal(
			prefix+"hello\n"+prefix+"world\n"+prefix+"hello\n"+prefix+"world\n"+prefix,
			b.String())
	}

	test(ADD, "+   ")
	test(DEL, "-   ")
}
