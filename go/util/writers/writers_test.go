// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package writers

import (
	"bytes"
	"io"
	"testing"

	"github.com/attic-labs/testify/assert"
)

type maxLineTestCase struct {
	data          string
	maxLines      uint32
	expected      string
	errorExpected bool
}

func TestMaxLineWriter(t *testing.T) {
	assert := assert.New(t)

	tcs := []maxLineTestCase{
		{"hey there\nthis text contains\n3 lines\n", 1, "hey there\n", true},
		{"hey there\nthis text contains\n3 lines\n", 2, "hey there\nthis text contains\n", true},
		{"hey there\nthis text contains\n3 lines\n", 3, "hey there\nthis text contains\n3 lines\n", false},
		{"hey there\nthis text contains\n3 lines\nand more\n", 3, "hey there\nthis text contains\n3 lines\n", true},
		{"hey there\nthis text contains\n3 lines\n", 4, "hey there\nthis text contains\n3 lines\n", false},
		{"hey there\nthis text contains\n3 lines\n", 0, "hey there\nthis text contains\n3 lines\n", false},
		{"\n\n\n\n", 2, "\n\n", true},
	}

	for i, tc := range tcs {
		buf := bytes.NewBuffer(nil)
		mlw := MaxLineWriter{Dest: buf, MaxLines: tc.maxLines}
		l, err := mlw.Write([]byte(tc.data))
		assert.Equal(len(tc.expected), l, "test #%d case failed", i)
		if tc.errorExpected {
			assert.Error(err, "test #%d case failed", i)
			assert.IsType(MaxLinesError{}, err, "test #%d case failed", i)
		} else {
			assert.NoError(err, "test #%d case failed", i)
		}
		assert.Equal(tc.expected, buf.String(), "test #%d case failed", i)
	}
}

type prefixTestCase struct {
	data        string
	prefix      string
	expected    string
	needsPrefix bool
}

func TestPrefixWriter(t *testing.T) {
	assert := assert.New(t)

	tcs := []prefixTestCase{
		{"\n", "yo:", "yo:\n", true},
		{"\n", "yo:", "\n", false},
		{"\n\n", "yo:", "yo:\nyo:\n", true},
		{"\n\n", "yo:", "\nyo:\n", false},
		{"hey there\nthis text contains\n3 lines\n", "yo:", "yo:hey there\nyo:this text contains\nyo:3 lines\n", true},
		{"hey there\nthis text contains\n3 lines\n", "yo:", "hey there\nyo:this text contains\nyo:3 lines\n", false},
		{"hey there\nthis text contains\n3 lines\n", "", "hey there\nthis text contains\n3 lines\n", true},
		{"hey there\nthis text contains\n3 lines\n", "", "hey there\nthis text contains\n3 lines\n", false},
	}

	for _, tc := range tcs {
		getPrefix := func(w *PrefixWriter) []byte {
			return []byte(tc.prefix)
		}
		buf := bytes.NewBuffer(nil)
		pw := PrefixWriter{Dest: buf, PrefixFunc: getPrefix, NeedsPrefix: tc.needsPrefix}
		l, err := pw.Write([]byte(tc.data))
		assert.NoError(err)
		assert.Equal(len(tc.expected), l)
		assert.Equal(tc.expected, buf.String())
	}
}

type prefixMaxLineTestCase struct {
	data          string
	prefix        string
	expected      string
	needsPrefix   bool
	maxLines      uint32
	errorExpected bool
}

func TestPrefixMaxLineWriter(t *testing.T) {
	assert := assert.New(t)

	tcs := []prefixMaxLineTestCase{
		{"hey there\nthis text contains\n3 lines\n", "yo:", "yo:hey there\nyo:this text contains\nyo:3 lines\n", true, 0, false},
		{"hey there\nthis text contains\n3 lines\n", "yo:", "yo:hey there\n", true, 1, true},
		{"hey there\nthis text contains\n3 lines\n", "yo:", "hey there\nyo:this text contains\nyo:3 lines\n", false, 0, false},
		{"hey there\nthis text contains\n3 lines\n", "yo:", "hey there\nyo:this text contains\n", false, 2, true},
		{"hey there\nthis text contains\n3 lines\n", "", "hey there\nthis text contains\n3 lines\n", true, 0, false},
		{"hey there\nthis text contains\n3 lines\n", "", "hey there\nthis text contains\n", false, 2, true},
	}

	doTest := func(tc prefixMaxLineTestCase, tcNum int, buf *bytes.Buffer, tw io.Writer) {
		l, err := tw.Write([]byte(tc.data))
		if tc.errorExpected {
			assert.Error(err, "test #%d case failed", tcNum)
			assert.IsType(MaxLinesError{}, err, "test #%d case failed", tcNum)
		} else {
			assert.NoError(err, "test #%d case failed", tcNum)
		}
		assert.Equal(len(tc.expected), l, "test #%d case failed", tcNum)
		assert.Equal(tc.expected, buf.String(), "test #%d case failed", tcNum)
	}

	for i, tc := range tcs {
		getPrefix := func(w *PrefixWriter) []byte {
			return []byte(tc.prefix)
		}
		buf := &bytes.Buffer{}
		mlw := &MaxLineWriter{Dest: buf, MaxLines: tc.maxLines}
		pw := &PrefixWriter{Dest: mlw, PrefixFunc: getPrefix, NeedsPrefix: tc.needsPrefix}
		doTest(tc, i, buf, pw)

		buf = &bytes.Buffer{}
		pw = &PrefixWriter{Dest: buf, PrefixFunc: getPrefix, NeedsPrefix: tc.needsPrefix}
		mlw = &MaxLineWriter{Dest: pw, MaxLines: tc.maxLines}
		doTest(tc, i, buf, mlw)
	}
}
