// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestConstructQueryString(t *testing.T) {
	assert := assert.New(t)
	prefix := "TestConstructQueryString"

	d1, e1 := ioutil.TempDir(os.TempDir(), prefix)
	defer os.RemoveAll(d1)
	d2, e2 := ioutil.TempDir(os.TempDir(), prefix)
	defer os.RemoveAll(d2)

	assert.NoError(e1)
	assert.NoError(e2)

	qs, stores := constructQueryString([]string{
		"foo=bar",
		"store1=ldb:" + d1,
		"store2=ldb:" + d2,
		"store3=ldb:" + d1,
		"hello=world",
	})

	assert.Equal(5, len(qs))
	assert.Equal("bar", qs.Get("foo"))
	assert.True(strings.HasPrefix(qs.Get("store1"), dsPathPrefix))
	assert.True(strings.HasPrefix(qs.Get("store2"), dsPathPrefix))
	assert.True(strings.HasPrefix(qs.Get("store3"), dsPathPrefix))
	assert.Equal(qs.Get("store1"), qs.Get("store3"))
	assert.NotEqual(qs.Get("store1"), qs.Get("store2"))
	assert.Equal(2, len(stores))
}
