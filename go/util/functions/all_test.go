// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package functions

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestAll(t *testing.T) {
	assert := assert.New(t)

	// Set |res| via |ch| to test it's running in parallel - if not, they'll deadlock.
	var res int
	ch := make(chan int)
	All(func() { ch <- 42 }, func() { res = <-ch })

	assert.Equal(42, res)
}
