// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package orderedparallel

import (
	"testing"
	"time"

	"github.com/attic-labs/testify/assert"
)

func TestOrderedParallel(t *testing.T) {
	itemCount := 1000

	input := make(chan interface{}, 24)
	output := New(input, func(item interface{}) interface{} {
		i := item.(int)
		// Earlier items wait for longer
		time.Sleep(time.Microsecond * time.Duration(itemCount-i))
		return i
	}, 24)

	go func() {
		for i := 0; i < itemCount; i++ {
			input <- i
		}

		close(input)
	}()

	expect := 0
	for out := range output {
		i := out.(int)
		assert.Equal(t, expect, i)
		expect++
	}
}
