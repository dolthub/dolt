// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package benchmark

import (
	"testing"
	"time"
)

func TestLatencyMap(t *testing.T) {
	m := NewLatencyMap()
	m.Add("foo", 100*time.Millisecond)
	m.Add("foo", 200*time.Millisecond)
	c, d := m.Get("foo")
	if c != 2 || d != 300*time.Millisecond {
		t.Errorf("got %v, %d, want 2, 150ms", c, d)
	}
}
