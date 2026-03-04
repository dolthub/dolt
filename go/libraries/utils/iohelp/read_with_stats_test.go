// Copyright 2026 Dolthub, Inc.
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

package iohelp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type countingCloser struct {
	cnt int
}

func (c *countingCloser) Read(p []byte) (int, error) {
	return len(p), nil
}

func (c *countingCloser) Close() error {
	c.cnt += 1
	return nil
}

func TestReadWithStatsCloseIsIdempotent(t *testing.T) {
	t.Run("Unstarted", func(t *testing.T) {
		closer := &countingCloser{}
		rdr := NewReaderWithStats(closer, 1024)
		rdr.Close()
		assert.Equal(t, 1, closer.cnt)
		rdr.Close()
		assert.Equal(t, 2, closer.cnt)
	})
	t.Run("Started", func(t *testing.T) {
		closer := &countingCloser{}
		rdr := NewReaderWithStats(closer, 1024)
		rdr.Start(func(ReadStats) {
		})
		rdr.Close()
		assert.Equal(t, 1, closer.cnt)
		rdr.Close()
		assert.Equal(t, 2, closer.cnt)
	})
}
