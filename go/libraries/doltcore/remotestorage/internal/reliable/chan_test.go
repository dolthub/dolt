// Copyright 2024 Dolthub, Inc.
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

package reliable

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChan(t *testing.T) {
	t.Run("ImmediateSrcClose", func(t *testing.T) {
		src := make(chan int)
		c := NewChan[int](src)
		close(src)
		_, ok := <-c.Recv()
		assert.False(t, ok)
		c.Close()
	})
	t.Run("DeliverAckAndClose", func(t *testing.T) {
		src := make(chan int)
		c := NewChan[int](src)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 8; i++ {
				src <- i
			}
			close(src)
		}()
		for i := 0; i < 8; i++ {
			r := <-c.Recv()
			assert.Equal(t, i, r)
			c.Ack()
		}
		_, ok := <-c.Recv()
		assert.False(t, ok)
		wg.Wait()
		c.Close()
	})
	t.Run("DeliverResetAckAndClose", func(t *testing.T) {
		src := make(chan int)
		c := NewChan[int](src)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 8; i++ {
				src <- i
			}
			close(src)
		}()
		for i := 0; i < 8; i++ {
			r := <-c.Recv()
			assert.Equal(t, i, r)
		}
		wg.Wait()

		_, ok := <-c.Recv()
		assert.False(t, ok)

		c.Reset()
		for i := 0; i < 8; i++ {
			r := <-c.Recv()
			assert.Equal(t, i, r)
		}
		_, ok = <-c.Recv()
		assert.False(t, ok)
		for i := 0; i < 4; i++ {
			c.Ack()
		}

		c.Reset()

		for i := 4; i < 8; i++ {
			r := <-c.Recv()
			assert.Equal(t, i, r)
		}
		_, ok = <-c.Recv()
		assert.False(t, ok)
		for i := 4; i < 8; i++ {
			c.Ack()
		}

		_, ok = <-c.Recv()
		assert.False(t, ok)
		c.Close()
	})
}
