// Copyright 2021 Dolthub, Inc.
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

package async

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSingleThread(t *testing.T) {
	tests := []struct {
		allocSize int
		numItems  int
	}{
		{128, 127},
		{128, 128},
		{128, 129},
		{1, 1024},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("alloc %d items %d", test.allocSize, test.numItems), func(t *testing.T) {
			rb := NewRingBuffer(test.allocSize)

			for i := 0; i < test.numItems; i++ {
				err := rb.Push(i, rb.epoch)
				assert.NoError(t, err)
			}

			for i := 0; i < test.numItems; i++ {
				item, err := rb.Pop()
				assert.NoError(t, err)
				assert.Equal(t, i, item.(int))
			}

			item, ok := rb.TryPop()
			assert.Nil(t, item)
			assert.False(t, ok)
		})
	}
}

func TestOneProducerOneConsumer(t *testing.T) {
	tests := []struct {
		allocSize int
		numItems  int
	}{
		{128, 127},
		{128, 128},
		{128, 129},
		{1, 1024},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("alloc %d items %d", test.allocSize, test.numItems), func(t *testing.T) {
			rb := NewRingBuffer(test.allocSize)

			go func() {
				defer rb.Close()

				for i := 0; i < test.numItems; i++ {
					err := rb.Push(i, rb.epoch)
					assert.NoError(t, err)
				}
			}()

			for i := 0; i < test.numItems; i++ {
				item, err := rb.Pop()
				assert.NoError(t, err)
				assert.Equal(t, i, item.(int))
			}

			item, err := rb.Pop()
			assert.Nil(t, item)
			assert.Equal(t, io.EOF, err)
		})
	}
}

func TestNProducersNConsumers(t *testing.T) {
	tests := []struct {
		producers        int
		consumers        int
		allocSize        int
		itemsPerProducer int
	}{
		{2, 8, 128, 127},
		{2, 8, 128, 128},
		{2, 8, 128, 129},
		{2, 8, 1, 1024},
		{8, 2, 1, 1024},
		{8, 8, 1, 1024},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("producers %d consumers %d alloc %d items per producer %d", test.producers, test.consumers, test.allocSize, test.itemsPerProducer), func(t *testing.T) {
			rb := NewRingBuffer(test.allocSize)

			producerGroup := &sync.WaitGroup{}
			producerGroup.Add(test.producers)
			for i := 0; i < test.producers; i++ {
				go func() {
					defer producerGroup.Done()
					for i := 0; i < test.itemsPerProducer; i++ {
						err := rb.Push(i, rb.epoch)
						assert.NoError(t, err)
					}
				}()
			}

			consumerResults := make([][]int, test.consumers)
			consumerGroup := &sync.WaitGroup{}
			consumerGroup.Add(test.consumers)
			for i := 0; i < test.consumers; i++ {
				results := make([]int, test.itemsPerProducer)
				consumerResults[i] = results
				go func() {
					defer consumerGroup.Done()
					for {
						item, err := rb.Pop()

						if err != nil {
							assert.Equal(t, io.EOF, err)
							return
						}

						results[item.(int)]++
					}
				}()
			}

			producerGroup.Wait()
			err := rb.Close()
			assert.NoError(t, err)
			consumerGroup.Wait()

			for i := 0; i < test.itemsPerProducer; i++ {
				sum := 0
				for j := 0; j < test.consumers; j++ {
					sum += consumerResults[j][i]
				}

				assert.Equal(t, test.producers, sum)
			}
		})
	}
}

func TestRingBufferEpoch(t *testing.T) {
	rb := NewRingBuffer(1024)
	epoch := rb.Reset()
	err := rb.Push(1, epoch)
	assert.NoError(t, err)
	err = rb.Push(2, epoch+1)
	assert.Error(t, err)
	assert.Equal(t, ErrWrongEpoch, err)
	v, ok := rb.TryPop()
	assert.True(t, ok)
	assert.Equal(t, 1, v)
	_, ok = rb.TryPop()
	assert.False(t, ok)
	newEpoch := rb.Reset()
	assert.NotEqual(t, epoch, newEpoch)
}
