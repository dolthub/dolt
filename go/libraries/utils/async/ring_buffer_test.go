package async

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"sync"
	"testing"
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
				err := rb.Push(i)
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
					err := rb.Push(i)
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
						err := rb.Push(i)
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
