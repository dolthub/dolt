package remotestorage

import (
	"sync"
	"sync/atomic"
)

func concurrentExec(work []func() error, concurrency int) error {
	if len(work) < concurrency {
		concurrency = len(work)
	}

	var wg sync.WaitGroup
	var atomicErr atomic.Value

	workChan := make(chan func() error, concurrency)
	stopChan := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-stopChan:
					return
				default:
				}

				select {
				case w, ok := <-workChan:
					if !ok {
						return
					}

					err := w()

					if err != nil {
						close(stopChan)
						atomicErr.Store(err)
						return
					}

				case <-stopChan:
					return
				}
			}
		}()
	}

	for _, w := range work {
		workChan <- w
	}

	close(workChan)
	wg.Wait()

	errInterface := atomicErr.Load()

	if errInterface != nil {
		return errInterface.(error)
	}

	return nil
}

func batchItr(elemCount, batchSize int, cb func(start, end int) (stop bool)) {
	for st, end := 0, batchSize; st < elemCount; st, end = end, end+batchSize {
		if end > elemCount {
			end = elemCount
		}

		stop := cb(st, end)

		if stop {
			break
		}
	}
}
