package remotestorage

import (
	"sync"
	"sync/atomic"
)

func concurrentExec(work []func() error, concurrency int) error {
	if concurrency <= 0 {
		panic("Invalid argument")
	} else if len(work) < concurrency {
		concurrency = len(work)
	}

	// Buffer size needs to be able to take in all the work, otherwise it can deadlock if an error causes the workers to
	// close the stop channel
	workChan := make(chan func() error, len(work))

	var wg sync.WaitGroup
	var firstErr atomic.Value
	var closeOnce sync.Once

	// start worker go routines based on the supplied concurrency
	stopChan := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		wg.Add(1)

		//worker go routine
		go func() {
			defer wg.Done()

			for {
				// verify we haven't received a stop message, fall through immediately if not
				select {
				case <-stopChan:
					return
				default:
				}

				// wait for a work or stop message
				select {
				case w, ok := <-workChan:
					if !ok {
						// workChan closed.  Time to exit
						return
					}

					// do the work and capture any errors
					err := w()

					if err != nil {
						// If one or more errors occur, the first error will close the stopChan and be saved as the
						// error that gets returned.
						closeOnce.Do(func() {
							close(stopChan)
							firstErr.Store(err)
						})

						return
					}

				// stop message received while waiting for work
				case <-stopChan:
					return
				}
			}
		}()
	}

	// write the work routines to the work channel
	for _, w := range work {
		workChan <- w
	}

	close(workChan)
	wg.Wait()

	firstErrVal := firstErr.Load()

	if firstErrVal != nil {
		return firstErrVal.(error)
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
