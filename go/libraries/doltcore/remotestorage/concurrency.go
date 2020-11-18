// Copyright 2019 Dolthub, Inc.
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

package remotestorage

import (
	"context"

	"golang.org/x/sync/errgroup"
)

func concurrentExec(work []func() error, concurrency int) error {
	if concurrency <= 0 {
		panic("Invalid argument")
	} else if len(work) < concurrency {
		concurrency = len(work)
	}

	ch := make(chan func() error)

	eg, ctx := errgroup.WithContext(context.Background())

	// Push the work...
	eg.Go(func() error {
		defer close(ch)
		for _, w := range work {
			select {
			case ch <- w:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	// Do the work...
	for i := 0; i < concurrency; i++ {
		eg.Go(func() error {
			for {
				select {
				case w, ok := <-ch:
					if !ok {
						return nil
					}
					if err := w(); err != nil {
						return err
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	return eg.Wait()
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
