// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package functions

import "sync"

// Runs all functions in |fs| in parallel, and returns when all functions have returned.
func All(fs ...func()) {
	wg := &sync.WaitGroup{}
	wg.Add(len(fs))
	for _, f_ := range fs {
		f := f_
		go func() {
			f()
			wg.Done()
		}()
	}
	wg.Wait()
}
