// Copyright 2019 Liquidata, Inc.
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

package diff

import (
	"context"
	"errors"
	"time"

	"github.com/liquidata-inc/dolt/go/store/atomicerr"

	"github.com/liquidata-inc/dolt/go/store/diff"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type AsyncDiffer struct {
	ae         *atomicerr.AtomicError
	stopChan   chan struct{}
	diffChan   chan diff.Difference
	bufferSize int
	isDone     bool
}

func NewAsyncDiffer(bufferedDiffs int) *AsyncDiffer {
	return &AsyncDiffer{
		atomicerr.New(),
		make(chan struct{}),
		make(chan diff.Difference, bufferedDiffs),
		bufferedDiffs,
		false,
	}
}

func tableDontDescendLists(v1, v2 types.Value) bool {
	kind := v1.Kind()
	return !types.IsPrimitiveKind(kind) && kind != types.TupleKind && kind == v2.Kind() && kind != types.RefKind
}

func (ad *AsyncDiffer) Start(ctx context.Context, from, to types.Map) {
	go func() {
		defer close(ad.diffChan)
		defer func() {
			// Ignore a panic from Diff...
			recover()
		}()
		diff.Diff(ctx, ad.ae, from, to, ad.diffChan, ad.stopChan, true, tableDontDescendLists)
	}()
}

func (ad *AsyncDiffer) IsDone() bool {
	return ad.isDone
}

func (ad *AsyncDiffer) Close() {
	defer func() {
		// ignore close failures
		recover()
	}()

	ad.isDone = true
	close(ad.stopChan)
}

func (ad *AsyncDiffer) GetDiffs(numDiffs int, timeout time.Duration) ([]*diff.Difference, error) {
	if err := ad.ae.Get(); err != nil {
		return nil, err
	}

	diffs := make([]*diff.Difference, 0, ad.bufferSize)
	timeoutChan := time.After(timeout)
	if !ad.isDone {
		for {
			select {
			case d, more := <-ad.diffChan:
				if more {
					diffs = append(diffs, &d)

					if numDiffs != 0 && numDiffs == len(diffs) {
						return diffs, nil
					}
				} else {
					ad.isDone = true
					return diffs, nil
				}

			case <-timeoutChan:
				return diffs, nil
			}
		}
	}

	return diffs, nil
}

func (ad *AsyncDiffer) ReadAll() ([]*diff.Difference, error) {
	diffs, err := ad.GetDiffs(0, 5*time.Minute)

	if err != nil {
		return nil, err
	}

	if !ad.isDone {
		return nil, errors.New("Unable to read the diffs in a reasonable amount of time")
	}

	return diffs, nil
}
