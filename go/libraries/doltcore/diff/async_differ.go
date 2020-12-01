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

package diff

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/async"
	"github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/types"
)

// Differ produces row diffs of two maps
type Differ interface {
	// Start initializes the iterator to iterate over 2 maps
	Start(ctx context.Context, from, to types.Map)
	// Close cleans up any resources
	Close() error
	// are not available it will return what is available.  A timeout of 0 returns what is immediately available without waiting.
	// a timeout of -1 will wait indefinitely until the number of diffs are available, or it can return all remaining diffs
	GetDiffs(numDiffs int, timeout time.Duration) ([]*diff.Difference, bool, error)
}

func GetDiffer(bufSz int, schs ...schema.Schema) (Differ, error) {
	if len(schs) == 0 {
		return nil, fmt.Errorf("must give schemas to get differ")
	}
	keyless := schema.IsKeyless(schs[0])
	for _, sch := range schs {
		if schema.IsKeyless(sch) != keyless {
			return nil, fmt.Errorf("all schemas must be keyed or keyless")
		}
	}

	ad := newAsyncDiffer(bufSz)
	if keyless {
		return keylessDiffer{ad}, nil
	}
	return ad, nil
}

type asyncDiffer struct {
	diffChan   chan diff.Difference
	bufferSize int

	eg       *errgroup.Group
	egCtx    context.Context
	egCancel func()
}

func newAsyncDiffer(bufferedDiffs int) *asyncDiffer {
	return &asyncDiffer{
		make(chan diff.Difference, bufferedDiffs),
		bufferedDiffs,
		nil,
		context.Background(),
		func() {},
	}
}

func tableDontDescendLists(v1, v2 types.Value) bool {
	kind := v1.Kind()
	return !types.IsPrimitiveKind(kind) && kind != types.TupleKind && kind == v2.Kind() && kind != types.RefKind
}

func (ad *asyncDiffer) Start(ctx context.Context, from, to types.Map) {
	ad.eg, ad.egCtx = errgroup.WithContext(ctx)
	ad.egCancel = async.GoWithCancel(ad.egCtx, ad.eg, func(ctx context.Context) error {
		defer close(ad.diffChan)
		return diff.Diff(ctx, from, to, ad.diffChan, true, tableDontDescendLists)
	})
}

func (ad *asyncDiffer) Close() error {
	ad.egCancel()
	return ad.eg.Wait()
}

func (ad *asyncDiffer) GetDiffs(numDiffs int, timeout time.Duration) ([]*diff.Difference, bool, error) {
	diffs := make([]*diff.Difference, 0, ad.bufferSize)
	timeoutChan := time.After(timeout)
	for {
		select {
		case d, more := <-ad.diffChan:
			if more {
				diffs = append(diffs, &d)
				if numDiffs != 0 && numDiffs == len(diffs) {
					return diffs, true, nil
				}
			} else {
				return diffs, false, ad.eg.Wait()
			}
		case <-timeoutChan:
			return diffs, true, nil
		case <-ad.egCtx.Done():
			return nil, false, ad.eg.Wait()
		}
	}
}

func (ad *asyncDiffer) ReadAll() ([]*diff.Difference, error) {
	diffs, hasMore, err := ad.GetDiffs(0, 5*time.Minute)
	if err != nil {
		return nil, err
	}
	if hasMore {
		return nil, errors.New("Unable to read the diffs in a reasonable amount of time")
	}
	return diffs, nil
}
