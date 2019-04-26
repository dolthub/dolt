package diff

import (
	"context"
	"errors"
	"github.com/attic-labs/noms/go/diff"
	"github.com/attic-labs/noms/go/types"
	"time"
)

type AsyncDiffer struct {
	stopChan   chan struct{}
	diffChan   chan diff.Difference
	bufferSize int
	isDone     bool
}

func NewAsyncDiffer(bufferedDiffs int) *AsyncDiffer {
	return &AsyncDiffer{
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

func (ad *AsyncDiffer) Start(ctx context.Context, v1, v2 types.Value) {
	go func() {
		diff.Diff(ctx, v2, v1, ad.diffChan, ad.stopChan, true, tableDontDescendLists)
		close(ad.diffChan)
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

func (ad *AsyncDiffer) GetDiffs(numDiffs int, timeout time.Duration) []*diff.Difference {
	diffs := make([]*diff.Difference, 0, ad.bufferSize)
	timeoutChan := time.After(timeout)
	if !ad.isDone {
		for {
			select {
			case d, more := <-ad.diffChan:
				if more {
					diffs = append(diffs, &d)

					if numDiffs != 0 && numDiffs == len(diffs) {
						return diffs
					}
				} else {
					ad.isDone = true
					return diffs
				}

			case <-timeoutChan:
				return diffs
			}
		}
	}

	return diffs
}

func (ad *AsyncDiffer) ReadAll() ([]*diff.Difference, error) {
	diffs := ad.GetDiffs(0, 5*time.Minute)

	if !ad.isDone {
		return nil, errors.New("Unable to read the diffs in a reasonable amount of time")
	}

	return diffs, nil
}
