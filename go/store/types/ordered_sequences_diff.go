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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"

	"github.com/dolthub/dolt/go/store/d"
)

type DiffChangeType uint8

const (
	DiffChangeAdded DiffChangeType = iota
	DiffChangeRemoved
	DiffChangeModified
)

type ValueChanged struct {
	ChangeType              DiffChangeType
	Key, OldValue, NewValue Value
}

func sendChange(ctx context.Context, changes chan<- ValueChanged, change ValueChanged) error {
	select {
	case changes <- change:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Streams the diff from |last| to |current| into |changes|, using a left-right approach.
// Left-right immediately descends to the first change and starts streaming changes, but compared to top-down it's serial and much slower to calculate the full diff.
func orderedSequenceDiffLeftRight(ctx context.Context, last orderedSequence, current orderedSequence, changes chan<- ValueChanged) error {
	trueFunc := func(context.Context, Value) (bool, bool, error) {
		return true, false, nil
	}
	return orderedSequenceDiffLeftRightInRange(ctx, last, current, emptyKey, trueFunc, changes)
}

func orderedSequenceDiffLeftRightInRange(ctx context.Context, last orderedSequence, current orderedSequence, startKey orderedKey, inRange ValueInRange, changes chan<- ValueChanged) error {
	lastCur, err := newCursorAt(ctx, last, startKey, false, false)
	if err != nil {
		return err
	}

	currentCur, err := newCursorAt(ctx, current, startKey, false, false)
	if err != nil {
		return err
	}

VALIDRANGES:
	for lastCur.valid() && currentCur.valid() {
		err := fastForward(ctx, lastCur, currentCur)
		if err != nil {
			return err
		}

		for lastCur.valid() && currentCur.valid() {
			equals, err := lastCur.seq.getCompareFn(currentCur.seq)(lastCur.idx, currentCur.idx)
			if err != nil {
				return err
			}

			if equals {
				break
			}

			lastKey, err := getCurrentKey(lastCur)
			if err != nil {
				return err
			}

			currentKey, err := getCurrentKey(currentCur)
			if err != nil {
				return err
			}

			if isLess, err := currentKey.Less(last.format(), lastKey); err != nil {
				return err
			} else if isLess {
				valid, skip, err := inRange(ctx, currentKey.v)
				if err != nil {
					return err
				}

				// Out of range
				if !valid {
					break VALIDRANGES
				}

				// Not skipping this value (want to skip for non-inclusive lower bound range)
				if !skip {
					mv, err := getMapValue(currentCur)
					if err != nil {
						return err
					}

					if err := sendChange(ctx, changes, ValueChanged{DiffChangeAdded, currentKey.v, nil, mv}); err != nil {
						return err
					}
				}

				_, err = currentCur.advance(ctx)
				if err != nil {
					return err
				}
			} else {
				valid, skip, err := inRange(ctx, lastKey.v)
				if err != nil {
					return err
				}

				// Out of range
				if !valid {
					break VALIDRANGES
				}

				// Skip this last key
				if skip {
					_, err = lastCur.advance(ctx)
					if err != nil {
						return err
					}
					continue
				}

				if isLess, err := lastKey.Less(last.format(), currentKey); err != nil {
					return err
				} else if isLess {
					mv, err := getMapValue(lastCur)
					if err != nil {
						return err
					}

					if err := sendChange(ctx, changes, ValueChanged{DiffChangeRemoved, lastKey.v, mv, nil}); err != nil {
						return err
					}

					_, err = lastCur.advance(ctx)
					if err != nil {
						return err
					}
				} else {
					lmv, err := getMapValue(lastCur)
					if err != nil {
						return err
					}

					cmv, err := getMapValue(currentCur)
					if err != nil {
						return err
					}

					if err := sendChange(ctx, changes, ValueChanged{DiffChangeModified, lastKey.v, lmv, cmv}); err != nil {
						return err
					}

					_, err = lastCur.advance(ctx)
					if err != nil {
						return err
					}

					_, err = currentCur.advance(ctx)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	for lastCur.valid() {
		lastKey, err := getCurrentKey(lastCur)
		if err != nil {
			return err
		}

		valid, skip, err := inRange(ctx, lastKey.v)
		if err != nil {
			return err
		}

		if !valid {
			break
		}

		if !skip {
			mv, err := getMapValue(lastCur)
			if err != nil {
				return err
			}

			if err := sendChange(ctx, changes, ValueChanged{DiffChangeRemoved, lastKey.v, mv, nil}); err != nil {
				return err
			}
		}

		_, err = lastCur.advance(ctx)
		if err != nil {
			return err
		}
	}

	for currentCur.valid() {
		currKey, err := getCurrentKey(currentCur)
		if err != nil {
			return err
		}

		valid, skip, err := inRange(ctx, currKey.v)
		if err != nil {
			return err
		}

		if !valid {
			break
		}

		if !skip {
			mv, err := getMapValue(currentCur)
			if err != nil {
				return err
			}

			if err := sendChange(ctx, changes, ValueChanged{DiffChangeAdded, currKey.v, nil, mv}); err != nil {
				return err
			}
		}

		_, err = currentCur.advance(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

/**
 * Advances |a| and |b| past their common sequence of equal values.
 */
func fastForward(ctx context.Context, a *sequenceCursor, b *sequenceCursor) error {
	if a.valid() && b.valid() {
		_, _, err := doFastForward(ctx, true, a, b)

		if err != nil {
			return err
		}
	}

	return nil
}

func syncWithIdx(ctx context.Context, cur *sequenceCursor, hasMore bool, allowPastEnd bool) error {
	err := cur.sync(ctx)

	if err != nil {
		return err
	}

	if hasMore {
		cur.idx = 0
	} else if allowPastEnd {
		cur.idx = cur.length()
	} else {
		cur.idx = cur.length() - 1
	}

	return nil
}

/*
 * Returns an array matching |a| and |b| respectively to whether that cursor has more values.
 */
func doFastForward(ctx context.Context, allowPastEnd bool, a *sequenceCursor, b *sequenceCursor) (aHasMore bool, bHasMore bool, err error) {
	d.PanicIfFalse(a.valid())
	d.PanicIfFalse(b.valid())
	aHasMore = true
	bHasMore = true

	for aHasMore && bHasMore {
		equals, err := isCurrentEqual(a, b)

		if err != nil {
			return false, false, err
		}

		if !equals {
			break
		}

		parentsEqAndNotNil := nil != a.parent && nil != b.parent

		if parentsEqAndNotNil {
			parentsEqAndNotNil, err = isCurrentEqual(a.parent, b.parent)

			if err != nil {
				return false, false, err
			}
		}

		if parentsEqAndNotNil {
			// Key optimisation: if the sequences have common parents, then entire chunks can be
			// fast-forwarded without reading unnecessary data.
			aHasMore, bHasMore, err = doFastForward(ctx, false, a.parent, b.parent)

			if err != nil {
				return false, false, err
			}

			err := syncWithIdx(ctx, a, aHasMore, allowPastEnd)

			if err != nil {
				return false, false, err
			}

			err = syncWithIdx(ctx, b, bHasMore, allowPastEnd)

			if err != nil {
				return false, false, err
			}
		} else {
			aHasMore, err = a.advanceMaybeAllowPastEnd(ctx, allowPastEnd)

			if err != nil {
				return false, false, err
			}

			bHasMore, err = b.advanceMaybeAllowPastEnd(ctx, allowPastEnd)

			if err != nil {
				return false, false, err
			}
		}
	}
	return aHasMore, bHasMore, nil
}

func isCurrentEqual(a *sequenceCursor, b *sequenceCursor) (bool, error) {
	return a.seq.getCompareFn(b.seq)(a.idx, b.idx)
}
