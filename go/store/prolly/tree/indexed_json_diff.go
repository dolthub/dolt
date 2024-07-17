// Copyright 2024 Dolthub, Inc.
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

package tree

import (
	"context"
	"github.com/dolthub/go-mysql-server/sql/types"
	"golang.org/x/exp/slices"
)

type IndexedJsonDiffer struct {
	differ                             Differ[jsonLocationKey, jsonLocationOrdering]
	currentFromCursor, currentToCursor *JsonCursor
	from, to                           IndexedJsonDocument
}

func NewIndexedJsonDiffer(ctx context.Context, from, to IndexedJsonDocument) (*IndexedJsonDiffer, error) {
	differ, err := DifferFromRoots[jsonLocationKey, jsonLocationOrdering](ctx, from.m.NodeStore, to.m.NodeStore, from.m.Root, to.m.Root, jsonLocationOrdering{}, false)
	if err != nil {
		return nil, err
	}
	return &IndexedJsonDiffer{
		differ: differ,
		from:   from,
		to:     to,
	}, nil
}

// Next computes the next diff between the two JSON documents.
func (jd *IndexedJsonDiffer) Next(ctx context.Context) (diff JsonDiff, err error) {
	for {
		if jd.currentFromCursor == nil && jd.currentToCursor == nil {
			// Either this is the first iteration, or the last iteration exhausted both chunks at the same time.
			// (ie, both chunks ended at the same JSON path). We can use `Differ.Next` to seek to the next difference.
			chunkDiff, err := jd.differ.Next(ctx)
			if err != nil {
				return JsonDiff{}, err
			}
			jd.currentFromCursor, err = newJsonCursorAtStartOfChunk(ctx, jd.from.m.NodeStore, jd.from.m.Root, []byte(chunkDiff.Key))
			if err != nil {
				return JsonDiff{}, err
			}
			jd.currentToCursor, err = newJsonCursorAtStartOfChunk(ctx, jd.to.m.NodeStore, jd.to.m.Root, []byte(chunkDiff.Key))
			if err != nil {
				return JsonDiff{}, err
			}
			if err != nil {
				return JsonDiff{}, err
			}
		} else if jd.currentFromCursor == nil {
			// We exhausted the current `from` chunk but not the `to` chunk. Since the chunk boundaries don't align on
			// the same key, we need to continue into the next chunk.
			err := jd.differ.from.advance(ctx)
			if err != nil {
				return JsonDiff{}, err
			}
			jd.currentFromCursor, err = newJsonCursorFromCursor(ctx, jd.differ.from)
			if err != nil {
				return JsonDiff{}, err
			}
		} else if jd.currentToCursor == nil {
			// We exhausted the current `to` chunk but not the `from` chunk. Since the chunk boundaries don't align on
			// the same key, we need to continue into the next chunk.
			err := jd.differ.to.advance(ctx)
			if err != nil {
				return JsonDiff{}, err
			}
			jd.currentToCursor, err = newJsonCursorFromCursor(ctx, jd.differ.to)
			if err != nil {
				return JsonDiff{}, err
			}
		}
		// Both cursors point to chunks that are different between the two documents.
		// We must be in one of the following states:
		// 1) Both cursors have the JSON path with the same values:
		//    - This location has not changed, advance both cursors and continue.
		// 2) Both cursors have the same JSON path but different values:
		//    - The value at that path has been modified.
		// 3) Both cursors point to the start of a value, but the paths differ:
		//    - A value has been inserted or deleted in the beginning/middle of an object.
		// 4) One cursor points to the start of a value, while the other cursor points to the end of that value's parent:
		// 	  - A value has been inserted or deleted at the end of an object or array.
		// 5) One cursor points to the initial element of an object/array, while the other points to the end of that same path:
		//    - A value has been changed from an object/array to a scalar, or vice-versa.
		// 6) One cursor points to the initial element of an object, while the other points to the initial element of an array:
		//    - The value has been change from an object to an array, or vice-versa.

		fromScanner := jd.currentFromCursor.jsonScanner
		toScanner := jd.currentToCursor.jsonScanner
		fromScannerAtStartOfValue := fromScanner.atStartOfValue()
		toScannerAtStartOfValue := toScanner.atStartOfValue()
		fromCurrentLocation := fromScanner.currentPath
		toCurrentLocation := toScanner.currentPath

		// helper function to advance a JsonCursor and set it to nil if it reaches the end of a chunk
		advanceCursor := func(jCur **JsonCursor) (err error) {
			if (*jCur).jsonScanner.atEndOfChunk() {
				*jCur = nil
			} else {
				err = (*jCur).jsonScanner.AdvanceToNextLocation()
				if err != nil {
					return err
				}
			}
			return nil
		}

		if !fromScannerAtStartOfValue && !toScannerAtStartOfValue {
			// Neither cursor points to the start of a value.
			// This should only be possible if they're at the same location.
			// Do a sanity check, then continue.
			if compareJsonLocations(fromCurrentLocation, toCurrentLocation) != 0 {
				return JsonDiff{}, jsonParseError
			}
			err = advanceCursor(&jd.currentFromCursor)
			if err != nil {
				return JsonDiff{}, err
			}
			err = advanceCursor(&jd.currentToCursor)
			if err != nil {
				return JsonDiff{}, err
			}
			continue
		}

		if fromScannerAtStartOfValue && toScannerAtStartOfValue {
			cmp := compareJsonLocations(fromCurrentLocation, toCurrentLocation)
			switch cmp {
			case 0:
				key := fromCurrentLocation.Clone().key

				// Both sides have the same key. If they're both an object or both an array, continue.
				// Otherwise, compare them and possibly return a modification.
				if (fromScanner.current() == '{' && toScanner.current() == '{') ||
					(fromScanner.current() == '[' && toScanner.current() == '[') {
					err = advanceCursor(&jd.currentFromCursor)
					if err != nil {
						return JsonDiff{}, err
					}
					err = advanceCursor(&jd.currentToCursor)
					if err != nil {
						return JsonDiff{}, err
					}
					continue
				}

				fromValue, err := jd.currentFromCursor.NextValue(ctx)
				if err != nil {
					return JsonDiff{}, err
				}
				toValue, err := jd.currentToCursor.NextValue(ctx)
				if err != nil {
					return JsonDiff{}, err
				}
				if !slices.Equal(fromValue, toValue) {
					// Case 2: The value at this path has been modified
					return JsonDiff{
						Key:  key,
						From: types.NewLazyJSONDocument(fromValue),
						To:   types.NewLazyJSONDocument(toValue),
						Type: ModifiedDiff,
					}, nil
				}
				// Case 1: This location has not changed
				continue

			case -1:
				key := fromCurrentLocation.Clone().key
				// Case 3: A value has been removed from an object
				removedValue, err := jd.currentFromCursor.NextValue(ctx)
				if err != nil {
					return JsonDiff{}, err
				}
				err = advanceCursor(&jd.currentFromCursor)
				if err != nil {
					return JsonDiff{}, err
				}
				return JsonDiff{
					Key:  key,
					From: types.NewLazyJSONDocument(removedValue),
					Type: RemovedDiff,
				}, nil
			case 1:
				key := toCurrentLocation.Clone().key
				// Case 3: A value has been added to an object
				addedValue, err := jd.currentToCursor.NextValue(ctx)
				if err != nil {
					return JsonDiff{}, err
				}
				err = advanceCursor(&jd.currentToCursor)
				if err != nil {
					return JsonDiff{}, err
				}
				return JsonDiff{
					Key:  key,
					To:   types.NewLazyJSONDocument(addedValue),
					Type: AddedDiff,
				}, nil
			}
		}

		if !fromScannerAtStartOfValue && toScannerAtStartOfValue {
			if fromCurrentLocation.getScannerState() != endOfValue {
				return JsonDiff{}, jsonParseError
			}
			key := toCurrentLocation.Clone().key
			// Case 4: A value has been inserted at the end of an object or array.
			addedValue, err := jd.currentToCursor.NextValue(ctx)
			if err != nil {
				return JsonDiff{}, err
			}
			err = advanceCursor(&jd.currentToCursor)
			if err != nil {
				return JsonDiff{}, err
			}
			return JsonDiff{
				Key:  key,
				To:   types.NewLazyJSONDocument(addedValue),
				Type: AddedDiff,
			}, nil
		}

		if fromScannerAtStartOfValue && !toScannerAtStartOfValue {
			if toCurrentLocation.getScannerState() != endOfValue {
				return JsonDiff{}, jsonParseError
			}
			key := fromCurrentLocation.Clone().key
			// Case 4: A value has been removed from the end of an object or array.
			addedValue, err := jd.currentFromCursor.NextValue(ctx)
			if err != nil {
				return JsonDiff{}, err
			}
			err = advanceCursor(&jd.currentFromCursor)
			if err != nil {
				return JsonDiff{}, err
			}
			return JsonDiff{
				Key:  key,
				From: types.NewLazyJSONDocument(addedValue),
				Type: RemovedDiff,
			}, nil
		}
	}
}
