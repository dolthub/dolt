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
	"io"
	"slices"

	"github.com/dolthub/go-mysql-server/sql/types"
)

type IndexedJsonDiffer struct {
	differ                             Differ[jsonLocationKey, *jsonLocationOrdering]
	currentFromCursor, currentToCursor *JsonCursor
	from, to                           IndexedJsonDocument
	started                            bool
}

var _ IJsonDiffer = &IndexedJsonDiffer{}

func NewIndexedJsonDiffer(ctx context.Context, from, to IndexedJsonDocument) (*IndexedJsonDiffer, error) {
	ordering := jsonLocationOrdering{}
	differ, err := DifferFromRoots[jsonLocationKey, *jsonLocationOrdering](ctx, from.m.NodeStore, to.m.NodeStore, from.m.Root, to.m.Root, &ordering, false)
	if err != nil {
		return nil, err
	}
	if ordering.err != nil {
		return nil, err
	}
	// We want to diff the prolly tree as if it was an address map pointing to the individual blob fragments, rather
	// than diffing the blob fragments themselves. We can accomplish this by just replacing the cursors in the differ
	// with their parents.
	differ.from = differ.from.parent
	differ.to = differ.to.parent
	differ.fromStop = differ.fromStop.parent
	differ.toStop = differ.toStop.parent

	var currentFromCursor, currentToCursor *JsonCursor
	if differ.from == nil {
		// The "from" document fits inside a single chunk.
		// We can't create the "from" cursor from the differ, so we create it here instead.
		diffKey := []byte{byte(startOfValue)}
		currentFromCursor, err = newJsonCursorAtStartOfChunk(ctx, from.m.NodeStore, from.m.Root, diffKey)
		if err != nil {
			return nil, err
		}
		// Advance the cursor past the "beginning of document" location, so that it aligns with the "to" cursor no matter what.
		err = advanceCursor(ctx, &currentFromCursor)
		if err != nil {
			return nil, err
		}
	}

	if differ.to == nil {
		// The "to" document fits inside a single chunk.
		// We can't create the "from" cursor from the differ, so we create it here instead.
		diffKey := []byte{byte(startOfValue)}
		currentToCursor, err = newJsonCursorAtStartOfChunk(ctx, to.m.NodeStore, to.m.Root, diffKey)
		if err != nil {
			return nil, err
		}
		// Advance the cursor past the "beginning of document" location, so that it aligns with the "from" cursor no matter what.
		err = advanceCursor(ctx, &currentToCursor)
		if err != nil {
			return nil, err
		}
	}

	return &IndexedJsonDiffer{
		differ:            differ,
		from:              from,
		to:                to,
		currentFromCursor: currentFromCursor,
		currentToCursor:   currentToCursor,
	}, nil
}

func advanceCursor(ctx context.Context, jCur **JsonCursor) (err error) {
	if (*jCur).jsonScanner.atEndOfChunk() {
		err = (*jCur).cur.advance(ctx)
		if err != nil {
			return err
		}
		*jCur = nil
	} else {
		err = (*jCur).jsonScanner.AdvanceToNextLocation()
		if err != nil {
			return err
		}
	}
	return nil
}

// Next computes the next diff between the two JSON documents.
// To accomplish this, it uses the underlying Differ to find chunks that have changed, and uses a pair of JsonCursors
// to walk corresponding chunks.
func (jd *IndexedJsonDiffer) Next(ctx context.Context) (diff JsonDiff, err error) {
	// helper function to advance a JsonCursor and set it to nil if it reaches the end of a chunk

	newAddedDiff := func(key []byte) (JsonDiff, error) {
		addedValue, err := jd.currentToCursor.NextValue(ctx)
		if err != nil {
			return JsonDiff{}, err
		}
		err = advanceCursor(ctx, &jd.currentToCursor)
		if err != nil {
			return JsonDiff{}, err
		}
		return JsonDiff{
			Key:  key,
			To:   types.NewLazyJSONDocument(addedValue),
			Type: AddedDiff,
		}, nil
	}

	newRemovedDiff := func(key []byte) (JsonDiff, error) {
		removedValue, err := jd.currentFromCursor.NextValue(ctx)
		if err != nil {
			return JsonDiff{}, err
		}
		err = advanceCursor(ctx, &jd.currentFromCursor)
		if err != nil {
			return JsonDiff{}, err
		}
		return JsonDiff{
			Key:  key,
			From: types.NewLazyJSONDocument(removedValue),
			Type: RemovedDiff,
		}, nil
	}

	for {
		if jd.currentFromCursor == nil && jd.currentToCursor == nil {
			if jd.differ.from == nil || jd.differ.to == nil {
				// One of the documents fits in a single chunk. We must have walked the entire document by now.
				return JsonDiff{}, io.EOF
			}

			// Either this is the first iteration, or the last iteration exhausted both chunks at the same time.
			// (ie, both chunks ended at the same JSON path). We can use `Differ.Next` to seek to the next difference.
			// Passing advanceCursors=false means that instead of using the returned diff, we read the cursors out of
			// the differ, and advance them manually once we've walked the chunk.
			_, err := jd.differ.next(ctx, false)
			if err != nil {
				return JsonDiff{}, err
			}

			jd.currentFromCursor, err = newJsonCursorFromCursor(ctx, jd.differ.from)
			if err != nil {
				return JsonDiff{}, err
			}
			jd.currentToCursor, err = newJsonCursorFromCursor(ctx, jd.differ.to)
			if err != nil {
				return JsonDiff{}, err
			}
		} else if jd.currentFromCursor == nil {
			// We exhausted the current `from` chunk but not the current `to` chunk. Since the chunk boundaries don't align on
			// the same key, we need to continue into the next chunk.

			// Alternatively, the "to" cursor was created during construction because the "to" document fit in a single chunk,
			// and the "from" cursor hasn't been created yet.

			jd.currentFromCursor, err = newJsonCursorFromCursor(ctx, jd.differ.from)
			if err != nil {
				return JsonDiff{}, err
			}

			err = advanceCursor(ctx, &jd.currentFromCursor)
			if err != nil {
				return JsonDiff{}, err
			}
			continue
		} else if jd.currentToCursor == nil {
			// We exhausted the current `to` chunk but not the current `from` chunk. Since the chunk boundaries don't align on
			// the same key, we need to continue into the next chunk.

			// Alternatively, the "from" cursor was created during construction because the "from" document fit in a single chunk,
			// and the "to" cursor hasn't been created yet.

			jd.currentToCursor, err = newJsonCursorFromCursor(ctx, jd.differ.to)
			if err != nil {
				return JsonDiff{}, err
			}

			err = advanceCursor(ctx, &jd.currentToCursor)
			if err != nil {
				return JsonDiff{}, err
			}
			continue
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
		//
		// The following states aren't actually possible because we will encounter state 2 first.
		// 5) One cursor points to the initial element of an object/array, while the other points to the end of that same path:
		//    - A value has been changed from an object/array to a scalar, or vice-versa.
		// 6) One cursor points to the initial element of an object, while the other points to the initial element of an array:
		//    - The value has been changed from an object to an array, or vice-versa.

		fromScanner := &jd.currentFromCursor.jsonScanner
		toScanner := &jd.currentToCursor.jsonScanner
		fromScannerAtStartOfValue := fromScanner.atStartOfValue()
		toScannerAtStartOfValue := toScanner.atStartOfValue()
		fromCurrentLocation := fromScanner.currentPath
		toCurrentLocation := toScanner.currentPath

		if !fromScannerAtStartOfValue && !toScannerAtStartOfValue {
			// Neither cursor points to the start of a value.
			// This should only be possible if they're at the same location.
			// Do a sanity check, then continue.
			cmp, err := compareJsonLocations(fromCurrentLocation, toCurrentLocation)
			if err != nil {
				return JsonDiff{}, err
			}
			if cmp != 0 {
				return JsonDiff{}, jsonParseError
			}
			err = advanceCursor(ctx, &jd.currentFromCursor)
			if err != nil {
				return JsonDiff{}, err
			}
			err = advanceCursor(ctx, &jd.currentToCursor)
			if err != nil {
				return JsonDiff{}, err
			}
			continue
		}

		if fromScannerAtStartOfValue && toScannerAtStartOfValue {
			cmp, err := compareJsonLocations(fromCurrentLocation, toCurrentLocation)
			if err != nil {
				return JsonDiff{}, err
			}
			switch cmp {
			case 0:
				key := fromCurrentLocation.Clone().key

				fromNextCharacter, err := jd.currentFromCursor.nextCharacter(ctx)
				if err == io.EOF {
					return JsonDiff{}, jsonParseError
				}
				if err != nil {
					return JsonDiff{}, err
				}
				toNextCharacter, err := jd.currentToCursor.nextCharacter(ctx)
				if err == io.EOF {
					return JsonDiff{}, jsonParseError
				}
				if err != nil {
					return JsonDiff{}, err
				}
				// Both sides have the same key. If they're both an object or both an array, continue.
				// Otherwise, compare them and possibly return a modification.
				if (fromNextCharacter == '{' && toNextCharacter == '{') ||
					(fromNextCharacter == '[' && toNextCharacter == '[') {
					err = advanceCursor(ctx, &jd.currentFromCursor)
					if err != nil {
						return JsonDiff{}, err
					}
					err = advanceCursor(ctx, &jd.currentToCursor)
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
				// Case 3: A value has been removed from an object
				key := fromCurrentLocation.Clone().key
				return newRemovedDiff(key)
			case 1:
				// Case 3: A value has been added to an object
				key := toCurrentLocation.Clone().key
				return newAddedDiff(key)
			}
		}

		if !fromScannerAtStartOfValue && toScannerAtStartOfValue {
			if fromCurrentLocation.getScannerState() != endOfValue {
				return JsonDiff{}, jsonParseError
			}
			// Case 4: A value has been inserted at the end of an object or array.
			key := toCurrentLocation.Clone().key
			return newAddedDiff(key)
		}

		if fromScannerAtStartOfValue && !toScannerAtStartOfValue {
			if toCurrentLocation.getScannerState() != endOfValue {
				return JsonDiff{}, jsonParseError
			}
			// Case 4: A value has been removed from the end of an object or array.
			key := fromCurrentLocation.Clone().key
			return newRemovedDiff(key)
		}
	}
}
