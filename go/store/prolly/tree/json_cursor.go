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
	"bytes"
	"context"
	"fmt"
	"io"
)

// JsonCursor wraps a cursor to an IndexedJsonDocument stored in a prolly tree. This allows seeking for a specific
// location in the stored JSON document.
type JsonCursor struct {
	cur         *cursor
	jsonScanner JsonScanner
}

// getPreviousKey returns the boundary before a JSON leaf without reading its predecessor Blob.
// A parentless leaf or the first leaf in the tree has no preceding boundary.
func getPreviousKey(ctx context.Context, cur *cursor) ([]byte, error) {
	if cur.parent == nil {
		return nil, nil
	}
	previous := cur.parent.clone()
	if err := previous.retreat(ctx); err != nil {
		return nil, err
	}
	if !previous.Valid() {
		return nil, nil
	}
	key := previous.CurrentKey()
	if err := errorIfNotSupportedLocation(key); err != nil {
		return nil, err
	}
	return key, nil
}

// newJsonCursor takes the root node of a prolly tree representing a JSON document, and creates a new JsonCursor for reading
// JSON, starting at the specified location in the document. Returns a boolean indicating whether the location already exists
// in the document. If the location does not exist in the document, the resulting JsonCursor
// will be at the location where the value would be if it was inserted.
func newJsonCursor(ctx context.Context, ns NodeStore, root *Node, startKey jsonLocation, forRemoval bool) (jCur *JsonCursor, found bool, err error) {
	jcur, err := newJsonCursorAtStartOfChunk(ctx, ns, root, startKey.key)
	if err != nil {
		return nil, false, err
	}
	found, err = jcur.AdvanceToLocation(ctx, startKey, forRemoval)
	return jcur, found, err
}

func newJsonCursorAtStartOfChunk(ctx context.Context, ns NodeStore, root *Node, startKey jsonLocationKey) (jCur *JsonCursor, err error) {
	ordering := jsonLocationOrdering{}
	cur, err := newCursorAtKey(ctx, ns, root, startKey, &ordering)
	if err != nil {
		return nil, err
	}
	return newJsonCursorFromCursor(ctx, cur)
}

// newJsonCursorFromCursor descends to a Blob before initializing its boundary scanner.
func newJsonCursorFromCursor(ctx context.Context, cur *cursor) (*JsonCursor, error) {
	if !cur.isLeaf() {
		nd, err := fetchChild(ctx, cur.nrw, cur.currentRef())
		if err != nil {
			return nil, err
		}
		return newJsonCursorFromCursor(ctx, &cursor{nd: nd, parent: cur, nrw: cur.nrw})
	}
	previousKey, err := getPreviousKey(ctx, cur)
	if err != nil {
		return nil, err
	}
	jsonBytes := cur.currentValue()
	jsonDecoder := ScanJsonFromMiddleWithKey(jsonBytes, previousKey)

	jcur := JsonCursor{cur: cur, jsonScanner: jsonDecoder}

	return &jcur, nil
}

func (j JsonCursor) Valid() bool {
	return j.cur.Valid()
}

// NextValue reads and consumes an entire value from the JSON document, returning its bytes.
// Precondition: The scanner is currently at the start of a value.
func (j *JsonCursor) NextValue(ctx context.Context) (result []byte, err error) {
	if !j.jsonScanner.atStartOfValue() {
		return nil, fmt.Errorf("JSON cursor in unexpected state. This is likely a bug")
	}
	path := j.jsonScanner.currentPath.Clone()
	path.setScannerState(endOfValue)
	jsonBuffer := j.jsonScanner.jsonBuffer
	startPos := j.jsonScanner.valueOffset

	parseChunk := func() {
		var crossedBoundary bool
		crossedBoundary, err = j.AdvanceToNextLocation(ctx)
		if err != nil {
			return
		}
		if crossedBoundary {
			result = append(result, jsonBuffer[startPos:]...)
			jsonBuffer = j.jsonScanner.jsonBuffer
			startPos = 0
		}
	}

	parseChunk()
	if err != nil {
		return
	}

	for {
		var cmp int
		cmp, err = compareJsonLocations(j.jsonScanner.currentPath, path)
		if err != nil {
			return
		}
		if cmp >= 0 {
			break
		}
		parseChunk()
		if err != nil {
			return
		}
	}
	result = append(result, jsonBuffer[startPos:j.jsonScanner.valueOffset]...)
	return
}

func (j *JsonCursor) isKeyInChunk(path jsonLocation) (bool, error) {
	if j.cur.parent == nil {
		// This is the only chunk, so the path must refer to this chunk.
		return true, nil
	}
	nodeEndPosition := jsonPathFromKey(j.cur.parent.CurrentKey())
	cmp, err := compareJsonLocations(path, nodeEndPosition)
	return cmp <= 0, err
}

// AdvanceToLocation causes the cursor to advance to the specified position. This function returns a boolean indicating
// whether the position already exists. If it doesn't, the cursor stops at the location where the value would be if it
// were inserted.
// The `forRemoval` parameter changes the behavior when advancing to the start of an object key. When this parameter is true,
// the cursor advances to the end of the previous value, prior to the object key. This allows the key to be removed along
// with the value.
func (j *JsonCursor) AdvanceToLocation(ctx context.Context, path jsonLocation, forRemoval bool) (found bool, err error) {
	return j.advanceToLocation(ctx, path, forRemoval, false)
}

// advanceToLocation can treat the end of a leaf as the start of its next
// array value when the first value shares the offset of the opening bracket.
func (j *JsonCursor) advanceToLocation(ctx context.Context, path jsonLocation, forRemoval, allowEntryAtBoundary bool) (found bool, err error) {
	isInChunk, err := j.isKeyInChunk(path)
	if err != nil {
		return false, err
	}
	if !isInChunk {
		// Our destination is in another chunk, load it.
		ordering := jsonLocationOrdering{}
		err := Seek(ctx, j.cur.parent, path.key, &ordering)
		if err != nil {
			return false, err
		}
		j.cur.nd, err = fetchChild(ctx, j.cur.nrw, j.cur.parent.currentRef())
		if err != nil {
			return false, err
		}
		previousKey, err := getPreviousKey(ctx, j.cur)
		if err != nil {
			return false, err
		}
		j.jsonScanner = ScanJsonFromMiddleWithKey(j.cur.currentValue(), previousKey)
	}

	previousScanner := j.jsonScanner
	cmp, err := compareJsonLocations(j.jsonScanner.currentPath, path)
	if err != nil {
		return false, err
	}
	for cmp < 0 {
		if allowEntryAtBoundary && j.jsonScanner.atEndOfChunk() &&
			j.jsonScanner.currentPath.getScannerState() == arrayInitialElement && j.cur.parent != nil {
			// The first array element can start at the same byte as the
			// opening bracket's end. The index records that transition.
			boundary := j.cur.parent.CurrentKey()
			if len(boundary) > 0 && boundary[0] == path.key[0] {
				found, err := EqualJsonDiffKeys(path.key, boundary)
				if found || err != nil {
					return found, err
				}
			}
		}
		previousScanner = j.jsonScanner.Clone()
		err := j.jsonScanner.AdvanceToNextLocation()
		if err == io.EOF {
			if allowEntryAtBoundary {
				return false, jsonParseError
			}
			// We reached the end of the document without finding the path. This shouldn't be possible, because
			// there is no path greater than the end-of-document path, which is always the last key.
			panic("Reached the end of the JSON document while advancing. This should not be possible. Is the document corrupt?")
		} else if err != nil {
			return false, err
		}
		cmp, err = compareJsonLocations(j.jsonScanner.currentPath, path)
		if err != nil {
			return false, err
		}
	}
	// If the supplied path doesn't exist in the document, then we want to stop the cursor at the start of the point
	// were it would appear. This may mean that we've gone too far and need to rewind one location.
	if cmp > 0 {
		j.jsonScanner = previousScanner
		return false, nil
	}
	// If the element is going to be removed, rewind the scanner one location, to before the key of the removed object.
	if forRemoval {
		j.jsonScanner = previousScanner
	}
	return true, nil
}

// advanceCursor resumes at the next leaf using the original input boundary, even
// when its bytes were copied without scanning. It returns io.EOF at the tree's end.
func (j *JsonCursor) advanceCursor(ctx context.Context) error {
	var boundary []byte
	if j.cur.parent != nil {
		boundary = j.cur.parent.CurrentKey()
	}
	if err := j.cur.advance(ctx); err != nil {
		return fmt.Errorf("advance JSON cursor: %w", err)
	}
	if !j.cur.Valid() {
		return io.EOF
	}
	path := j.jsonScanner.currentPath
	if !bytes.Equal(path.key, boundary) {
		path = jsonPathFromKey(boundary)
	}
	j.jsonScanner = ScanJsonFromMiddle(j.cur.currentValue(), path)
	return nil
}

func (j *JsonCursor) AdvanceToNextLocation(ctx context.Context) (crossedBoundary bool, err error) {
	err = j.jsonScanner.AdvanceToNextLocation()
	if err == io.EOF {
		crossedBoundary = true
		// We hit the end of the chunk, load the next one
		err = j.advanceCursor(ctx)
		if err != nil {
			return false, err
		}
		return true, j.jsonScanner.AdvanceToNextLocation()
	} else if err != nil {
		return
	}
	return

}

func (j *JsonCursor) GetCurrentPath() jsonLocation {
	return j.jsonScanner.currentPath
}

func (j *JsonCursor) nextCharacter(ctx context.Context) (byte, error) {
	if j.jsonScanner.atEndOfChunk() {
		err := j.advanceCursor(ctx)
		if err != nil {
			return 255, err
		}
	}
	return j.jsonScanner.jsonBuffer[j.jsonScanner.valueOffset], nil
}
