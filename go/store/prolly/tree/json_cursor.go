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
	"fmt"
	"io"
)

// JsonCursor wraps a cursor to an IndexedJsonDocument stored in a prolly tree. This allows seeking for a specific
// location in the stored JSON document.
type JsonCursor struct {
	cur         *cursor
	jsonScanner JsonScanner
}

// getPreviousKey computes the key of a cursor's predecessor node. This is important for scanning JSON because that
// key represents the location within the document where the current node begins, and can be used to compute the location
// of every value within the node.
func getPreviousKey(ctx context.Context, cur *cursor) ([]byte, error) {
	cur2 := cur.clone()
	err := cur2.retreat(ctx)
	if err != nil {
		return nil, err
	}
	// If we're at the start of the tree, return nil.
	if !cur2.Valid() {
		return nil, nil
	}
	key := cur2.CurrentKey()
	if len(key) == 0 {
		key = cur2.parent.CurrentKey()
	}
	err = errorIfNotSupportedLocation(key)
	if err != nil {
		return nil, err
	}
	return key, nil
}

// newJsonCursor takes the root node of a prolly tree representing a JSON document, and creates a new JsonCursor for reading
// JSON, starting at the specified location in the document. Returns a boolean indicating whether the location already exists
// in the document. If the location does not exist in the document, the resulting JsonCursor
// will be at the location where the value would be if it was inserted.
func newJsonCursor(ctx context.Context, ns NodeStore, root Node, startKey jsonLocation, forRemoval bool) (jCur *JsonCursor, found bool, err error) {
	jcur, err := newJsonCursorAtStartOfChunk(ctx, ns, root, startKey.key)
	if err != nil {
		return nil, false, err
	}
	found, err = jcur.AdvanceToLocation(ctx, startKey, forRemoval)
	return jcur, found, err
}

func newJsonCursorAtStartOfChunk(ctx context.Context, ns NodeStore, root Node, startKey []byte) (jCur *JsonCursor, err error) {
	ordering := jsonLocationOrdering{}
	cur, err := newCursorAtKey(ctx, ns, root, startKey, &ordering)
	if err != nil {
		return nil, err
	}
	if ordering.err != nil {
		return nil, err
	}
	return newJsonCursorFromCursor(ctx, cur)
}

func newJsonCursorFromCursor(ctx context.Context, cur *cursor) (*JsonCursor, error) {
	previousKey, err := getPreviousKey(ctx, cur)
	if err != nil {
		return nil, err
	}
	if !cur.isLeaf() {
		nd, err := fetchChild(ctx, cur.nrw, cur.currentRef())
		if err != nil {
			return nil, err
		}
		return newJsonCursorFromCursor(ctx, &cursor{nd: nd, parent: cur, nrw: cur.nrw})
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
		if cmp < 0 {
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
		if ordering.err != nil {
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
		previousScanner = j.jsonScanner.Clone()
		err := j.jsonScanner.AdvanceToNextLocation()
		if err == io.EOF {
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

func (j *JsonCursor) advanceCursor(ctx context.Context) error {
	err := j.cur.advance(ctx)
	if err != nil {
		return err
	}
	if !j.cur.Valid() {
		// We hit the end of the tree. This shouldn't happen.
		return io.EOF
	}
	j.jsonScanner = ScanJsonFromMiddle(j.cur.currentValue(), j.jsonScanner.currentPath)
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
