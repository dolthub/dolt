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
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"sync"
)

type jsonLocationKey = []byte

type address = []byte

type StaticJsonMap = StaticMap[jsonLocationKey, address, jsonLocationOrdering]

// IndexedJsonDocument is an implementation of sql.JSONWrapper that stores the document in a prolly tree.
// Every leaf node in the tree is a blob containing a substring of the original document. This allows the document
// to be reconstructed by walking the tree and concatenating the values in the leaves.
// Every non-leaf node is an address map, using a jsonLocation as the key. This key corresponds to the final nameable
// location within the JSON substring.
// The |interfaceFunc| field caches the result of ToInterface in the event it gets called multiple times.
type IndexedJsonDocument struct {
	m             StaticJsonMap
	interfaceFunc func() (interface{}, error)
}

var _ types.JSONBytes = IndexedJsonDocument{}
var _ types.MutableJSON = IndexedJsonDocument{}
var _ fmt.Stringer = IndexedJsonDocument{}
var _ driver.Valuer = IndexedJsonDocument{}

func NewIndexedJsonDocument(ctx context.Context, root Node, ns NodeStore) IndexedJsonDocument {
	m := StaticMap[jsonLocationKey, address, jsonLocationOrdering]{
		Root:      root,
		NodeStore: ns,
		Order:     jsonLocationOrdering{},
	}
	return IndexedJsonDocument{
		m: m,
		interfaceFunc: sync.OnceValues(func() (interface{}, error) {
			return getInterfaceFromIndexedJsonMap(ctx, m)
		}),
	}
}

// Clone implements sql.JSONWrapper. Mutating an IndexedJsonDocument always returns a new IndexedJsonDocument without
// modifying the state of the original. But creating a new instance allows callers to modify the value returned by ToInterface()
// on the "mutable" copy without affecting the value cached on the original.
func (i IndexedJsonDocument) Clone(ctx context.Context) sql.JSONWrapper {
	m := i.m
	return IndexedJsonDocument{
		m: m,
		interfaceFunc: sync.OnceValues(func() (interface{}, error) {
			return getInterfaceFromIndexedJsonMap(ctx, m)
		}),
	}
}

// ToInterface implements sql.JSONWrapper
func (i IndexedJsonDocument) ToInterface() (interface{}, error) {
	return i.interfaceFunc()
}

// getInterfaceFromIndexedJsonMap extracts the JSON document from a StaticJsonMap and converts it into an interface{}
func getInterfaceFromIndexedJsonMap(ctx context.Context, m StaticJsonMap) (val interface{}, err error) {
	jsonBytes, err := getBytesFromIndexedJsonMap(ctx, m)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(jsonBytes, &val)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// getInterfaceFromIndexedJsonMap extracts the JSON bytes from a StaticJsonMap
func getBytesFromIndexedJsonMap(ctx context.Context, m StaticJsonMap) (bytes []byte, err error) {
	err = m.WalkNodes(ctx, func(ctx context.Context, n Node) error {
		if n.IsLeaf() {
			bytes = append(bytes, n.GetValue(0)...)
		}
		return nil
	})
	return bytes, err
}

// Lookup implements types.SearchableJSON
func (i IndexedJsonDocument) Lookup(ctx context.Context, pathString string) (sql.JSONWrapper, error) {
	if strings.Contains(pathString, "*") {
		// Optimized lookup doesn't currently support wildcards. Fall back on an unoptimized approach.
		// TODO: Optimized lookup on wildcards.
		val, err := i.ToInterface()
		if err != nil {
			return nil, err
		}
		nonIndexedDoc := types.JSONDocument{Val: val}
		return nonIndexedDoc.Lookup(ctx, pathString)
	}
	path, err := jsonPathElementsFromMySQLJsonPath([]byte(pathString))
	if err != nil {
		return nil, err
	}

	jCur, err := newJsonCursor(ctx, i.m.NodeStore, i.m.Root, path)
	if err != nil {
		return nil, err
	}

	cursorPath := jCur.GetCurrentPath()
	cmp := compareJsonLocations(cursorPath, path)
	if cmp != 0 {
		// The key doesn't exist in the document.
		return nil, nil
	}

	valueBytes, err := jCur.NextValue(ctx)
	if err != nil {
		return nil, err
	}
	return &types.LazyJSONDocument{Bytes: valueBytes}, nil
}

// Insert implements types.MutableJSON
func (i IndexedJsonDocument) Insert(path string, val sql.JSONWrapper) (types.MutableJSON, bool, error) {
	// TODO: Add context parameter to MutableJSON.Insert
	ctx := context.Background()
	keyPath, err := jsonPathElementsFromMySQLJsonPath([]byte(path))
	if err != nil {
		return nil, false, err
	}

	jsonCursor, err := newJsonCursor(ctx, i.m.NodeStore, i.m.Root, keyPath)
	if err != nil {
		return nil, false, err
	}

	cursorPath := jsonCursor.GetCurrentPath()
	cmp := compareJsonLocations(cursorPath, keyPath)
	if cmp == 0 {
		// The key already exists in the document.
		return i, false, nil
	}

	// If the inserted path is equivalent to "$" (which also includes "$[0]" on non-arrays), do nothing.
	if cursorPath.size() == 0 && cursorPath.getScannerState() == startOfValue {
		return i, false, nil
	}

	// Attempting to insert an object key into an array should result in no modification.
	// TODO: These are terrible variable names, come up with better ones.
	keyLastPathElement := keyPath.getLastPathElement()
	cursorLastPathElement := cursorPath.getLastPathElement()
	if cursorLastPathElement.isArrayIndex && !keyLastPathElement.isArrayIndex {
		return i, false, nil
	}

	// If the insertion is valid, then |jsonCursor| now points to the correct insertion point.
	// But the insertion may not be valid if the user-supplied path does not exist.
	// (For instance, attempting to insert into the document {"a": 1} at the path "$.b.c" should do nothing.)
	// We can check this by comparing the insertion point with the provided key: if the user-supplied path ends in ".b.c",
	// then the insertion point must be inside b (either be the initial location of b, or the start or end of one of b's children.)
	switch cursorPath.size() {
	case keyPath.size() - 1:
		// Attempting to treat a scalar or object like an array is unusual: inserting into index > 0 wraps the scalar or object
		// in an array and appends to it.
		if keyLastPathElement.isArrayIndex && !cursorLastPathElement.isArrayIndex {
			arrayIndex := keyLastPathElement.getArrayIndex()
			if arrayIndex == 0 {
				// Either the target path doesn't exist (and this is a no-op), or we're attempting to write to a
				// location that already exists (and this is also a no-op)
				return i, false, nil
			}
			// Wrap the original value in an array and append this to it.

			jsonChunker, err := newJsonChunker(ctx, jsonCursor, i.m.NodeStore)
			if err != nil {
				return nil, false, err
			}

			originalValue, err := jsonCursor.NextValue(ctx)
			if err != nil {
				return i, false, err
			}

			insertedValueBytes, err := types.MarshallJson(val)
			if err != nil {
				return nil, false, err
			}

			jsonChunker.appendJsonToBuffer([]byte(fmt.Sprintf("[%s,%s]", originalValue, insertedValueBytes)))
			jsonChunker.processBuffer(ctx)

			newRoot, err := jsonChunker.Done(ctx)
			if err != nil {
				return nil, false, err
			}

			return NewIndexedJsonDocument(ctx, newRoot, i.m.NodeStore), true, nil

		}
		if cursorPath.getScannerState() != arrayInitialElement && cursorPath.getScannerState() != objectInitialElement {
			return i, false, nil
		}
	case keyPath.size():
	default:
		return i, false, nil
	}

	// A bad path may attempt to treat a scalar like an object.
	// For example, attempting to insert into the path "$.a.b" in the document {"a": 1}
	// We can detect this by checking to see if the insertion point in the original document comes before the inserted path.
	// (For example, the insertion point occurs at $.a.START, which is before $.a.b)
	if cmp < 0 && cursorPath.getScannerState() == startOfValue {
		// We just attempted to insert into a scalar.
		return i, false, nil
	}

	insertedValueBytes, err := types.MarshallJson(val)
	if err != nil {
		return nil, false, err
	}

	// The key is guaranteed to not exist in the source doc. The cursor is pointing to the start of the subsequent object,
	// which will be the insertion point for the added value.
	jsonChunker, err := newJsonChunker(ctx, jsonCursor, i.m.NodeStore)
	if err != nil {
		return nil, false, err
	}

	// If required, adds a comma before writing the value.
	if !jsonChunker.jScanner.firstElementOrEndOfEmptyValue() {
		jsonChunker.appendJsonToBuffer([]byte{','})
	}

	// If the value is a newly inserted key, write the key.
	if !keyLastPathElement.isArrayIndex {
		jsonChunker.appendJsonToBuffer([]byte(fmt.Sprintf(`"%s":`, keyLastPathElement.key)))
	}

	// Manually set the chunker's path and offset to the start of the value we're about to insert.
	jsonChunker.jScanner.valueOffset = len(jsonChunker.jScanner.jsonBuffer)
	jsonChunker.jScanner.currentPath = keyPath
	jsonChunker.appendJsonToBuffer(insertedValueBytes)
	jsonChunker.processBuffer(ctx)

	newRoot, err := jsonChunker.Done(ctx)
	if err != nil {
		return nil, false, err
	}

	return NewIndexedJsonDocument(ctx, newRoot, i.m.NodeStore), true, nil
}

// Remove is not yet implemented, so we call it on a types.JSONDocument instead.
func (i IndexedJsonDocument) Remove(path string) (types.MutableJSON, bool, error) {
	v, err := i.ToInterface()
	if err != nil {
		return nil, false, err
	}
	return types.JSONDocument{Val: v}.Remove(path)
}

// Set is not yet implemented, so we call it on a types.JSONDocument instead.
func (i IndexedJsonDocument) Set(path string, val sql.JSONWrapper) (types.MutableJSON, bool, error) {
	v, err := i.ToInterface()
	if err != nil {
		return nil, false, err
	}
	return types.JSONDocument{Val: v}.Set(path, val)
}

// Replace is not yet implemented, so we call it on a types.JSONDocument instead.
func (i IndexedJsonDocument) Replace(path string, val sql.JSONWrapper) (types.MutableJSON, bool, error) {
	v, err := i.ToInterface()
	if err != nil {
		return nil, false, err
	}
	return types.JSONDocument{Val: v}.Replace(path, val)
}

// ArrayInsert is not yet implemented, so we call it on a types.JSONDocument instead.
func (i IndexedJsonDocument) ArrayInsert(path string, val sql.JSONWrapper) (types.MutableJSON, bool, error) {
	v, err := i.ToInterface()
	if err != nil {
		return nil, false, err
	}
	return types.JSONDocument{Val: v}.ArrayInsert(path, val)
}

// ArrayAppend is not yet implemented, so we call it on a types.JSONDocument instead.
func (i IndexedJsonDocument) ArrayAppend(path string, val sql.JSONWrapper) (types.MutableJSON, bool, error) {
	v, err := i.ToInterface()
	if err != nil {
		return nil, false, err
	}
	return types.JSONDocument{Val: v}.ArrayAppend(path, val)
}

// Value implements driver.Valuer for interoperability with other go libraries
func (i IndexedJsonDocument) Value() (driver.Value, error) {
	return types.StringifyJSON(i)
}

// String implements the fmt.Stringer interface.
func (i IndexedJsonDocument) String() string {
	s, err := types.StringifyJSON(i)
	if err != nil {
		return fmt.Sprintf("error while stringifying JSON: %s", err.Error())
	}
	return s
}

// GetBytes implements the JSONBytes interface.
func (i IndexedJsonDocument) GetBytes() (bytes []byte, err error) {
	// TODO: Add context parameter to JSONBytes.GetBytes
	ctx := context.Background()
	return getBytesFromIndexedJsonMap(ctx, i.m)
}
