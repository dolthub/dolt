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
	"io"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	sqljson "github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type jsonLocationKey = []byte

type address = []byte

type StaticJsonMap = StaticMap[jsonLocationKey, address, *jsonLocationOrdering]

// IndexedJsonDocument is an implementation of sql.JSONWrapper that stores the document in a prolly tree.
// Every leaf node in the tree is a blob containing a substring of the original document. This allows the document
// to be reconstructed by walking the tree and concatenating the values in the leaves.
// Every non-leaf node is an address map, using a jsonLocation as the key. This key corresponds to the final nameable
// location within the JSON substring.
// The |interfaceFunc| field caches the result of ToInterface in the event it gets called multiple times.
type IndexedJsonDocument struct {
	m             StaticJsonMap
	interfaceFunc func() (interface{}, error)
	ctx           context.Context
}

var _ types.JSONBytes = IndexedJsonDocument{}
var _ types.MutableJSON = IndexedJsonDocument{}
var _ fmt.Stringer = IndexedJsonDocument{}
var _ driver.Valuer = IndexedJsonDocument{}

func NewIndexedJsonDocument(ctx context.Context, root Node, ns NodeStore) IndexedJsonDocument {
	m := StaticMap[jsonLocationKey, address, *jsonLocationOrdering]{
		Root:      root,
		NodeStore: ns,
		Order:     &jsonLocationOrdering{},
	}
	return IndexedJsonDocument{
		m: m,
		interfaceFunc: sync.OnceValues(func() (interface{}, error) {
			return getInterfaceFromIndexedJsonMap(ctx, m)
		}),
		ctx: ctx,
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
		ctx: ctx,
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

func tryWithFallback(
	ctx context.Context,
	i IndexedJsonDocument,
	tryFunc func() error,
	fallbackFunc func(document types.JSONDocument) error) error {
	err := tryFunc()
	if err == unknownLocationKeyError || err == unsupportedPathError || err == jsonParseError {
		if err != unsupportedPathError {
			if sqlCtx, ok := ctx.(*sql.Context); ok {
				sqlCtx.GetLogger().Warn(err)
			}
		}
		v, err := i.ToInterface()
		if err != nil {
			return err
		}
		return fallbackFunc(types.JSONDocument{Val: v})
	}
	return err
}

// Lookup implements types.SearchableJSON
func (i IndexedJsonDocument) Lookup(ctx context.Context, pathString string) (result sql.JSONWrapper, err error) {
	err = tryWithFallback(
		ctx,
		i,
		func() error {
			result, err = i.tryLookup(ctx, pathString)
			return err
		},
		func(jsonDocument types.JSONDocument) error {
			result, err = jsonDocument.Lookup(ctx, pathString)
			return err
		})
	return result, err
}

func (i IndexedJsonDocument) tryLookup(ctx context.Context, pathString string) (sql.JSONWrapper, error) {
	path, err := jsonPathElementsFromMySQLJsonPath([]byte(pathString))
	if err != nil {
		return nil, err
	}

	return i.lookupByLocation(ctx, path)
}

func (i IndexedJsonDocument) lookupByLocation(ctx context.Context, path jsonLocation) (sql.JSONWrapper, error) {
	jCur, found, err := newJsonCursor(ctx, i.m.NodeStore, i.m.Root, path, false)
	if err != nil {
		return nil, err
	}

	if !found {
		// The key doesn't exist in the document.
		return nil, nil
	}

	valueBytes, err := jCur.NextValue(ctx)
	if err != nil {
		return nil, err
	}
	return types.NewLazyJSONDocument(valueBytes), nil
}

// Insert implements types.MutableJSON
func (i IndexedJsonDocument) Insert(ctx context.Context, path string, val sql.JSONWrapper) (result types.MutableJSON, changed bool, err error) {
	err = tryWithFallback(
		ctx,
		i,
		func() error {
			result, changed, err = i.tryInsert(ctx, path, val)
			return err
		},
		func(jsonDocument types.JSONDocument) error {
			result, changed, err = jsonDocument.Insert(ctx, path, val)
			return err
		})
	return result, changed, err
}

func (i IndexedJsonDocument) tryInsert(ctx context.Context, path string, val sql.JSONWrapper) (types.MutableJSON, bool, error) {
	keyPath, err := jsonPathElementsFromMySQLJsonPath([]byte(path))
	if err != nil {
		return nil, false, err
	}

	jsonCursor, found, err := newJsonCursor(ctx, i.m.NodeStore, i.m.Root, keyPath, false)
	if err != nil {
		return nil, false, err
	}

	if found {
		// The key already exists in the document.
		return i, false, nil
	}

	return i.insertIntoCursor(ctx, keyPath, jsonCursor, val)
}

func (i IndexedJsonDocument) insertIntoCursor(ctx context.Context, keyPath jsonLocation, jsonCursor *JsonCursor, val sql.JSONWrapper) (IndexedJsonDocument, bool, error) {
	cursorPath := jsonCursor.GetCurrentPath()

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
				return IndexedJsonDocument{}, false, err
			}

			originalValue, err := jsonCursor.NextValue(ctx)
			if err != nil {
				return i, false, err
			}

			insertedValueBytes, err := types.MarshallJson(val)
			if err != nil {
				return IndexedJsonDocument{}, false, err
			}

			jsonChunker.appendJsonToBuffer([]byte(fmt.Sprintf("[%s,%s]", originalValue, insertedValueBytes)))
			err = jsonChunker.processBuffer(ctx)
			if err != nil {
				return IndexedJsonDocument{}, false, err
			}

			newRoot, err := jsonChunker.Done(ctx)
			if err != nil {
				return IndexedJsonDocument{}, false, err
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
	cmp, err := compareJsonLocations(cursorPath, keyPath)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}
	if cmp < 0 && cursorPath.getScannerState() == startOfValue {
		// We just attempted to insert into a scalar.
		return i, false, nil
	}

	insertedValueBytes, err := types.MarshallJson(val)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}

	// The key is guaranteed to not exist in the source doc. The cursor is pointing to the start of the subsequent object,
	// which will be the insertion point for the added value.
	jsonChunker, err := newJsonChunker(ctx, jsonCursor, i.m.NodeStore)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}

	// If required, adds a comma before writing the value.
	if !jsonChunker.jScanner.firstElementOrEndOfEmptyValue() {
		jsonChunker.appendJsonToBuffer([]byte{','})
	}

	// If the value is a newly inserted key, write the key.
	if !keyLastPathElement.isArrayIndex {
		jsonChunker.appendJsonToBuffer([]byte(fmt.Sprintf(`"%s":`, escapeKey(keyLastPathElement.key))))
	}

	// Manually set the chunker's path and offset to the start of the value we're about to insert.
	jsonChunker.jScanner.valueOffset = len(jsonChunker.jScanner.jsonBuffer)
	jsonChunker.jScanner.currentPath = keyPath
	jsonChunker.appendJsonToBuffer(insertedValueBytes)
	err = jsonChunker.processBuffer(ctx)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}

	newRoot, err := jsonChunker.Done(ctx)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}

	return NewIndexedJsonDocument(ctx, newRoot, i.m.NodeStore), true, nil
}

// Remove implements types.MutableJSON
func (i IndexedJsonDocument) Remove(ctx context.Context, path string) (result types.MutableJSON, changed bool, err error) {
	if path == "$" {
		return nil, false, fmt.Errorf("The path expression '$' is not allowed in this context.")
	}
	err = tryWithFallback(
		ctx,
		i,
		func() error {
			result, changed, err = i.tryRemove(ctx, path)
			return err
		},
		func(jsonDocument types.JSONDocument) error {
			result, changed, err = jsonDocument.Remove(ctx, path)
			return err
		})
	return result, changed, err
}

func (i IndexedJsonDocument) tryRemove(ctx context.Context, path string) (types.MutableJSON, bool, error) {
	keyPath, err := jsonPathElementsFromMySQLJsonPath([]byte(path))
	if err != nil {
		return nil, false, err
	}
	return i.removeWithLocation(ctx, keyPath)
}

func (i IndexedJsonDocument) RemoveWithKey(ctx context.Context, key []byte) (IndexedJsonDocument, bool, error) {
	return i.removeWithLocation(ctx, jsonPathFromKey(key))
}

func (i IndexedJsonDocument) removeWithLocation(ctx context.Context, keyPath jsonLocation) (IndexedJsonDocument, bool, error) {
	jsonCursor, found, err := newJsonCursor(ctx, i.m.NodeStore, i.m.Root, keyPath, true)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}
	if !found {
		// The key does not exist in the document.
		return i, false, nil
	}

	// The cursor is now pointing to the end of the value prior to the one being removed.
	jsonChunker, err := newJsonChunker(ctx, jsonCursor, i.m.NodeStore)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}

	startofRemovedLocation := jsonCursor.GetCurrentPath()
	startofRemovedLocation = startofRemovedLocation.Clone()
	isInitialElement := startofRemovedLocation.getScannerState().isInitialElement()

	// Advance the cursor to the end of the value being removed.
	keyPath.setScannerState(endOfValue)
	_, err = jsonCursor.AdvanceToLocation(ctx, keyPath, false)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}

	// If removing the first element of an object/array, skip past the comma, and set the chunker as if it's
	// at the start of the object/array.
	if isInitialElement && jsonCursor.jsonScanner.current() == ',' {
		jsonCursor.jsonScanner.valueOffset++
		jsonChunker.jScanner.currentPath = startofRemovedLocation
	}

	newRoot, err := jsonChunker.Done(ctx)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}

	return NewIndexedJsonDocument(ctx, newRoot, i.m.NodeStore), true, nil
}

// Set implements types.MutableJSON
func (i IndexedJsonDocument) Set(ctx context.Context, path string, val sql.JSONWrapper) (result types.MutableJSON, changed bool, err error) {
	err = tryWithFallback(
		ctx,
		i,
		func() error {
			result, changed, err = i.trySet(ctx, path, val)
			return err
		},
		func(jsonDocument types.JSONDocument) error {
			result, changed, err = jsonDocument.Set(ctx, path, val)
			return err
		})
	return result, changed, err
}

func (i IndexedJsonDocument) trySet(ctx context.Context, path string, val sql.JSONWrapper) (types.MutableJSON, bool, error) {
	keyPath, err := jsonPathElementsFromMySQLJsonPath([]byte(path))
	if err != nil {
		return nil, false, err
	}
	return i.setWithLocation(ctx, keyPath, val)
}

func (i IndexedJsonDocument) SetWithKey(ctx context.Context, key []byte, val sql.JSONWrapper) (IndexedJsonDocument, bool, error) {
	return i.setWithLocation(ctx, jsonPathFromKey(key), val)
}

func (i IndexedJsonDocument) setWithLocation(ctx context.Context, keyPath jsonLocation, val sql.JSONWrapper) (IndexedJsonDocument, bool, error) {
	jsonCursor, found, err := newJsonCursor(ctx, i.m.NodeStore, i.m.Root, keyPath, false)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}

	// The supplied path may be 0-indexing into a scalar, which is the same as referencing the scalar. Remove
	// the index and try again.
	for !found && keyPath.size() > jsonCursor.jsonScanner.currentPath.size() {
		lastKeyPathElement := keyPath.getLastPathElement()
		if !lastKeyPathElement.isArrayIndex || lastKeyPathElement.getArrayIndex() != 0 {
			// The key does not exist in the document.
			break
		}

		keyPath.pop()
		cmp, err := compareJsonLocations(keyPath, jsonCursor.jsonScanner.currentPath)
		if err != nil {
			return IndexedJsonDocument{}, false, err
		}
		found = cmp == 0
	}

	if found {
		return i.replaceIntoCursor(ctx, keyPath, jsonCursor, val)
	} else {
		return i.insertIntoCursor(ctx, keyPath, jsonCursor, val)
	}
}

// Replace implements types.MutableJSON
func (i IndexedJsonDocument) Replace(ctx context.Context, path string, val sql.JSONWrapper) (result types.MutableJSON, changed bool, err error) {
	err = tryWithFallback(
		ctx,
		i,
		func() error {
			result, changed, err = i.tryReplace(ctx, path, val)
			return err
		},
		func(jsonDocument types.JSONDocument) error {
			result, changed, err = jsonDocument.Replace(ctx, path, val)
			return err
		})
	return result, changed, err
}

func (i IndexedJsonDocument) tryReplace(ctx context.Context, path string, val sql.JSONWrapper) (types.MutableJSON, bool, error) {
	keyPath, err := jsonPathElementsFromMySQLJsonPath([]byte(path))
	if err != nil {
		return nil, false, err
	}

	jsonCursor, found, err := newJsonCursor(ctx, i.m.NodeStore, i.m.Root, keyPath, false)
	if err != nil {
		return nil, false, err
	}

	// The supplied path may be 0-indexing into a scalar, which is the same as referencing the scalar. Remove
	// the index and try again.
	for !found && keyPath.size() > jsonCursor.jsonScanner.currentPath.size() {
		lastKeyPathElement := keyPath.getLastPathElement()
		if !lastKeyPathElement.isArrayIndex || lastKeyPathElement.getArrayIndex() != 0 {
			// The key does not exist in the document.
			return i, false, nil
		}

		keyPath.pop()
		cmp, err := compareJsonLocations(keyPath, jsonCursor.jsonScanner.currentPath)
		if err != nil {
			return nil, false, err
		}
		found = cmp == 0
	}

	if !found {
		// The key does not exist in the document.
		return i, false, nil
	}

	return i.replaceIntoCursor(ctx, keyPath, jsonCursor, val)
}

func (i IndexedJsonDocument) replaceIntoCursor(ctx context.Context, keyPath jsonLocation, jsonCursor *JsonCursor, val sql.JSONWrapper) (IndexedJsonDocument, bool, error) {

	// The cursor is now pointing to the start of the value being replaced.
	jsonChunker, err := newJsonChunker(ctx, jsonCursor, i.m.NodeStore)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}

	// Advance the cursor to the end of the value being removed.
	keyPath.setScannerState(endOfValue)
	_, err = jsonCursor.AdvanceToLocation(ctx, keyPath, false)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}

	insertedValueBytes, err := types.MarshallJson(val)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}

	jsonChunker.appendJsonToBuffer(insertedValueBytes)
	err = jsonChunker.processBuffer(ctx)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}

	newRoot, err := jsonChunker.Done(ctx)
	if err != nil {
		return IndexedJsonDocument{}, false, err
	}

	return NewIndexedJsonDocument(ctx, newRoot, i.m.NodeStore), true, nil
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
	return getBytesFromIndexedJsonMap(i.ctx, i.m)
}

func (i IndexedJsonDocument) getFirstCharacter(ctx context.Context) (byte, error) {
	stopIterationError := fmt.Errorf("stop")
	var firstCharacter byte
	err := i.m.WalkNodes(ctx, func(ctx context.Context, nd Node) error {
		if nd.IsLeaf() {
			firstCharacter = nd.GetValue(0)[0]
			return stopIterationError
		}
		return nil
	})
	if err != stopIterationError {
		return 0, err
	}
	return firstCharacter, nil
}

func (i IndexedJsonDocument) getTypeCategory() (jsonTypeCategory, error) {
	firstCharacter, err := i.getFirstCharacter(i.ctx)
	if err != nil {
		return 0, err
	}
	return getTypeCategoryFromFirstCharacter(firstCharacter), nil
}

func GetTypeCategory(wrapper sql.JSONWrapper) (jsonTypeCategory, error) {
	switch doc := wrapper.(type) {
	case IndexedJsonDocument:
		return doc.getTypeCategory()
	case *types.LazyJSONDocument:
		return getTypeCategoryFromFirstCharacter(doc.Bytes[0]), nil
	default:
		val, err := doc.ToInterface()
		if err != nil {
			return 0, err
		}
		return getTypeCategoryOfValue(val)
	}
}

// Type implements types.ComparableJson
func (i IndexedJsonDocument) Type(ctx context.Context) (string, error) {
	firstCharacter, err := i.getFirstCharacter(ctx)
	if err != nil {
		return "", err
	}

	switch firstCharacter {
	case '{':
		return "OBJECT", nil
	case '[':
		return "ARRAY", nil
	}
	// At this point the value must be a scalar, so it's okay to just load the whole thing.
	val, err := i.ToInterface()
	if err != nil {
		return "", err
	}
	return sqljson.TypeOfJsonValue(val), nil
}

// Compare implements types.ComparableJson
func (i IndexedJsonDocument) Compare(other interface{}) (int, error) {
	thisTypeCategory, err := i.getTypeCategory()
	if err != nil {
		return 0, err
	}

	otherIndexedDocument, ok := other.(IndexedJsonDocument)
	if !ok {
		val, err := i.ToInterface()
		if err != nil {
			return 0, err
		}
		otherVal := other
		if otherWrapper, ok := other.(sql.JSONWrapper); ok {
			otherVal, err = otherWrapper.ToInterface()
			if err != nil {
				return 0, err
			}
		}
		return types.CompareJSON(val, otherVal)
	}

	otherTypeCategory, err := otherIndexedDocument.getTypeCategory()
	if err != nil {
		return 0, err
	}
	if thisTypeCategory < otherTypeCategory {
		return -1, nil
	}
	if thisTypeCategory > otherTypeCategory {
		return 1, nil
	}
	switch thisTypeCategory {
	case jsonTypeNull:
		return 0, nil
	case jsonTypeArray, jsonTypeObject:
		// To compare two values that are both arrays or both objects, we must locate the first location
		// where they differ.

		jsonDiffer, err := NewIndexedJsonDiffer(i.ctx, i, otherIndexedDocument)
		if err != nil {
			return 0, err
		}
		firstDiff, err := jsonDiffer.Next(i.ctx)
		if err == io.EOF {
			// The two documents have no differences.
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		switch firstDiff.Type {
		case AddedDiff:
			// A key is present in other but not this.
			return -1, nil
		case RemovedDiff:
			return 1, nil
		case ModifiedDiff:
			// Since both modified values have already been loaded into memory,
			// We can just compare them.
			return types.JSON.Compare(firstDiff.From, firstDiff.To)
		default:
			panic("Impossible diff type")
		}
	default:
		val, err := i.ToInterface()
		if err != nil {
			return 0, err
		}
		return types.CompareJSON(val, other)
	}
}
