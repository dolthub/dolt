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
var _ types.SearchableJSON = IndexedJsonDocument{}
var _ fmt.Stringer = IndexedJsonDocument{}
var _ driver.Valuer = IndexedJsonDocument{}

func NewIndexedJsonDocument(root Node, ns NodeStore) IndexedJsonDocument {
	m := StaticMap[jsonLocationKey, address, jsonLocationOrdering]{
		Root:      root,
		NodeStore: ns,
		Order:     jsonLocationOrdering{},
	}
	return IndexedJsonDocument{
		m: m,
		interfaceFunc: sync.OnceValues(func() (interface{}, error) {
			return getInterfaceFromIndexedJsonMap(m)
		}),
	}
}

// Clone implements sql.JSONWrapper. Mutating an IndexedJsonDocument always returns a new IndexedJsonDocument without
// modifying the state of the original. But creating a new instance allows callers to modify the value returned by ToInterface()
// on the "mutable" copy without affecting the value cached on the original.
func (i IndexedJsonDocument) Clone() sql.JSONWrapper {
	// TODO: Add context parameter to JSONWrapper.Clone()
	m := i.m
	return IndexedJsonDocument{
		m: m,
		interfaceFunc: sync.OnceValues(func() (interface{}, error) {
			return getInterfaceFromIndexedJsonMap(m)
		}),
	}
}

// ToInterface implements sql.JSONWrapper
func (i IndexedJsonDocument) ToInterface() (interface{}, error) {
	return i.interfaceFunc()
}

// getInterfaceFromIndexedJsonMap extracts the JSON document from a StaticJsonMap and converts it into an interface{}
func getInterfaceFromIndexedJsonMap(m StaticJsonMap) (val interface{}, err error) {
	ctx := context.Background()
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
	path, err := jsonPathElementsFromMySQLJsonPath([]byte(pathString))
	if err != nil {
		return nil, err
	}

	jCur, err := newJsonCursor(ctx, i.m.NodeStore, i.m.Root, path)
	if err != nil {
		return nil, err
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

	if compareJsonLocations(jsonCursor.GetCurrentPath(), keyPath) == 0 {
		// The key already exists in the document.
		return i, false, nil
	}

	// The key is guaranteed to not exist in the source doc. The cursor is pointing to the start of the subsequent object,
	// which will be the insertion point for the added value.
	jsonChunker, err := newJsonChunker(ctx, jsonCursor, keyPath, i.m.NodeStore)
	if err != nil {
		return nil, false, err
	}

	insertedValueBytes, err := types.MarshallJson(val)
	if err != nil {
		return nil, false, err
	}

	jsonChunker.jScanner.currentPath = keyPath
	jsonChunker.writeValue(ctx, keyPath, insertedValueBytes)

	newRoot, err := jsonChunker.Done(ctx)
	if err != nil {
		return nil, false, err
	}

	return NewIndexedJsonDocument(newRoot, i.m.NodeStore), true, nil
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
