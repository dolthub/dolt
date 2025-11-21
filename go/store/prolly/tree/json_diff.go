// Copyright 2023 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"io"
	"reflect"
)

// JsonDiffer produces a stream of JsonDiffs describing the differences
// between two JSON documents.
type JsonDiffer interface {
	Next(ctx context.Context) (JsonDiff, error)
}

// JsonDiff describes a single change between two JSON documents.
// Key is a serialized JsonLocation value describing a path into the document
// where the change occurred.
type JsonDiff struct {
	From sql.JSONWrapper
	To   sql.JSONWrapper
	Key  []byte
	Type DiffType
}

// NoDiffJsonDiffer is a JsonDiffer that reports no diffs
type NoDiffJsonDiffer struct{}

func (differ NoDiffJsonDiffer) Next(ctx context.Context) (diff JsonDiff, err error) {
	return JsonDiff{}, io.EOF
}

// SingleDiffJsonDiffer is a JsonDiffer that reports a single diff
type SingleDiffJsonDiffer struct {
	diff JsonDiff
	done bool
}

func (differ *SingleDiffJsonDiffer) Next(ctx context.Context) (diff JsonDiff, err error) {
	if differ.done {
		return JsonDiff{}, io.EOF
	}
	differ.done = true
	return differ.diff, nil
}

type jsonKeyPair struct {
	value interface{}
	key   string
}

func newInMemoryJsonDiffer(key jsonLocationKey, fromValue interface{}, toValue interface{}) (diff JsonDiffer) {
	if reflect.TypeOf(fromValue) != reflect.TypeOf(toValue) {
		return &SingleDiffJsonDiffer{diff: key.emitModified(fromValue, toValue)}
	} else {
		switch from := fromValue.(type) {
		case types.JsonObject:
			return newInMemoryJsonObjectDiffer(key, from, toValue.(types.JsonObject))
		case types.JsonArray:
			return newInMemoryJsonArrayDiffer(key, from, toValue.(types.JsonArray))
		default:
			if fromValue == toValue {
				return NoDiffJsonDiffer{}
			}
			return &SingleDiffJsonDiffer{diff: key.emitModified(fromValue, toValue)}
		}
	}
}

// InMemoryJsonDiffer computes the diff between two JSON objects that are fully loaded in memory.
type InMemoryJsonDiffer struct {
	currentFromPair *jsonKeyPair
	currentToPair   *jsonKeyPair
	subDiffer       JsonDiffer
	root            jsonLocationKey
	from            types.JSONIter
	to              types.JSONIter
}

var _ JsonDiffer = &InMemoryJsonDiffer{}

func newInMemoryJsonObjectDiffer(root []byte, from, to types.JsonObject) *InMemoryJsonDiffer {
	fromIter := types.NewJSONIter(from)
	toIter := types.NewJSONIter(to)
	return &InMemoryJsonDiffer{
		root: root,
		from: fromIter,
		to:   toIter,
	}
}

func (differ *InMemoryJsonDiffer) Next(ctx context.Context) (diff JsonDiff, err error) {
	for {
		if differ.subDiffer != nil {
			diff, err := differ.subDiffer.Next(ctx)
			if err == io.EOF {
				differ.subDiffer = nil
				differ.currentFromPair = nil
				differ.currentToPair = nil
				continue
			} else if err != nil {
				return JsonDiff{}, err
			}
			return diff, nil
		}
		if differ.currentFromPair == nil && differ.from.HasNext() {
			key, value, err := differ.from.Next()
			if err != nil {
				return JsonDiff{}, err
			}
			differ.currentFromPair = &jsonKeyPair{key: key, value: value}
		}

		if differ.currentToPair == nil && differ.to.HasNext() {
			key, value, err := differ.to.Next()
			if err != nil {
				return JsonDiff{}, err
			}
			differ.currentToPair = &jsonKeyPair{key: key, value: value}
		}

		if differ.currentFromPair == nil && differ.currentToPair == nil {
			return JsonDiff{}, io.EOF
		}

		if differ.currentFromPair == nil && differ.currentToPair != nil {
			diff = differ.root.appendObjectKeyString(differ.currentToPair.key).emitAdded(differ.currentToPair.value)
			differ.currentToPair = nil
			return diff, nil
		} else if differ.currentFromPair != nil && differ.currentToPair == nil {
			diff = differ.root.appendObjectKeyString(differ.currentFromPair.key).emitRemoved(differ.currentFromPair.value)
			differ.currentFromPair = nil
			return diff, nil
		} else { // differ.currentFromPair != nil && differ.currentToPair != nil
			keyCmp := bytes.Compare([]byte(differ.currentFromPair.key), []byte(differ.currentToPair.key))
			if keyCmp > 0 {
				// `to` key comes before `from` key. Right key must have been inserted.
				diff = differ.root.appendObjectKeyString(differ.currentToPair.key).emitAdded(differ.currentToPair.value)
				differ.currentToPair = nil
				return diff, nil
			} else if keyCmp < 0 {
				// `to` key comes after `from` key. Right key must have been deleted.
				diff = differ.root.appendObjectKeyString(differ.currentFromPair.key).emitRemoved(differ.currentFromPair.value)
				differ.currentFromPair = nil
				return diff, nil
			} else {
				key := differ.root.appendObjectKeyString(differ.currentFromPair.key)
				fromValue := differ.currentFromPair.value
				toValue := differ.currentToPair.value
				differ.currentFromPair = nil
				differ.currentToPair = nil
				differ.subDiffer = newInMemoryJsonDiffer(key, fromValue, toValue)
				continue
			}
		}
	}
}

// InMemoryJsonArrayDiffer diffs two JSON arrays that are fully loaded in memory.
// Currently, this works by comparing matching indexes, which means it produces
// confusing results when an insert or removal has shifted the indexes of values.
// TODO: Support Longest Common Subsequence (LCS) approximations for identifying
// inserts and removals, like Git does.
type InMemoryJsonArrayDiffer struct {
	subDiffer                  JsonDiffer
	root                       jsonLocationKey
	idx                        int
	fromJsonArray, toJsonArray types.JsonArray
}

func newInMemoryJsonArrayDiffer(root []byte, fromJsonArray, toJsonArray types.JsonArray) *InMemoryJsonArrayDiffer {
	return &InMemoryJsonArrayDiffer{
		root:          root,
		idx:           0,
		fromJsonArray: fromJsonArray,
		toJsonArray:   toJsonArray,
	}
}

func (differ *InMemoryJsonArrayDiffer) Next(ctx context.Context) (diff JsonDiff, err error) {
	for {
		if differ.subDiffer != nil {
			diff, err := differ.subDiffer.Next(ctx)
			if err == io.EOF {
				differ.subDiffer = nil
				continue
			} else if err != nil {
				return JsonDiff{}, err
			}
			return diff, nil
		}
		if differ.idx >= len(differ.fromJsonArray) && differ.idx >= len(differ.toJsonArray) {
			return JsonDiff{}, io.EOF
		}
		if differ.idx >= len(differ.fromJsonArray) {
			// We've exhausted the "from" array but not the "to" array, indicating elements were added.
			diff = differ.root.appendArrayIndex(differ.idx).emitAdded(differ.toJsonArray[differ.idx])
			differ.idx++
			return diff, nil
		}
		if differ.idx >= len(differ.toJsonArray) {
			// We've exhausted the "to" array but not the "from" array, indicating elements were removed.
			diff = differ.root.appendArrayIndex(differ.idx).emitRemoved(differ.fromJsonArray[differ.idx])
			differ.idx++
			return diff, nil
		}
		key := differ.root.appendArrayIndex(differ.idx)
		fromValue := differ.fromJsonArray[differ.idx]
		toValue := differ.toJsonArray[differ.idx]
		differ.idx++
		differ.subDiffer = newInMemoryJsonDiffer(key, fromValue, toValue)
		continue
	}
}

func NewJsonDiffer(ctx context.Context, from, to sql.JSONWrapper) (differ JsonDiffer, err error) {
	indexedFrom, isFromIndexed := from.(IndexedJsonDocument)
	indexedTo, isToIndexed := to.(IndexedJsonDocument)

	if isFromIndexed && isToIndexed {
		return NewIndexedJsonDiffer(ctx, indexedFrom, indexedTo)
	} else {
		fromObject, err := from.ToInterface(ctx)
		if err != nil {
			return nil, err
		}
		toObject, err := to.ToInterface(ctx)
		if err != nil {
			return nil, err
		}
		return newInMemoryJsonDiffer([]byte{byte(startOfValue)}, fromObject, toObject), nil
	}
}
