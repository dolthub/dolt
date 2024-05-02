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

package doltdb

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// DoltgresRootObject is an object that is located on the root, and is used by Doltgres. These are used for when
// standard tables are not sufficient, such as with sequences operating outside the standard table-update model.
type DoltgresRootObject interface {
	// Index is a unique identifier for this particular kind of root object. This will be used when merging to determine
	// which root objects need to be merged together, as well as during serialization and deserialization. Unlike a
	// standard identifier, Index is safe to use as an index to a slice or array.
	Index() DoltgresRootObjectIndex
	// Changed returns whether the root object has changed in a way that will be reflected upon serialization. Also
	// returns the hash that was provided during deserialization, which means that a change will result in a different
	// hash being calculated (and therefore, no change would produce the same hash). It is worth noting that the
	// aforementioned will not be true after serializing the root object, although these cases are properly handled.
	Changed() (bool, hash.Hash)
}

// DoltgresRootObjectIndex is a unique identifier for a DoltgresRootObjects. Unlike standard identifiers, indexes must
// start at zero and increment contiguously.
type DoltgresRootObjectIndex uint32

// DoltgresRootObjectSerializationFunc handles serialization for a specific index of DoltgresRootObject. It is important
// to note that nil slices and zero-length slices are treated differently, in that a nil slice signifies that the root
// object is exactly equivalent to its default state. The serialization function must also modify the given root object
// such that any calls to Change() that immediately follow serialization will return false.
type DoltgresRootObjectSerializationFunc func(ctx context.Context, rootObj DoltgresRootObject) ([]byte, error)

// DoltgresRootObjectDeserializationFunc handles deserialization for a specific index of DoltgresRootObject. If a nil
// slice is received, then this must return a valid DoltgresRootObject that is in its default state. This is distinct
// from the zero-length slice, which is not nil. The given hash must remain with the returned object for the life of the
// object. Any calls to Changed() that immediately follow deserialization must return false.
type DoltgresRootObjectDeserializationFunc func(ctx context.Context, data []byte, h hash.Hash) (DoltgresRootObject, error)

// DoltgresRootObjectMergeFunc handles merging for a specific index of DoltgresRootObject. All given objects will be
// non-nil. Any calls to Changed() that immediately follows merging for the returned root object must return true, and
// also return an empty hash.
type DoltgresRootObjectMergeFunc func(ctx context.Context, ourRootObj, theirRootObj, ancRootObj DoltgresRootObject) (DoltgresRootObject, error)

// The internal collection of functions for serialization, deserialization, and merging root objects.
var (
	doltgresRootObjectSerializationFuncs   []DoltgresRootObjectSerializationFunc
	doltgresRootObjectDeserializationFuncs []DoltgresRootObjectDeserializationFunc
	doltgresRootObjectMergeFuncs           []DoltgresRootObjectMergeFunc
)

// RegisterDoltgresRootObjectSerialization registers the given serialization function for the given index. Panics if
// a function has already been registered for this index.
func RegisterDoltgresRootObjectSerialization(index DoltgresRootObjectIndex, f DoltgresRootObjectSerializationFunc) {
	if int64(index) >= int64(len(doltgresRootObjectSerializationFuncs)) {
		newFuncs := make([]DoltgresRootObjectSerializationFunc, index+1)
		copy(newFuncs, doltgresRootObjectSerializationFuncs)
		doltgresRootObjectSerializationFuncs = newFuncs
	}
	if doltgresRootObjectSerializationFuncs[index] != nil {
		panic("duplicate index on serialization registration")
	}
	doltgresRootObjectSerializationFuncs[index] = f
}

// RegisterDoltgresRootObjectDeserialization registers the given deserialization function for the given index. Panics if
// a function has already been registered for this index.
func RegisterDoltgresRootObjectDeserialization(index DoltgresRootObjectIndex, f DoltgresRootObjectDeserializationFunc) {
	if int64(index) >= int64(len(doltgresRootObjectDeserializationFuncs)) {
		newFuncs := make([]DoltgresRootObjectDeserializationFunc, index+1)
		copy(newFuncs, doltgresRootObjectDeserializationFuncs)
		doltgresRootObjectDeserializationFuncs = newFuncs
	}
	if doltgresRootObjectDeserializationFuncs[index] != nil {
		panic("duplicate index on deserialization registration")
	}
	doltgresRootObjectDeserializationFuncs[index] = f
}

// RegisterDoltgresRootObjectMerge registers the given merge function for the given index. Panics if a function has
// already been registered for this index.
func RegisterDoltgresRootObjectMerge(index DoltgresRootObjectIndex, f DoltgresRootObjectMergeFunc) {
	if int64(index) >= int64(len(doltgresRootObjectMergeFuncs)) {
		newFuncs := make([]DoltgresRootObjectMergeFunc, index+1)
		copy(newFuncs, doltgresRootObjectMergeFuncs)
		doltgresRootObjectMergeFuncs = newFuncs
	}
	if doltgresRootObjectMergeFuncs[index] != nil {
		panic("duplicate index on merge registration")
	}
	doltgresRootObjectMergeFuncs[index] = f
}

// ValidateDoltgresRootObjectRegistration ensures that all root objects have registered the required functions. Doltgres
// calls this function after registering all root objects.
func ValidateDoltgresRootObjectRegistration() error {
	if len(doltgresRootObjectSerializationFuncs) != len(doltgresRootObjectDeserializationFuncs) ||
		len(doltgresRootObjectSerializationFuncs) != len(doltgresRootObjectMergeFuncs) {
		return fmt.Errorf("serialization:   %d\ndeserialization: %d\nmerge:           %d",
			len(doltgresRootObjectSerializationFuncs), len(doltgresRootObjectDeserializationFuncs), len(doltgresRootObjectMergeFuncs))
	}
	for i := range doltgresRootObjectSerializationFuncs {
		if doltgresRootObjectSerializationFuncs[i] == nil {
			return fmt.Errorf("missing serialization function for index %d", i)
		}
		if doltgresRootObjectDeserializationFuncs[i] == nil {
			return fmt.Errorf("missing deserialization function for index %d", i)
		}
		if doltgresRootObjectMergeFuncs[i] == nil {
			return fmt.Errorf("missing merge function for index %d", i)
		}
	}
	return nil
}

// GetDoltgresRootObjectCount returns the number of registered root objects.
func GetDoltgresRootObjectCount() uint32 {
	return uint32(len(doltgresRootObjectSerializationFuncs))
}

// MergeDoltgresRootObjectRefs merges all DoltgresRootObjects that differ between ourRoot and theirRoot using the
// registered merge functions.
func MergeDoltgresRootObjectRefs(ctx context.Context, mergedRoot, ourRoot, theirRoot, ancRoot *RootValue) (*RootValue, error) {
	// Load all of the refs. They'll all have the same length
	ourRootObjRefs, err := ourRoot.GetDoltgresRootObjectRefs(ctx)
	if err != nil {
		return nil, err
	}
	theirRootObjRefs, err := theirRoot.GetDoltgresRootObjectRefs(ctx)
	if err != nil {
		return nil, err
	}
	ancRootObjRefs, err := ancRoot.GetDoltgresRootObjectRefs(ctx)
	if err != nil {
		return nil, err
	}

	mergedRootObjs := make([]DoltgresRootObject, len(ourRootObjRefs))
	for i := range ourRootObjRefs {
		ourRootObjRef, theirRootObjRef, ancRootObjRef := ourRootObjRefs[i], theirRootObjRefs[i], ancRootObjRefs[i]
		// If any of the objects have changes that were not previously taken care of, then we'll error
		if ourRootObjRef.loadedObj != nil {
			changed, _ := ourRootObjRef.loadedObj.Changed()
			if changed {
				return nil, fmt.Errorf("cannot merge DoltgresRootObjects that have pending changes")
			}
		}
		if theirRootObjRef.loadedObj != nil {
			changed, _ := theirRootObjRef.loadedObj.Changed()
			if changed {
				return nil, fmt.Errorf("cannot merge DoltgresRootObjects that have pending changes")
			}
		}
		if ancRootObjRef.loadedObj != nil {
			changed, _ := ancRootObjRef.loadedObj.Changed()
			if changed {
				return nil, fmt.Errorf("cannot merge DoltgresRootObjects that have pending changes")
			}
		}
		// If there are no changes between the two branches, then we have nothing to merge and we'll just take ours
		if ourRootObjRef.DataAddr.Equal(theirRootObjRef.DataAddr) {
			mergedRootObjs[i], err = ourRootObjRef.RootObject(ctx, ourRoot.vrw)
			if err != nil {
				return nil, err
			}
			continue
		}
		// There are changes, so we'll use the merge function.
		ourRootObj, err := ourRootObjRef.RootObject(ctx, ourRoot.vrw)
		if err != nil {
			return nil, err
		}
		theirRootObj, err := theirRootObjRef.RootObject(ctx, theirRoot.vrw)
		if err != nil {
			return nil, err
		}
		ancRootObj, err := ancRootObjRef.RootObject(ctx, ancRoot.vrw)
		if err != nil {
			return nil, err
		}
		mergedRootObjs[i], err = doltgresRootObjectMergeFuncs[ourRootObjRef.Index](ctx, ourRootObj, theirRootObj, ancRootObj)
		if err != nil {
			return nil, err
		}
	}

	return mergedRoot.PutDoltgresRootObjects(ctx, mergedRootObjs...)
}

// DoltgresRootObjectRef holds a reference to a stored DoltgresRootObject, along with a potentially cached load of the
// DoltgresRootObject. It is valid for this cached object to be modified, as the normal workflow will ensure that all
// changes are persisted on a new root, while allowing multiple users to access the same root object.
type DoltgresRootObjectRef struct {
	Index     DoltgresRootObjectIndex
	DataAddr  hash.Hash
	loadedObj DoltgresRootObject
}

// defaultDoltgresRootObjects returns a set of default root objects for all registered indexes.
func defaultDoltgresRootObjects() []DoltgresRootObjectRef {
	refs := make([]DoltgresRootObjectRef, GetDoltgresRootObjectCount())
	for i := range refs {
		refs[i].Index = DoltgresRootObjectIndex(i)
	}
	return refs
}

// RootObject either returns the cached root object, or loads the root object from the reader.
func (ref *DoltgresRootObjectRef) RootObject(ctx context.Context, vr types.ValueReader) (DoltgresRootObject, error) {
	if ref.loadedObj != nil {
		return ref.loadedObj, nil
	}
	if ref.DataAddr.IsEmpty() {
		var err error
		ref.loadedObj, err = doltgresRootObjectDeserializationFuncs[ref.Index](ctx, nil, ref.DataAddr)
		if err != nil {
			return nil, err
		}
		return ref.loadedObj, nil
	}
	dataValue, err := vr.ReadValue(ctx, ref.DataAddr)
	if err != nil {
		return nil, err
	}
	dataBlob := dataValue.(types.Blob)
	dataBlobLength := dataBlob.Len()
	data := make([]byte, dataBlobLength)
	n, err := dataBlob.ReadAt(context.Background(), data, 0)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if uint64(n) != dataBlobLength {
		return nil, fmt.Errorf("wanted %d bytes from blob for root object, got %d", dataBlobLength, n)
	}
	ref.loadedObj, err = doltgresRootObjectDeserializationFuncs[ref.Index](ctx, data, ref.DataAddr)
	if err != nil {
		return nil, err
	}
	return ref.loadedObj, nil
}

// UpdateDataAddress returns a new ref with an updated hash, while also containing the cached root object. This also
// purges the root object from this ref. The workflow ensures that the new ref is used when attempting to view any
// changes made to the cached root object.
func (ref *DoltgresRootObjectRef) UpdateDataAddress(ctx context.Context, vrw types.ValueReadWriter) (DoltgresRootObjectRef, error) {
	if ref.loadedObj == nil {
		return *ref, nil
	}
	changed, originalHash := ref.loadedObj.Changed()
	if !changed && ref.DataAddr.Equal(originalHash) {
		return *ref, nil
	}
	// Unload the object from this ref
	loadedObj := ref.loadedObj
	ref.loadedObj = nil

	data, err := doltgresRootObjectSerializationFuncs[ref.Index](ctx, loadedObj)
	if err != nil {
		return DoltgresRootObjectRef{}, err
	}
	if data == nil {
		return DoltgresRootObjectRef{
			Index:     ref.Index,
			DataAddr:  hash.Hash{},
			loadedObj: nil,
		}, nil
	}
	dataBlob, err := types.NewBlob(ctx, vrw, bytes.NewReader(data))
	if err != nil {
		return DoltgresRootObjectRef{}, err
	}
	hashRef, err := vrw.WriteValue(ctx, dataBlob)
	if err != nil {
		return DoltgresRootObjectRef{}, err
	}
	return DoltgresRootObjectRef{
		Index:     ref.Index,
		DataAddr:  hashRef.TargetHash(),
		loadedObj: loadedObj,
	}, nil
}
