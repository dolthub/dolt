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

package datas

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	flatbuffers "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type StashList struct {
	am      prolly.AddressMap
	addr    hash.Hash
	lastIdx int
}

func (s *StashList) AddressMap() prolly.AddressMap {
	return s.am
}

func (s *StashList) Addr() hash.Hash {
	return s.addr
}

func (s *StashList) Count() (int, error) {
	return s.am.Count()
}

type stashHead struct {
	key  int
	addr hash.Hash
}

// AddStash returns hash address of updated stash list map after adding the new stash using given hash address of the new stash.
func (s *StashList) AddStash(ctx context.Context, vw types.ValueWriter, stashAddr hash.Hash) (hash.Hash, error) {
	stashID := strconv.Itoa(s.lastIdx + 1)

	ame := s.am.Editor()
	err := ame.Add(ctx, stashID, stashAddr)
	if err != nil {
		return hash.Hash{}, err
	}

	s.am, err = ame.Flush(ctx)
	if err != nil {
		return hash.Hash{}, err
	}
	return s.updateStashListMap(ctx, vw)
}

// RemoveStashAtIdx returns hash address of updated stash list map after removing the stash at given index of the stash list.
func (s *StashList) RemoveStashAtIdx(ctx context.Context, vw types.ValueWriter, idx int) (hash.Hash, error) {
	amCount, err := s.am.Count()
	if err != nil {
		return hash.Hash{}, err
	}
	if amCount <= idx {
		return hash.Hash{}, fmt.Errorf("fatal: log for 'stash' only has %v entries", amCount)
	}

	stash, err := getNthStash(ctx, s.am, amCount, idx)
	if err != nil {
		return hash.Hash{}, err
	}

	ame := s.am.Editor()
	err = ame.Delete(ctx, strconv.Itoa(stash.key))
	if err != nil {
		return hash.Hash{}, err
	}

	s.am, err = ame.Flush(ctx)
	if err != nil {
		return hash.Hash{}, err
	}
	return s.updateStashListMap(ctx, vw)
}

// getAllStashes returns array of stashHead object which contains the key and hash address for a stash stored in the stash list map.
// This function returns the array in the order of the latest to the oldest stash.
func (s *StashList) getAllStashes(ctx context.Context) ([]*stashHead, error) {
	amCount, err := s.am.Count()
	if err != nil {
		return nil, err
	}
	if amCount == 0 {
		return nil, nil
	}

	return getStashListOrdered(ctx, s.am, amCount), nil
}

// updateStashListMap returns address hash of updated stash list map.
func (s *StashList) updateStashListMap(ctx context.Context, vw types.ValueWriter) (hash.Hash, error) {
	// update stash map data and reset the stash map's hash
	data := stashlist_flatbuffer(s.am)
	r, err := vw.WriteValue(ctx, types.SerialMessage(data))
	if err != nil {
		return hash.Hash{}, err
	}
	s.addr = r.TargetHash()

	return s.addr, nil
}

// getStashAtIdx returns a stash object address hash at given index from the stash list.
func (s *StashList) getStashAtIdx(ctx context.Context, idx int) (hash.Hash, error) {
	amCount, err := s.am.Count()
	if err != nil {
		return hash.Hash{}, err
	}
	if amCount <= idx {
		return hash.Hash{}, fmt.Errorf("fatal: log for 'stash' only has %v entries", amCount)
	}

	stash, err := getNthStash(ctx, s.am, amCount, idx)
	if err != nil {
		return hash.Hash{}, err
	}

	return stash.addr, nil
}

// IsStashList determines whether the types.Value is a stash list object.
func IsStashList(v types.Value) (bool, error) {
	if _, ok := v.(types.Struct); ok {
		// this should not return true as stash is not supported for old format
		return false, nil
	} else if sm, ok := v.(types.SerialMessage); ok {
		return serial.GetFileID(sm) == serial.StashListFileID, nil
	} else {
		return false, nil
	}
}

// GetStashAtIdx returns hash address of stash at given index in the stash list.
func GetStashAtIdx(ctx context.Context, ns tree.NodeStore, val types.Value, idx int) (hash.Hash, error) {
	stashList, err := getExistingStashList(ctx, ns, val)
	if err != nil {
		return hash.Hash{}, err
	}

	return stashList.getStashAtIdx(ctx, idx)
}

// GetHashListFromStashList returns array of hash addresses of stashes from the stash list.
func GetHashListFromStashList(ctx context.Context, ns tree.NodeStore, val types.Value) ([]hash.Hash, error) {
	stashList, err := getExistingStashList(ctx, ns, val)
	if err != nil {
		return nil, err
	}

	stashes, err := stashList.getAllStashes(ctx)
	if err != nil {
		return nil, err
	}

	var stashHashList = make([]hash.Hash, len(stashes))

	for i, si := range stashes {
		stashHashList[i] = si.addr
	}

	return stashHashList, nil
}

// LoadStashList returns StashList object that contains the AddressMap that contains all stashes. This method creates
// new StashList address map, if there is none exists yet (dataset head is null). Otherwise, it returns the address map
// that corresponds to given root hash value.
func LoadStashList(ctx context.Context, nbf *types.NomsBinFormat, ns tree.NodeStore, vr types.ValueReader, ds Dataset) (*StashList, error) {
	if !nbf.UsesFlatbuffers() {
		return nil, errors.New("loadStashList: stash is not supported for old storage format")
	}

	rootHash, hasHead := ds.MaybeHeadAddr()
	if !hasHead {
		nam, err := prolly.NewEmptyAddressMap(ns)
		if err != nil {
			return nil, err
		}

		return &StashList{nam, nam.HashOf(), -1}, nil
	}

	val, err := vr.ReadValue(ctx, rootHash)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, errors.New("root hash doesn't exist")
	}

	return getExistingStashList(ctx, ns, val)
}

// getExistingStashList returns stash list expecting that a stash list exists at given nodeStore and value.
func getExistingStashList(ctx context.Context, ns tree.NodeStore, val types.Value) (*StashList, error) {
	am, err := parse_stashlist([]byte(val.(types.SerialMessage)), ns)
	if err != nil {
		return nil, err
	}

	amCount, err := am.Count()
	if err != nil {
		return nil, err
	}

	if amCount == 0 {
		return &StashList{am, am.Node().HashOf(), -1}, nil
	}

	// the latest entry will be the first element in the ordered list
	stashes := getStashListOrdered(ctx, am, amCount)
	lastIdx := stashes[0].key

	return &StashList{am, am.Node().HashOf(), lastIdx}, nil
}

// getStashListOrdered returns ordered stash list using given address map and number of elements in the map.
// The ordering is back iterated on the current map, which gives the last added stash as the first element in the list.
func getStashListOrdered(ctx context.Context, am prolly.AddressMap, count int) []*stashHead {
	var stashList = make([]*stashHead, count)
	// fill the array backwards
	var idx = count - 1
	_ = am.IterAll(ctx, func(key string, addr hash.Hash) error {
		j, err := strconv.Atoi(key)
		if err != nil {
			return err
		}
		stashList[idx] = &stashHead{j, addr}
		idx--
		return nil
	})

	return stashList
}

// getNthStash returns the stash at n-th position in the ordered stash list.
func getNthStash(ctx context.Context, am prolly.AddressMap, count, idx int) (*stashHead, error) {
	var stashList = getStashListOrdered(ctx, am, count)
	if count <= idx {
		return nil, fmt.Errorf("error: stash list only has %v entries", idx)
	}
	return stashList[idx], nil
}

func stashlist_flatbuffer(am prolly.AddressMap) serial.Message {
	builder := flatbuffers.NewBuilder(1024)
	ambytes := []byte(tree.ValueFromNode(am.Node()).(types.SerialMessage))
	voff := builder.CreateByteVector(ambytes)
	serial.StashListStart(builder)
	serial.StashListAddAddressMap(builder, voff)
	return serial.FinishMessage(builder, serial.StashListEnd(builder), []byte(serial.StashListFileID))
}

func parse_stashlist(bs []byte, ns tree.NodeStore) (prolly.AddressMap, error) {
	if serial.GetFileID(bs) != serial.StashListFileID {
		return prolly.AddressMap{}, fmt.Errorf("expected stash list file id, got: " + serial.GetFileID(bs))
	}
	sr, err := serial.TryGetRootAsStashList(bs, serial.MessagePrefixSz)
	if err != nil {
		return prolly.AddressMap{}, err
	}
	mapbytes := sr.AddressMapBytes()
	node, fileId, err := tree.NodeFromBytes(mapbytes)
	if err != nil {
		return prolly.AddressMap{}, err
	}
	if fileId != serial.AddressMapFileID {
		return prolly.AddressMap{}, fmt.Errorf("unexpected file ID, expected %s, got %s", serial.AddressMapFileID, fileId)
	}
	return prolly.NewAddressMap(node, ns)
}
