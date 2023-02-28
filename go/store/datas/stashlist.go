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

	flatbuffers "github.com/google/flatbuffers/go"

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

type stashHead struct {
	key  int
	addr hash.Hash
}

// addStash returns hash address of updated stash list map after adding the new stash using given hash address of the new stash.
func (s *StashList) addStash(ctx context.Context, db *database, stashAddr hash.Hash) (hash.Hash, error) {
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
	return s.updateStashListMap(ctx, db)
}

// removeStashAtIdx returns hash address of updated stash list map after removing the stash at given index of the stash list.
func (s *StashList) removeStashAtIdx(ctx context.Context, db *database, idx int) (hash.Hash, error) {
	amCount, err := s.am.Count()
	if err != nil {
		return hash.Hash{}, err
	}
	if amCount <= idx {
		return hash.Hash{}, errors.New(fmt.Sprintf("fatal: log for 'stash' only has %v entries", amCount))
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
	return s.updateStashListMap(ctx, db)
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

func (s *StashList) clearAllStashes(ctx context.Context, db *database) (hash.Hash, error) {
	amCount, err := s.am.Count()
	if err != nil {
		return hash.Hash{}, err
	}
	if amCount == 0 {
		return s.addr, nil
	}

	ame := s.am.Editor()
	err = s.am.IterAll(ctx, func(key string, addr hash.Hash) error {
		return ame.Delete(ctx, key)
	})
	if err != nil {
		return hash.Hash{}, err
	}

	s.am, err = ame.Flush(ctx)
	if err != nil {
		return hash.Hash{}, err
	}
	return s.updateStashListMap(ctx, db)
}

func (s *StashList) updateStashListMap(ctx context.Context, db *database) (hash.Hash, error) {
	// update stash map data and reset the stash map's hash
	data := stashlist_flatbuffer(s.am)
	r, err := db.WriteValue(ctx, types.SerialMessage(data))
	if err != nil {
		return hash.Hash{}, err
	}
	s.addr = r.TargetHash()

	return s.addr, nil
}

func (s *StashList) getStashAtIdx(ctx context.Context, idx int) (hash.Hash, error) {
	amCount, err := s.am.Count()
	if err != nil {
		return hash.Hash{}, err
	}
	if amCount <= idx {
		return hash.Hash{}, errors.New(fmt.Sprintf("fatal: log for 'stash' only has %v entries", amCount))
	}

	stash, err := getNthStash(ctx, s.am, amCount, idx)
	if err != nil {
		return hash.Hash{}, err
	}

	return stash.addr, nil
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

// loadStashList returns StashList object that contains the AddressMap that contains all stashes. This method creates
// new StashList address map, if there is none exists yet (rootHash is null). Otherwise, it returns the address map
// that corresponds to given root hash value.
func loadStashList(ctx context.Context, db *database, rootHash hash.Hash) (*StashList, error) {
	if !db.Format().UsesFlatbuffers() {
		return nil, errors.New("newStash: stash is not supported for old storage format")
	}

	if rootHash == (hash.Hash{}) {
		nam, err := prolly.NewEmptyAddressMap(db.ns)
		if err != nil {
			return nil, err
		}

		return &StashList{nam, nam.HashOf(), 0}, nil
	}

	val, err := db.ReadValue(ctx, rootHash)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, errors.New("root hash doesn't exist")
	}

	return getExistingStashList(ctx, db.nodeStore(), val)
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

// pushStash takes root hash to the stash list and components needed to create new stash to store in the stash list
// address map. This method uses loadStashList function that either creates the stash list if it does not exist already
// or loads the existing stash list. This function returns the updated root hash value to the stash list address map.
func pushStash(ctx context.Context, db *database, rootHash hash.Hash, stashRootRef types.Ref, headCommitAddr hash.Hash, meta *StashMeta) (hash.Hash, error) {
	// create a new stash
	stashAddr, _, err := newStashForValue(ctx, db, stashRootRef, headCommitAddr, meta)
	if err != nil {
		return hash.Hash{}, err
	}

	// this either creates new map or loads current map
	// rootHash is the head Addr of stashesRef stored in datasets
	// reusing storeRoot implementation to get address map interface to use for stash
	stashMap, err := loadStashList(ctx, db, rootHash)
	if err != nil {
		return hash.Hash{}, err
	}

	return stashMap.addStash(ctx, db, stashAddr)
}

func removeStashAtIdx(ctx context.Context, db *database, ns tree.NodeStore, val types.Value, idx int) (hash.Hash, error) {
	stashList, err := getExistingStashList(ctx, ns, val)
	if err != nil {
		return hash.Hash{}, err
	}

	return stashList.removeStashAtIdx(ctx, db, idx)
}

func clearAllStashes(ctx context.Context, db *database, ns tree.NodeStore, val types.Value) (hash.Hash, error) {
	stashList, err := getExistingStashList(ctx, ns, val)
	if err != nil {
		return hash.Hash{}, err
	}

	return stashList.clearAllStashes(ctx, db)
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
	for j, addr := range stashList {
		if j == idx {
			return addr, nil
		}
	}

	return nil, errors.New(fmt.Sprintf("could not find the stash element at postion %v", idx))
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
		panic("expected stash list file id, got: " + serial.GetFileID(bs))
	}
	sr, err := serial.TryGetRootAsStashList(bs, serial.MessagePrefixSz)
	if err != nil {
		return prolly.AddressMap{}, err
	}
	mapbytes := sr.AddressMapBytes()
	node, err := tree.NodeFromBytes(mapbytes)
	if err != nil {
		return prolly.AddressMap{}, err
	}
	return prolly.NewAddressMap(node, ns)
}
