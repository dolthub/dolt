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

func (s *StashList) CurIdx() int {
	return s.lastIdx
}

func (s *StashList) Addr() hash.Hash {
	return s.addr
}

type stashHead struct {
	key  int
	addr hash.Hash
}

// TODO: idx could also be string?
func (s *StashList) addStash(ctx context.Context, db *database, stashAddr hash.Hash) (hash.Hash, error) {
	stashID := strconv.Itoa(s.lastIdx + 1)

	// update/add stashID and stash to the stash map
	ae := s.am.Editor()
	err := ae.Update(ctx, stashID, stashAddr)
	if err != nil {
		return hash.Hash{}, err
	}
	s.am, err = ae.Flush(ctx)
	if err != nil {
		return hash.Hash{}, err
	}

	// update stash map data and reset the stash map's hash
	data := stashlist_flatbuffer(s.am)
	r, err := db.WriteValue(ctx, types.SerialMessage(data))
	if err != nil {
		return hash.Hash{}, err
	}
	s.addr = r.TargetHash()

	return s.addr, nil
}

// this should not happen by itself, it should happen always after getStash is called and merge is successful
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

	// update stash map data and reset the stash map's hash
	data := stashlist_flatbuffer(s.am)
	r, err := db.WriteValue(ctx, types.SerialMessage(data))
	if err != nil {
		return hash.Hash{}, err
	}
	s.addr = r.TargetHash()

	return s.addr, nil
}

// this should not happen by itself, it should happen always after getStash is successful
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

func getExistingStashList(ctx context.Context, ns tree.NodeStore, val types.Value) (*StashList, error) {
	am, err := parse_stashlist([]byte(val.(types.SerialMessage)), ns)
	if err != nil {
		return nil, err
	}

	amCount, err := am.Count()
	if err != nil {
		return nil, err
	}

	// get the last stash
	stash, err := getNthStash(ctx, am, amCount, amCount-1)
	if err != nil {
		return nil, err
	}

	return &StashList{am, am.Node().HashOf(), stash.key}, nil
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

// getStashListOrdered returns ordered stash list using given address map and number of elements in the map.
// The ordering is back iterated on the current map, which gives the last added stash as the first element in the list.
func getStashListOrdered(ctx context.Context, am prolly.AddressMap, count int) []*stashHead {
	var stashList = make([]*stashHead, count)
	var i = 0
	// TODO CHECK: is this ordered?
	_ = am.IterAll(ctx, func(key string, addr hash.Hash) error {
		j, err := strconv.Atoi(key)
		if err != nil {
			return err
		}
		stashList[i] = &stashHead{j, addr}
		i++
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

func GetStashAtIdx(ctx context.Context, ns tree.NodeStore, val types.Value, idx int) (hash.Hash, error) {
	stashList, err := getExistingStashList(ctx, ns, val)
	if err != nil {
		return hash.Hash{}, err
	}

	return stashList.getStashAtIdx(ctx, idx)
}

func GetHashListFromStashList(ctx context.Context, ns tree.NodeStore, val types.Value) ([]hash.Hash, error) {
	stashList, err := getExistingStashList(ctx, ns, val)
	if err != nil {
		return nil, err
	}

	stashes, err := stashList.getAllStashes(ctx)
	if err != nil {
		return nil, err
	}

	//// order it highest to lowest (newest to oldest)
	//sort.Slice(stashes, func(i, j int) bool {
	//	return stashes[i].key > stashes[j].key
	//})

	var stashHashList = make([]hash.Hash, len(stashes))

	for i, si := range stashes {
		stashHashList[i] = si.addr
	}

	return stashHashList, nil
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
