// Copyright 2021 Dolthub, Inc.
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

package message

import (
	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/val"

	trie "github.com/nicktobey/go-succinct-data-structure-trie"
)

// ItemAccess accesses items in a serial.Message.
type ItemAccess struct {
	// bufStart is the offset to the start of the
	// Item buffer within a serial.Message.
	// bufLen is the length of the Item buffer.
	bufStart, bufLen uint16

	// offStart, if nonzero, is the offset to the
	// start of the uin16 offset buffer within a
	// serial.Message. A zero value for offStart
	// indicates an empty offset buffer.
	// bufLen is the length of the Item buffer.
	offStart, offLen uint16

	// If the serial.Message does not contain an
	// offset buffer (offStart is zero), then
	// Items have a fixed width equal to itemWidth.
	itemWidth uint16
	IsTrie    bool
}

func GetTrieMap(acc ItemAccess, msg serial.Message) *trie.FrozenTrieMap {
	nodeCount := uint16(msg[acc.offStart+2]) + uint16(msg[acc.offStart+3])<<8
	keysDataLength := (nodeCount + 7) / 8
	keysDataStart := acc.offStart + 4
	keysDataEnd := keysDataStart + uint16(keysDataLength)
	keysData := msg[keysDataStart:keysDataEnd]
	keysDirectoryStart := keysDataEnd
	keysDirectoryEnd := acc.offStart + acc.offLen
	keysDirectory := msg[keysDirectoryStart:keysDirectoryEnd]

	keys := trie.RankDirectory{}
	keys.Init(
		string(keysDirectory),
		string(keysData),
		uint(nodeCount), trie.L1, trie.L2)
	prefixTreeDataBits := uint16(nodeCount)*11 + 1
	prefixTreeDataBytes := (prefixTreeDataBits + 7) / 8
	prefixTreeDataStart := acc.bufStart
	prefixTreeDataEnd := prefixTreeDataStart + prefixTreeDataBytes
	prefixTreeData := msg[prefixTreeDataStart:prefixTreeDataEnd]

	prefixTreeDirectoryStart := prefixTreeDataEnd
	prefixTreeDirectoryEnd := acc.bufStart + acc.bufLen

	prefixTreeDirectory := msg[prefixTreeDirectoryStart:prefixTreeDirectoryEnd]

	ft := trie.FrozenTrie{}
	ft.Init(
		string(prefixTreeData),
		string(prefixTreeDirectory),
		uint(nodeCount))
	itemTrie := trie.FrozenTrieMap{}
	itemTrie.Init(ft, keys)

	return &itemTrie
}

// GetItem returns the ith Item from the buffer.
func (acc ItemAccess) GetItem(i int, msg serial.Message) []byte {
	buf := msg[acc.bufStart : acc.bufStart+acc.bufLen]
	off := msg[acc.offStart : acc.offStart+acc.offLen]

	if acc.IsTrie {
		itemTrie := GetTrieMap(acc, msg)
		keyString := itemTrie.ReverseLookup(uint(i + 1))
		result := []byte(keyString)
		return result
	}
	if acc.offStart != 0 {
		stop := val.ReadUint16(off[(i*2)+2 : (i*2)+4])
		start := val.ReadUint16(off[(i * 2) : (i*2)+2])
		return buf[start:stop]
	} else {
		stop := int(acc.itemWidth) * (i + 1)
		start := int(acc.itemWidth) * i
		return buf[start:stop]
	}
}
