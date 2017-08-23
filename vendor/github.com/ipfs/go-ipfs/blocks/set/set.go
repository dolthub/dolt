// Package set defines the BlockSet interface which provides
// abstraction for sets of Cids.
// It provides a default implementation using cid.Set.
package set

import (
	cid "gx/ipfs/QmTprEaAA2A9bst5XH7exuyi5KzNMK3SEDNN8rBDnKWcUS/go-cid"

	"github.com/ipfs/go-ipfs/blocks/bloom"
)

// BlockSet represents a mutable set of blocks CIDs.
type BlockSet interface {
	AddBlock(*cid.Cid)
	RemoveBlock(*cid.Cid)
	HasKey(*cid.Cid) bool
	// GetBloomFilter creates and returns a bloom filter to which
	// all the CIDs in the set have been added.
	GetBloomFilter() bloom.Filter
	GetKeys() []*cid.Cid
}

// SimpleSetFromKeys returns a default implementation of BlockSet
// using cid.Set. The given keys are added to the set.
func SimpleSetFromKeys(keys []*cid.Cid) BlockSet {
	sbs := &simpleBlockSet{blocks: cid.NewSet()}
	for _, k := range keys {
		sbs.AddBlock(k)
	}
	return sbs
}

// NewSimpleBlockSet returns a new empty default implementation
// of BlockSet using cid.Set.
func NewSimpleBlockSet() BlockSet {
	return &simpleBlockSet{blocks: cid.NewSet()}
}

type simpleBlockSet struct {
	blocks *cid.Set
}

func (b *simpleBlockSet) AddBlock(k *cid.Cid) {
	b.blocks.Add(k)
}

func (b *simpleBlockSet) RemoveBlock(k *cid.Cid) {
	b.blocks.Remove(k)
}

func (b *simpleBlockSet) HasKey(k *cid.Cid) bool {
	return b.blocks.Has(k)
}

func (b *simpleBlockSet) GetBloomFilter() bloom.Filter {
	f := bloom.BasicFilter()
	for _, k := range b.blocks.Keys() {
		f.Add(k.Bytes())
	}
	return f
}

func (b *simpleBlockSet) GetKeys() []*cid.Cid {
	return b.blocks.Keys()
}
