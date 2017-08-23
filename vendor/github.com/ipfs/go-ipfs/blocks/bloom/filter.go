// Package bloom implements a simple bloom filter.
package bloom

import (
	"encoding/binary"
	"errors"
	// Non crypto hash, because speed
	"gx/ipfs/QmeWQMDa5dSdP4n8WDeoY5z8L2EKVqF4ZvK4VEHsLqXsGu/hamming"
	"hash"

	"github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/mtchavez/jenkins"
)

// A Filter represents a bloom filter.
type Filter interface {
	Add([]byte)
	Find([]byte) bool
	Merge(Filter) (Filter, error)
	HammingDistance(Filter) (int, error)
}

// NewFilter creates a new bloom Filter with the given
// size. k (the number of hash functions), is hardcoded to 3.
func NewFilter(size int) Filter {
	return &filter{
		hash:   jenkins.New(),
		filter: make([]byte, size),
		k:      3,
	}
}

type filter struct {
	filter []byte
	hash   hash.Hash32
	k      int
}

// BasicFilter calls NewFilter with a bloom filter size of
// 2048 bytes.
func BasicFilter() Filter {
	return NewFilter(2048)
}

func (f *filter) Add(bytes []byte) {
	for _, bit := range f.getBitIndicies(bytes) {
		f.setBit(bit)
	}
}

func (f *filter) getBitIndicies(bytes []byte) []uint32 {
	indicies := make([]uint32, f.k)

	f.hash.Write(bytes)
	b := make([]byte, 4)

	for i := 0; i < f.k; i++ {
		res := f.hash.Sum32()
		indicies[i] = res % (uint32(len(f.filter)) * 8)

		binary.LittleEndian.PutUint32(b, res)
		f.hash.Write(b)
	}

	f.hash.Reset()

	return indicies
}

func (f *filter) Find(bytes []byte) bool {
	for _, bit := range f.getBitIndicies(bytes) {
		if !f.getBit(bit) {
			return false
		}
	}
	return true
}

func (f *filter) setBit(i uint32) {
	f.filter[i/8] |= (1 << byte(i%8))
}

func (f *filter) getBit(i uint32) bool {
	return f.filter[i/8]&(1<<byte(i%8)) != 0
}

func (f *filter) Merge(o Filter) (Filter, error) {
	casfil, ok := o.(*filter)
	if !ok {
		return nil, errors.New("Unsupported filter type")
	}

	if len(casfil.filter) != len(f.filter) {
		return nil, errors.New("filter lengths must match")
	}

	if casfil.k != f.k {
		return nil, errors.New("filter k-values must match")
	}

	nfilt := new(filter)
	nfilt.hash = f.hash
	nfilt.filter = make([]byte, len(f.filter))
	nfilt.k = f.k

	for i, v := range f.filter {
		nfilt.filter[i] = v | casfil.filter[i]
	}

	return nfilt, nil
}

func (f *filter) HammingDistance(o Filter) (int, error) {
	casfil, ok := o.(*filter)
	if !ok {
		return 0, errors.New("Unsupported filter type")
	}

	if len(f.filter) != len(casfil.filter) {
		return 0, errors.New("filter lengths must match")
	}

	acc := 0

	// xor together
	for i := 0; i < len(f.filter); i++ {
		acc += hamming.Byte(f.filter[i], casfil.filter[i])
	}

	return acc, nil
}
