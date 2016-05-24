// Package blake2 provides a Go wrapper around an optimized, public domain
// implementation of BLAKE2.
// The cryptographic hash function BLAKE2 is an improved version of the SHA-3
// finalist BLAKE. Like BLAKE or SHA-3, BLAKE2 offers the highest security, yet
// is fast as MD5 on 64-bit platforms and requires at least 33% less RAM than
// SHA-2 or SHA-3 on low-end systems.
package blake2

import (
	// #cgo CFLAGS: -O3
	// #include "blake2.h"
	"C"
	"hash"
	"unsafe"
)

type digest struct {
	state      C.blake2b_state
	key        []byte
	param      C.blake2b_param
	isLastNode bool
}

const (
	SaltSize     = C.BLAKE2B_SALTBYTES
	PersonalSize = C.BLAKE2B_PERSONALBYTES
)

// Tree contains parameters for tree hashing. Each node in the tree
// can be hashed concurrently, and incremental changes can be done in
// a Merkle tree fashion.
type Tree struct {
	// Fanout: how many children each tree node has. 0 for unlimited.
	// 1 means sequential mode.
	Fanout uint8
	// Maximal depth of the tree. Beyond this height, nodes are just
	// added to the root of the tree. 255 for unlimited. 1 means
	// sequential mode.
	MaxDepth uint8
	// Leaf maximal byte length, how much data each leaf summarizes. 0
	// for unlimited or sequential mode.
	LeafSize uint32
	// Depth of this node. 0 for leaves or sequential mode.
	NodeDepth uint8
	// Offset of this node within this level of the tree. 0 for the
	// first, leftmost, leaf, or sequential mode.
	NodeOffset uint64
	// Inner hash byte length, in the range [0, 64]. 0 for sequential
	// mode.
	InnerHashSize uint8

	// IsLastNode indicates this node is the last, rightmost, node of
	// a level of the tree.
	IsLastNode bool
}

// Config contains parameters for the hash function that affect its
// output.
type Config struct {
	// Digest byte length, in the range [1, 64]. If 0, default size of 64 bytes is used.
	Size uint8
	// Key is up to 64 arbitrary bytes, for keyed hashing mode. Can be nil.
	Key []byte
	// Salt is up to 16 arbitrary bytes, used to randomize the hash. Can be nil.
	Salt []byte
	// Personal is up to 16 arbitrary bytes, used to make the hash
	// function unique for each application. Can be nil.
	Personal []byte

	// Parameters for tree hashing. Set to nil to use default
	// sequential mode.
	Tree *Tree
}

// New returns a new custom BLAKE2b hash.
//
// If config is nil, uses a 64-byte digest size.
func New(config *Config) hash.Hash {
	d := &digest{
		param: C.blake2b_param{
			digest_length: 64,
			fanout:        1,
			depth:         1,
		},
	}
	if config != nil {
		if config.Size != 0 {
			d.param.digest_length = C.uint8_t(config.Size)
		}
		if len(config.Key) > 0 {
			// let the C library worry about the exact limit; we just
			// worry about fitting into the variable
			if len(config.Key) > 255 {
				panic("blake2b key too long")
			}
			d.param.key_length = C.uint8_t(len(config.Key))
			d.key = config.Key
		}
		salt := (*[C.BLAKE2B_SALTBYTES]byte)(unsafe.Pointer(&d.param.salt[0]))
		copy(salt[:], config.Salt)
		personal := (*[C.BLAKE2B_SALTBYTES]byte)(unsafe.Pointer(&d.param.personal[0]))
		copy(personal[:], config.Personal)

		if config.Tree != nil {
			d.param.fanout = C.uint8_t(config.Tree.Fanout)
			d.param.depth = C.uint8_t(config.Tree.MaxDepth)
			d.param.leaf_length = C.uint32_t(config.Tree.LeafSize)
			d.param.node_offset = C.uint64_t(config.Tree.NodeOffset)
			d.param.node_depth = C.uint8_t(config.Tree.NodeDepth)
			d.param.inner_length = C.uint8_t(config.Tree.InnerHashSize)

			d.isLastNode = config.Tree.IsLastNode
		}
	}
	d.Reset()
	return d
}

// NewBlake2B returns a new 512-bit BLAKE2B hash.
func NewBlake2B() hash.Hash {
	return New(&Config{Size: 64})
}

// NewKeyedBlake2B returns a new 512-bit BLAKE2B hash with the given secret key.
func NewKeyedBlake2B(key []byte) hash.Hash {
	return New(&Config{Size: 64, Key: key})
}

func (*digest) BlockSize() int {
	return 128
}

func (d *digest) Size() int {
	return int(d.param.digest_length)
}

func (d *digest) Reset() {
	var key unsafe.Pointer
	if d.param.key_length > 0 {
		key = unsafe.Pointer(&d.key[0])
	}
	if C.blake2b_init_parametrized(&d.state, &d.param, key) < 0 {
		panic("blake2: unable to reset")
	}
	if d.isLastNode {
		d.state.last_node = C.uint8_t(1)
	}
}

func (d *digest) Sum(buf []byte) []byte {
	digest := make([]byte, d.Size())
	// Make a copy of d.state so that caller can keep writing and summing.
	s := d.state
	C.blake2b_final(&s, (*C.uint8_t)(&digest[0]), C.uint8_t(d.Size()))
	return append(buf, digest...)
}

func (d *digest) Write(buf []byte) (int, error) {
	if len(buf) > 0 {
		C.blake2b_update(&d.state, (*C.uint8_t)(&buf[0]), C.uint64_t(len(buf)))
	}
	return len(buf), nil
}
