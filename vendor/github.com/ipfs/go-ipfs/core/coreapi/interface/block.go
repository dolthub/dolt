package iface

import (
	"context"
	"io"

	options "github.com/ipfs/go-ipfs/core/coreapi/interface/options"
)

// BlockStat contains information about a block
type BlockStat interface {
	// Size is the size of a block
	Size() int

	// Path returns path to the block
	Path() ResolvedPath
}

// BlockAPI specifies the interface to the block layer
type BlockAPI interface {
	// Put imports raw block data, hashing it using specified settings.
	Put(context.Context, io.Reader, ...options.BlockPutOption) (BlockStat, error)

	// Get attempts to resolve the path and return a reader for data in the block
	Get(context.Context, Path) (io.Reader, error)

	// Rm removes the block specified by the path from local blockstore.
	// By default an error will be returned if the block can't be found locally.
	//
	// NOTE: If the specified block is pinned it won't be removed and no error
	// will be returned
	Rm(context.Context, Path, ...options.BlockRmOption) error

	// Stat returns information on
	Stat(context.Context, Path) (BlockStat, error)
}
