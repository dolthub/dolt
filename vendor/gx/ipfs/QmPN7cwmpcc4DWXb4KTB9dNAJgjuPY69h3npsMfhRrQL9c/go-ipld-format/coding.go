package format

import (
	"fmt"
	"sync"

	blocks "gx/ipfs/QmSn9Td7xgxm9EV7iEjTckpUWmWApggzPxu7eFGWkkpwin/go-block-format"
)

// DecodeBlockFunc functions decode blocks into nodes.
type DecodeBlockFunc func(block blocks.Block) (Node, error)

type BlockDecoder interface {
	Register(codec uint64, decoder DecodeBlockFunc)
	Decode(blocks.Block) (Node, error)
}
type safeBlockDecoder struct {
	// Can be replaced with an RCU if necessary.
	lock     sync.RWMutex
	decoders map[uint64]DecodeBlockFunc
}

// Register registers decoder for all blocks with the passed codec.
//
// This will silently replace any existing registered block decoders.
func (d *safeBlockDecoder) Register(codec uint64, decoder DecodeBlockFunc) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.decoders[codec] = decoder
}

func (d *safeBlockDecoder) Decode(block blocks.Block) (Node, error) {
	// Short-circuit by cast if we already have a Node.
	if node, ok := block.(Node); ok {
		return node, nil
	}

	ty := block.Cid().Type()

	d.lock.RLock()
	decoder, ok := d.decoders[ty]
	d.lock.RUnlock()

	if ok {
		return decoder(block)
	} else {
		// TODO: get the *long* name for this format
		return nil, fmt.Errorf("unrecognized object type: %d", ty)
	}
}

var DefaultBlockDecoder BlockDecoder = &safeBlockDecoder{decoders: make(map[uint64]DecodeBlockFunc)}

// Decode decodes the given block using the default BlockDecoder.
func Decode(block blocks.Block) (Node, error) {
	return DefaultBlockDecoder.Decode(block)
}

// Register registers block decoders with the default BlockDecoder.
func Register(codec uint64, decoder DecodeBlockFunc) {
	DefaultBlockDecoder.Register(codec, decoder)
}
