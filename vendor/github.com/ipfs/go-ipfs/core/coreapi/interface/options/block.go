package options

import (
	"fmt"
	cid "gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	mh "gx/ipfs/QmPnFwZ2JXKnXgMw8CdBPxn7FWh6LLdjUjxV1fKHuJnkr8/go-multihash"
)

type BlockPutSettings struct {
	Codec    string
	MhType   uint64
	MhLength int
}

type BlockRmSettings struct {
	Force bool
}

type BlockPutOption func(*BlockPutSettings) error
type BlockRmOption func(*BlockRmSettings) error

func BlockPutOptions(opts ...BlockPutOption) (*BlockPutSettings, cid.Prefix, error) {
	options := &BlockPutSettings{
		Codec:    "",
		MhType:   mh.SHA2_256,
		MhLength: -1,
	}

	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return nil, cid.Prefix{}, err
		}
	}

	var pref cid.Prefix
	pref.Version = 1

	if options.Codec == "" {
		if options.MhType != mh.SHA2_256 || (options.MhLength != -1 && options.MhLength != 32) {
			options.Codec = "protobuf"
		} else {
			options.Codec = "v0"
		}
	}

	if options.Codec == "v0" && options.MhType == mh.SHA2_256 {
		pref.Version = 0
	}

	formatval, ok := cid.Codecs[options.Codec]
	if !ok {
		return nil, cid.Prefix{}, fmt.Errorf("unrecognized format: %s", options.Codec)
	}

	if options.Codec == "v0" {
		if options.MhType != mh.SHA2_256 || (options.MhLength != -1 && options.MhLength != 32) {
			return nil, cid.Prefix{}, fmt.Errorf("only sha2-255-32 is allowed with CIDv0")
		}
	}

	pref.Codec = formatval

	pref.MhType = options.MhType
	pref.MhLength = options.MhLength

	return options, pref, nil
}

func BlockRmOptions(opts ...BlockRmOption) (*BlockRmSettings, error) {
	options := &BlockRmSettings{
		Force: false,
	}

	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return nil, err
		}
	}
	return options, nil
}

type blockOpts struct{}

var Block blockOpts

// Format is an option for Block.Put which specifies the multicodec to use to
// serialize the object. Default is "v0"
func (blockOpts) Format(codec string) BlockPutOption {
	return func(settings *BlockPutSettings) error {
		settings.Codec = codec
		return nil
	}
}

// Hash is an option for Block.Put which specifies the multihash settings to use
// when hashing the object. Default is mh.SHA2_256 (0x12).
// If mhLen is set to -1, default length for the hash will be used
func (blockOpts) Hash(mhType uint64, mhLen int) BlockPutOption {
	return func(settings *BlockPutSettings) error {
		settings.MhType = mhType
		settings.MhLength = mhLen
		return nil
	}
}

// Force is an option for Block.Rm which, when set to true, will ignore
// non-existing blocks
func (blockOpts) Force(force bool) BlockRmOption {
	return func(settings *BlockRmSettings) error {
		settings.Force = force
		return nil
	}
}
