package options

import (
	"math"

	cid "gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
)

type DagPutSettings struct {
	InputEnc string
	Codec    uint64
	MhType   uint64
	MhLength int
}

type DagTreeSettings struct {
	Depth int
}

type DagPutOption func(*DagPutSettings) error
type DagTreeOption func(*DagTreeSettings) error

func DagPutOptions(opts ...DagPutOption) (*DagPutSettings, error) {
	options := &DagPutSettings{
		InputEnc: "json",
		Codec:    cid.DagCBOR,
		MhType:   math.MaxUint64,
		MhLength: -1,
	}

	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return nil, err
		}
	}
	return options, nil
}

func DagTreeOptions(opts ...DagTreeOption) (*DagTreeSettings, error) {
	options := &DagTreeSettings{
		Depth: -1,
	}

	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return nil, err
		}
	}
	return options, nil
}

type dagOpts struct{}

var Dag dagOpts

// InputEnc is an option for Dag.Put which specifies the input encoding of the
// data. Default is "json", most formats/codecs support "raw"
func (dagOpts) InputEnc(enc string) DagPutOption {
	return func(settings *DagPutSettings) error {
		settings.InputEnc = enc
		return nil
	}
}

// Codec is an option for Dag.Put which specifies the multicodec to use to
// serialize the object. Default is cid.DagCBOR (0x71)
func (dagOpts) Codec(codec uint64) DagPutOption {
	return func(settings *DagPutSettings) error {
		settings.Codec = codec
		return nil
	}
}

// Hash is an option for Dag.Put which specifies the multihash settings to use
// when hashing the object. Default is based on the codec used
// (mh.SHA2_256 (0x12) for DagCBOR). If mhLen is set to -1, default length for
// the hash will be used
func (dagOpts) Hash(mhType uint64, mhLen int) DagPutOption {
	return func(settings *DagPutSettings) error {
		settings.MhType = mhType
		settings.MhLength = mhLen
		return nil
	}
}

// Depth is an option for Dag.Tree which specifies maximum depth of the
// returned tree. Default is -1 (no depth limit)
func (dagOpts) Depth(depth int) DagTreeOption {
	return func(settings *DagTreeSettings) error {
		settings.Depth = depth
		return nil
	}
}
