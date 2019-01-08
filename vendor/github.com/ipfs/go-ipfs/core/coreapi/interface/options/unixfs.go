package options

import (
	"errors"
	"fmt"

	cid "gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	mh "gx/ipfs/QmPnFwZ2JXKnXgMw8CdBPxn7FWh6LLdjUjxV1fKHuJnkr8/go-multihash"
	dag "gx/ipfs/QmSei8kFMfqdJq7Q68d2LMnHbTWKKg2daA29ezUYFAUNgc/go-merkledag"
)

type Layout int

const (
	BalancedLayout Layout = iota
	TrickleLayout
)

type UnixfsAddSettings struct {
	CidVersion int
	MhType     uint64

	Inline       bool
	InlineLimit  int
	RawLeaves    bool
	RawLeavesSet bool

	Chunker string
	Layout  Layout

	Pin      bool
	OnlyHash bool
	Local    bool
	FsCache  bool
	NoCopy   bool

	Wrap      bool
	Hidden    bool
	StdinName string

	Events   chan<- interface{}
	Silent   bool
	Progress bool
}

type UnixfsAddOption func(*UnixfsAddSettings) error

func UnixfsAddOptions(opts ...UnixfsAddOption) (*UnixfsAddSettings, cid.Prefix, error) {
	options := &UnixfsAddSettings{
		CidVersion: -1,
		MhType:     mh.SHA2_256,

		Inline:       false,
		InlineLimit:  32,
		RawLeaves:    false,
		RawLeavesSet: false,

		Chunker: "size-262144",
		Layout:  BalancedLayout,

		Pin:      false,
		OnlyHash: false,
		Local:    false,
		FsCache:  false,
		NoCopy:   false,

		Wrap:      false,
		Hidden:    false,
		StdinName: "",

		Events:   nil,
		Silent:   false,
		Progress: false,
	}

	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return nil, cid.Prefix{}, err
		}
	}

	// nocopy -> rawblocks
	if options.NoCopy && !options.RawLeaves {
		// fixed?
		if options.RawLeavesSet {
			return nil, cid.Prefix{}, fmt.Errorf("nocopy option requires '--raw-leaves' to be enabled as well")
		}

		// No, satisfy mandatory constraint.
		options.RawLeaves = true
	}

	// (hash != "sha2-256") -> CIDv1
	if options.MhType != mh.SHA2_256 {
		switch options.CidVersion {
		case 0:
			return nil, cid.Prefix{}, errors.New("CIDv0 only supports sha2-256")
		case 1, -1:
			options.CidVersion = 1
		default:
			return nil, cid.Prefix{}, fmt.Errorf("unknown CID version: %d", options.CidVersion)
		}
	} else {
		if options.CidVersion < 0 {
			// Default to CIDv0
			options.CidVersion = 0
		}
	}

	// cidV1 -> raw blocks (by default)
	if options.CidVersion > 0 && !options.RawLeavesSet {
		options.RawLeaves = true
	}

	prefix, err := dag.PrefixForCidVersion(options.CidVersion)
	if err != nil {
		return nil, cid.Prefix{}, err
	}

	prefix.MhType = options.MhType
	prefix.MhLength = -1

	return options, prefix, nil
}

type unixfsOpts struct{}

var Unixfs unixfsOpts

// CidVersion specifies which CID version to use. Defaults to 0 unless an option
// that depends on CIDv1 is passed.
func (unixfsOpts) CidVersion(version int) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.CidVersion = version
		return nil
	}
}

// Hash function to use. Implies CIDv1 if not set to sha2-256 (default).
//
// Table of functions is declared in https://github.com/multiformats/go-multihash/blob/master/multihash.go
func (unixfsOpts) Hash(mhtype uint64) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.MhType = mhtype
		return nil
	}
}

// RawLeaves specifies whether to use raw blocks for leaves (data nodes with no
// links) instead of wrapping them with unixfs structures.
func (unixfsOpts) RawLeaves(enable bool) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.RawLeaves = enable
		settings.RawLeavesSet = true
		return nil
	}
}

// Inline tells the adder to inline small blocks into CIDs
func (unixfsOpts) Inline(enable bool) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.Inline = enable
		return nil
	}
}

// InlineLimit sets the amount of bytes below which blocks will be encoded
// directly into CID instead of being stored and addressed by it's hash.
// Specifying this option won't enable block inlining. For that use `Inline`
// option. Default: 32 bytes
//
// Note that while there is no hard limit on the number of bytes, it should be
// kept at a reasonably low value, such as 64; implementations may choose to
// reject anything larger.
func (unixfsOpts) InlineLimit(limit int) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.InlineLimit = limit
		return nil
	}
}

// Chunker specifies settings for the chunking algorithm to use.
//
// Default: size-262144, formats:
// size-[bytes] - Simple chunker splitting data into blocks of n bytes
// rabin-[min]-[avg]-[max] - Rabin chunker
func (unixfsOpts) Chunker(chunker string) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.Chunker = chunker
		return nil
	}
}

// Layout tells the adder how to balance data between leaves.
// options.BalancedLayout is the default, it's optimized for static seekable
// files.
// options.TrickleLayout is optimized for streaming data,
func (unixfsOpts) Layout(layout Layout) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.Layout = layout
		return nil
	}
}

// Pin tells the adder to pin the file root recursively after adding
func (unixfsOpts) Pin(pin bool) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.Pin = pin
		return nil
	}
}

// HashOnly will make the adder calculate data hash without storing it in the
// blockstore or announcing it to the network
func (unixfsOpts) HashOnly(hashOnly bool) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.OnlyHash = hashOnly
		return nil
	}
}

// Local will add the data to blockstore without announcing it to the network
//
// Note that this doesn't prevent other nodes from getting this data
func (unixfsOpts) Local(local bool) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.Local = local
		return nil
	}
}

// Wrap tells the adder to wrap the added file structure with an additional
// directory.
func (unixfsOpts) Wrap(wrap bool) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.Wrap = wrap
		return nil
	}
}

// Hidden enables adding of hidden files (files prefixed with '.')
func (unixfsOpts) Hidden(hidden bool) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.Hidden = hidden
		return nil
	}
}

// StdinName is the name set for files which don specify FilePath as
// os.Stdin.Name()
func (unixfsOpts) StdinName(name string) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.StdinName = name
		return nil
	}
}

// Events specifies channel which will be used to report events about ongoing
// Add operation.
//
// Note that if this channel blocks it may slowdown the adder
func (unixfsOpts) Events(sink chan<- interface{}) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.Events = sink
		return nil
	}
}

// Silent reduces event output
func (unixfsOpts) Silent(silent bool) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.Silent = silent
		return nil
	}
}

// Progress tells the adder whether to enable progress events
func (unixfsOpts) Progress(enable bool) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.Progress = enable
		return nil
	}
}

// FsCache tells the adder to check the filestore for pre-existing blocks
//
// Experimental
func (unixfsOpts) FsCache(enable bool) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.FsCache = enable
		return nil
	}
}

// NoCopy tells the adder to add the files using filestore. Implies RawLeaves.
//
// Experimental
func (unixfsOpts) Nocopy(enable bool) UnixfsAddOption {
	return func(settings *UnixfsAddSettings) error {
		settings.NoCopy = enable
		return nil
	}
}
