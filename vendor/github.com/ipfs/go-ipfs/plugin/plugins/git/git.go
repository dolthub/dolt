package git

import (
	"compress/zlib"
	"fmt"
	"io"
	"math"

	"github.com/ipfs/go-ipfs/core/coredag"
	"github.com/ipfs/go-ipfs/plugin"

	"gx/ipfs/QmTprEaAA2A9bst5XH7exuyi5KzNMK3SEDNN8rBDnKWcUS/go-cid"
	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
	"gx/ipfs/QmYNyRZJBUYPNrLszFmrBrPJbsBh2vMsefz5gnDpB5M1P6/go-ipld-format"
	git "gx/ipfs/Qma7Kuwun7w8SZphjEPDVxvGfetBkqdNGmigDA13sJdLex/go-ipld-git"
)

// Plugins is exported list of plugins that will be loaded
var Plugins = []plugin.Plugin{
	&gitPlugin{},
}

type gitPlugin struct{}

var _ plugin.PluginIPLD = (*gitPlugin)(nil)

func (*gitPlugin) Name() string {
	return "ipld-git"
}

func (*gitPlugin) Version() string {
	return "0.0.1"
}

func (*gitPlugin) Init() error {
	return nil
}

func (*gitPlugin) RegisterBlockDecoders(dec format.BlockDecoder) error {
	dec.Register(cid.GitRaw, git.DecodeBlock)
	return nil
}

func (*gitPlugin) RegisterInputEncParsers(iec coredag.InputEncParsers) error {
	iec.AddParser("raw", "git", parseRawGit)
	iec.AddParser("zlib", "git", parseZlibGit)
	return nil
}

func parseRawGit(r io.Reader, mhType uint64, mhLen int) ([]format.Node, error) {
	if mhType != math.MaxUint64 && mhType != mh.SHA1 {
		return nil, fmt.Errorf("unsupported mhType %d", mhType)
	}

	if mhLen != -1 && mhLen != mh.DefaultLengths[mh.SHA1] {
		return nil, fmt.Errorf("invalid mhLen %d", mhType)
	}

	nd, err := git.ParseObject(r)
	if err != nil {
		return nil, err
	}

	return []format.Node{nd}, nil
}

func parseZlibGit(r io.Reader, mhType uint64, mhLen int) ([]format.Node, error) {
	rc, err := zlib.NewReader(r)
	if err != nil {
		return nil, err
	}

	defer rc.Close()
	return parseRawGit(rc, mhType, mhLen)
}
