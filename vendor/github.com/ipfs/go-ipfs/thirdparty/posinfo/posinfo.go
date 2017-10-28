package posinfo

import (
	"os"

	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
)

type PosInfo struct {
	Offset   uint64
	FullPath string
	Stat     os.FileInfo // can be nil
}

type FilestoreNode struct {
	node.Node
	PosInfo *PosInfo
}
