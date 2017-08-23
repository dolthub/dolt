package bitswap

import (
	bsnet "github.com/ipfs/go-ipfs/exchange/bitswap/network"
	"github.com/ipfs/go-ipfs/thirdparty/testutil"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
)

type Network interface {
	Adapter(testutil.Identity) bsnet.BitSwapNetwork

	HasPeer(peer.ID) bool
}
