package bitswap

import (
	"sort"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
)

type Stat struct {
	ProvideBufLen   int
	Wantlist        []*cid.Cid
	Peers           []string
	BlocksReceived  uint64
	DataReceived    uint64
	BlocksSent      uint64
	DataSent        uint64
	DupBlksReceived uint64
	DupDataReceived uint64
}

func (bs *Bitswap) Stat() (*Stat, error) {
	st := new(Stat)
	st.ProvideBufLen = len(bs.newBlocks)
	st.Wantlist = bs.GetWantlist()
	bs.counterLk.Lock()
	c := bs.counters
	st.BlocksReceived = c.blocksRecvd
	st.DupBlksReceived = c.dupBlocksRecvd
	st.DupDataReceived = c.dupDataRecvd
	st.BlocksSent = c.blocksSent
	st.DataSent = c.dataSent
	st.DataReceived = c.dataRecvd
	bs.counterLk.Unlock()

	for _, p := range bs.engine.Peers() {
		st.Peers = append(st.Peers, p.Pretty())
	}
	sort.Strings(st.Peers)

	return st, nil
}
