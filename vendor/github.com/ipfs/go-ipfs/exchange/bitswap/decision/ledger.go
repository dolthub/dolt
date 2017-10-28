package decision

import (
	"sync"
	"time"

	wl "github.com/ipfs/go-ipfs/exchange/bitswap/wantlist"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
)

func newLedger(p peer.ID) *ledger {
	return &ledger{
		wantList:   wl.New(),
		Partner:    p,
		sentToPeer: make(map[string]time.Time),
	}
}

// ledger stores the data exchange relationship between two peers.
// NOT threadsafe
type ledger struct {
	// Partner is the remote Peer.
	Partner peer.ID

	// Accounting tracks bytes sent and recieved.
	Accounting debtRatio

	// lastExchange is the time of the last data exchange.
	lastExchange time.Time

	// exchangeCount is the number of exchanges with this peer
	exchangeCount uint64

	// wantList is a (bounded, small) set of keys that Partner desires.
	wantList *wl.Wantlist

	// sentToPeer is a set of keys to ensure we dont send duplicate blocks
	// to a given peer
	sentToPeer map[string]time.Time

	// ref is the reference count for this ledger, its used to ensure we
	// don't drop the reference to this ledger in multi-connection scenarios
	ref int

	lk sync.Mutex
}

type Receipt struct {
	Peer      string
	Value     float64
	Sent      uint64
	Recv      uint64
	Exchanged uint64
}

type debtRatio struct {
	BytesSent uint64
	BytesRecv uint64
}

func (dr *debtRatio) Value() float64 {
	return float64(dr.BytesSent) / float64(dr.BytesRecv+1)
}

func (l *ledger) SentBytes(n int) {
	l.exchangeCount++
	l.lastExchange = time.Now()
	l.Accounting.BytesSent += uint64(n)
}

func (l *ledger) ReceivedBytes(n int) {
	l.exchangeCount++
	l.lastExchange = time.Now()
	l.Accounting.BytesRecv += uint64(n)
}

func (l *ledger) Wants(k *cid.Cid, priority int) {
	log.Debugf("peer %s wants %s", l.Partner, k)
	l.wantList.Add(k, priority)
}

func (l *ledger) CancelWant(k *cid.Cid) {
	l.wantList.Remove(k)
}

func (l *ledger) WantListContains(k *cid.Cid) (*wl.Entry, bool) {
	return l.wantList.Contains(k)
}

func (l *ledger) ExchangeCount() uint64 {
	return l.exchangeCount
}
