package peerset

import (
	"sync"

	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
)

// PeerSet is a threadsafe set of peers
type PeerSet struct {
	ps   map[peer.ID]struct{}
	lk   sync.RWMutex
	size int
}

func New() *PeerSet {
	ps := new(PeerSet)
	ps.ps = make(map[peer.ID]struct{})
	ps.size = -1
	return ps
}

func NewLimited(size int) *PeerSet {
	ps := new(PeerSet)
	ps.ps = make(map[peer.ID]struct{})
	ps.size = size
	return ps
}

func (ps *PeerSet) Add(p peer.ID) {
	ps.lk.Lock()
	ps.ps[p] = struct{}{}
	ps.lk.Unlock()
}

func (ps *PeerSet) Contains(p peer.ID) bool {
	ps.lk.RLock()
	_, ok := ps.ps[p]
	ps.lk.RUnlock()
	return ok
}

func (ps *PeerSet) Size() int {
	ps.lk.RLock()
	defer ps.lk.RUnlock()
	return len(ps.ps)
}

// TryAdd Attempts to add the given peer into the set.
// This operation can fail for one of two reasons:
// 1) The given peer is already in the set
// 2) The number of peers in the set is equal to size
func (ps *PeerSet) TryAdd(p peer.ID) bool {
	var success bool
	ps.lk.Lock()
	if _, ok := ps.ps[p]; !ok && (len(ps.ps) < ps.size || ps.size == -1) {
		success = true
		ps.ps[p] = struct{}{}
	}
	ps.lk.Unlock()
	return success
}

func (ps *PeerSet) Peers() []peer.ID {
	ps.lk.Lock()
	out := make([]peer.ID, 0, len(ps.ps))
	for p, _ := range ps.ps {
		out = append(out, p)
	}
	ps.lk.Unlock()
	return out
}
