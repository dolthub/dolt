package kbucket

import (
	"container/list"
	"sync"

	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
)

// Bucket holds a list of peers.
type Bucket struct {
	lk   sync.RWMutex
	list *list.List
}

func newBucket() *Bucket {
	b := new(Bucket)
	b.list = list.New()
	return b
}

func (b *Bucket) Peers() []peer.ID {
	b.lk.RLock()
	defer b.lk.RUnlock()
	ps := make([]peer.ID, 0, b.list.Len())
	for e := b.list.Front(); e != nil; e = e.Next() {
		id := e.Value.(peer.ID)
		ps = append(ps, id)
	}
	return ps
}

func (b *Bucket) Has(id peer.ID) bool {
	b.lk.RLock()
	defer b.lk.RUnlock()
	for e := b.list.Front(); e != nil; e = e.Next() {
		if e.Value.(peer.ID) == id {
			return true
		}
	}
	return false
}

func (b *Bucket) Remove(id peer.ID) {
	b.lk.Lock()
	defer b.lk.Unlock()
	for e := b.list.Front(); e != nil; e = e.Next() {
		if e.Value.(peer.ID) == id {
			b.list.Remove(e)
		}
	}
}

func (b *Bucket) MoveToFront(id peer.ID) {
	b.lk.Lock()
	defer b.lk.Unlock()
	for e := b.list.Front(); e != nil; e = e.Next() {
		if e.Value.(peer.ID) == id {
			b.list.MoveToFront(e)
		}
	}
}

func (b *Bucket) PushFront(p peer.ID) {
	b.lk.Lock()
	b.list.PushFront(p)
	b.lk.Unlock()
}

func (b *Bucket) PopBack() peer.ID {
	b.lk.Lock()
	defer b.lk.Unlock()
	last := b.list.Back()
	b.list.Remove(last)
	return last.Value.(peer.ID)
}

func (b *Bucket) Len() int {
	b.lk.RLock()
	defer b.lk.RUnlock()
	return b.list.Len()
}

// Split splits a buckets peers into two buckets, the methods receiver will have
// peers with CPL equal to cpl, the returned bucket will have peers with CPL
// greater than cpl (returned bucket has closer peers)
func (b *Bucket) Split(cpl int, target ID) *Bucket {
	b.lk.Lock()
	defer b.lk.Unlock()

	out := list.New()
	newbuck := newBucket()
	newbuck.list = out
	e := b.list.Front()
	for e != nil {
		peerID := ConvertPeerID(e.Value.(peer.ID))
		peerCPL := commonPrefixLen(peerID, target)
		if peerCPL > cpl {
			cur := e
			out.PushBack(e.Value)
			e = e.Next()
			b.list.Remove(cur)
			continue
		}
		e = e.Next()
	}
	return newbuck
}
