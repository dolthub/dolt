package queue

import (
	"container/heap"
	"math/big"
	"sync"

	ks "gx/ipfs/QmUusaX99BZoELh7dmPgirqRQ1FAmMnmnBn3oiqDFGBUSc/go-keyspace"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
)

// peerMetric tracks a peer and its distance to something else.
type peerMetric struct {
	// the peer
	peer peer.ID

	// big.Int for XOR metric
	metric *big.Int
}

// peerMetricHeap implements a heap of peerDistances
type peerMetricHeap []*peerMetric

func (ph peerMetricHeap) Len() int {
	return len(ph)
}

func (ph peerMetricHeap) Less(i, j int) bool {
	return -1 == ph[i].metric.Cmp(ph[j].metric)
}

func (ph peerMetricHeap) Swap(i, j int) {
	ph[i], ph[j] = ph[j], ph[i]
}

func (ph *peerMetricHeap) Push(x interface{}) {
	item := x.(*peerMetric)
	*ph = append(*ph, item)
}

func (ph *peerMetricHeap) Pop() interface{} {
	old := *ph
	n := len(old)
	item := old[n-1]
	*ph = old[0 : n-1]
	return item
}

// distancePQ implements heap.Interface and PeerQueue
type distancePQ struct {
	// from is the Key this PQ measures against
	from ks.Key

	// heap is a heap of peerDistance items
	heap peerMetricHeap

	sync.RWMutex
}

func (pq *distancePQ) Len() int {
	pq.Lock()
	defer pq.Unlock()
	return len(pq.heap)
}

func (pq *distancePQ) Enqueue(p peer.ID) {
	pq.Lock()
	defer pq.Unlock()

	distance := ks.XORKeySpace.Key([]byte(p)).Distance(pq.from)

	heap.Push(&pq.heap, &peerMetric{
		peer:   p,
		metric: distance,
	})
}

func (pq *distancePQ) Dequeue() peer.ID {
	pq.Lock()
	defer pq.Unlock()

	if len(pq.heap) < 1 {
		panic("called Dequeue on an empty PeerQueue")
		// will panic internally anyway, but we can help debug here
	}

	o := heap.Pop(&pq.heap)
	p := o.(*peerMetric)
	return p.peer
}

// NewXORDistancePQ returns a PeerQueue which maintains its peers sorted
// in terms of their distances to each other in an XORKeySpace (i.e. using
// XOR as a metric of distance).
func NewXORDistancePQ(from string) PeerQueue {
	return &distancePQ{
		from: ks.XORKeySpace.Key([]byte(from)),
		heap: peerMetricHeap{},
	}
}
