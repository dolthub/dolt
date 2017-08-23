package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
)

func TestQueue(t *testing.T) {

	p1 := peer.ID("11140beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a31") // these aren't valid, because need to hex-decode.
	p2 := peer.ID("11140beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a32") // these aren't valid, because need to hex-decode.
	p3 := peer.ID("11140beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33") // these aren't valid, because need to hex-decode.
	p4 := peer.ID("11140beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a34") // these aren't valid, because need to hex-decode.
	p5 := peer.ID("11140beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a31") // these aren't valid, because need to hex-decode.
	// but they work.

	// these are the peer.IDs' XORKeySpace Key values:
	// [228 47 151 130 156 102 222 232 218 31 132 94 170 208 80 253 120 103 55 35 91 237 48 157 81 245 57 247 66 150 9 40]
	// [26 249 85 75 54 49 25 30 21 86 117 62 85 145 48 175 155 194 210 216 58 14 241 143 28 209 129 144 122 28 163 6]
	// [78 135 26 216 178 181 224 181 234 117 2 248 152 115 255 103 244 34 4 152 193 88 9 225 8 127 216 158 226 8 236 246]
	// [125 135 124 6 226 160 101 94 192 57 39 12 18 79 121 140 190 154 147 55 44 83 101 151 63 255 94 179 51 203 241 51]

	pq := NewXORDistancePQ("11140beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a31")
	pq.Enqueue(p3)
	pq.Enqueue(p1)
	pq.Enqueue(p2)
	pq.Enqueue(p4)
	pq.Enqueue(p5)
	pq.Enqueue(p1)

	// should come out as: p1, p4, p3, p2

	if d := pq.Dequeue(); d != p1 && d != p5 {
		t.Error("ordering failed")
	}

	if d := pq.Dequeue(); d != p1 && d != p5 {
		t.Error("ordering failed")
	}

	if d := pq.Dequeue(); d != p1 && d != p5 {
		t.Error("ordering failed")
	}

	if pq.Dequeue() != p4 {
		t.Error("ordering failed")
	}

	if pq.Dequeue() != p3 {
		t.Error("ordering failed")
	}

	if pq.Dequeue() != p2 {
		t.Error("ordering failed")
	}

}

func newPeerTime(t time.Time) peer.ID {
	s := fmt.Sprintf("hmmm time: %v", t)
	h, _ := mh.Sum([]byte(s), mh.SHA2_256, -1)
	return peer.ID(h)
}

func TestSyncQueue(t *testing.T) {
	tickT := time.Microsecond * 50
	max := 5000
	consumerN := 10
	countsIn := make([]int, consumerN*2)
	countsOut := make([]int, consumerN)

	if testing.Short() {
		max = 1000
	}

	ctx := context.Background()
	pq := NewXORDistancePQ("11140beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a31")
	cq := NewChanQueue(ctx, pq)
	wg := sync.WaitGroup{}

	produce := func(p int) {
		defer wg.Done()

		tick := time.Tick(tickT)
		for i := 0; i < max; i++ {
			select {
			case tim := <-tick:
				countsIn[p]++
				cq.EnqChan <- newPeerTime(tim)
			case <-ctx.Done():
				return
			}
		}
	}

	consume := func(c int) {
		defer wg.Done()

		for {
			select {
			case <-cq.DeqChan:
				countsOut[c]++
				if countsOut[c] >= max*2 {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}

	// make n * 2 producers and n consumers
	for i := 0; i < consumerN; i++ {
		wg.Add(3)
		go produce(i)
		go produce(consumerN + i)
		go consume(i)
	}

	wg.Wait()

	sum := func(ns []int) int {
		total := 0
		for _, n := range ns {
			total += n
		}
		return total
	}

	if sum(countsIn) != sum(countsOut) {
		t.Errorf("didnt get all of them out: %d/%d", sum(countsOut), sum(countsIn))
	}
}
