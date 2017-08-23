package peerstore

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	testutil "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer/test"
)

func TestLatencyEWMAFun(t *testing.T) {
	t.Skip("run it for fun")

	m := NewMetrics()
	id, err := testutil.RandPeerID()
	if err != nil {
		t.Fatal(err)
	}

	mu := 100.0
	sig := 10.0
	next := func() time.Duration {
		mu = (rand.NormFloat64() * sig) + mu
		return time.Duration(mu)
	}

	print := func() {
		fmt.Printf("%3.f %3.f --> %d\n", sig, mu, m.LatencyEWMA(id))
	}

	for {
		select {
		case <-time.After(200 * time.Millisecond):
			m.RecordLatency(id, next())
			print()
		}
	}
}

func TestLatencyEWMA(t *testing.T) {
	m := NewMetrics()
	id, err := testutil.RandPeerID()
	if err != nil {
		t.Fatal(err)
	}

	exp := 100.0
	mu := exp
	sig := 10.0
	next := func() time.Duration {
		mu := (rand.NormFloat64() * sig) + mu
		return time.Duration(mu)
	}

	for i := 0; i < 10; i++ {
		select {
		case <-time.After(200 * time.Millisecond):
			m.RecordLatency(id, next())
		}
	}

	lat := m.LatencyEWMA(id)
	if math.Abs(exp-float64(lat)) > sig {
		t.Fatal("latency outside of expected range: ", exp, lat, sig)
	}
}
