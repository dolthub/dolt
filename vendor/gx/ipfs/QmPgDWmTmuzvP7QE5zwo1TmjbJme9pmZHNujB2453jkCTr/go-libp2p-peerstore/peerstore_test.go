package peerstore

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
)

func getAddrs(t *testing.T, n int) []ma.Multiaddr {
	var addrs []ma.Multiaddr
	for i := 0; i < n; i++ {
		a, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", i))
		if err != nil {
			t.Fatal(err)
		}

		addrs = append(addrs, a)
	}
	return addrs
}

func TestAddrStream(t *testing.T) {
	addrs := getAddrs(t, 100)

	pid := peer.ID("testpeer")

	ps := NewPeerstore()

	ps.AddAddrs(pid, addrs[:10], time.Hour)

	ctx, cancel := context.WithCancel(context.Background())

	addrch := ps.AddrStream(ctx, pid)

	// while that subscription is active, publish ten more addrs
	// this tests that it doesnt hang
	for i := 10; i < 20; i++ {
		ps.AddAddr(pid, addrs[i], time.Hour)
	}

	// now receive them (without hanging)
	timeout := time.After(time.Second * 10)
	for i := 0; i < 20; i++ {
		select {
		case <-addrch:
		case <-timeout:
			t.Fatal("timed out")
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// now send the rest of the addresses
		for _, a := range addrs[20:80] {
			ps.AddAddr(pid, a, time.Hour)
		}
	}()

	// receive some concurrently with the goroutine
	timeout = time.After(time.Second * 10)
	for i := 0; i < 40; i++ {
		select {
		case <-addrch:
		case <-timeout:
		}
	}

	<-done

	// receive some more after waiting for that goroutine to complete
	timeout = time.After(time.Second * 10)
	for i := 0; i < 20; i++ {
		select {
		case <-addrch:
		case <-timeout:
		}
	}

	// now cancel it, and add a few more addresses it doesnt hang afterwards
	cancel()

	for _, a := range addrs[80:] {
		ps.AddAddr(pid, a, time.Hour)
	}
}

func TestGetStreamBeforePeerAdded(t *testing.T) {
	addrs := getAddrs(t, 10)
	pid := peer.ID("testpeer")

	ps := NewPeerstore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ach := ps.AddrStream(ctx, pid)

	for i := 0; i < 10; i++ {
		ps.AddAddr(pid, addrs[i], time.Hour)
	}

	received := make(map[string]bool)
	var count int

	for i := 0; i < 10; i++ {
		a, ok := <-ach
		if !ok {
			t.Fatal("channel shouldnt be closed yet")
		}
		if a == nil {
			t.Fatal("got a nil address, thats weird")
		}
		count++
		if received[a.String()] {
			t.Fatal("received duplicate address")
		}
		received[a.String()] = true
	}

	select {
	case <-ach:
		t.Fatal("shouldnt have received any more addresses")
	default:
	}

	if count != 10 {
		t.Fatal("should have received exactly ten addresses, got ", count)
	}

	for _, a := range addrs {
		if !received[a.String()] {
			t.Log(received)
			t.Fatalf("expected to receive address %s but didnt", a)
		}
	}
}

func TestAddrStreamDuplicates(t *testing.T) {
	addrs := getAddrs(t, 10)
	pid := peer.ID("testpeer")

	ps := NewPeerstore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ach := ps.AddrStream(ctx, pid)

	go func() {
		for i := 0; i < 10; i++ {
			ps.AddAddr(pid, addrs[i], time.Hour)
			ps.AddAddr(pid, addrs[rand.Intn(10)], time.Hour)
		}

		// make sure that all addresses get processed before context is cancelled
		time.Sleep(time.Millisecond * 50)
		cancel()
	}()

	received := make(map[string]bool)
	var count int
	for a := range ach {
		if a == nil {
			t.Fatal("got a nil address, thats weird")
		}
		count++
		if received[a.String()] {
			t.Fatal("received duplicate address")
		}
		received[a.String()] = true
	}

	if count != 10 {
		t.Fatal("should have received exactly ten addresses")
	}
}

func TestPeerstoreProtoStore(t *testing.T) {
	ps := NewPeerstore()
	p1 := peer.ID("TESTPEER")

	protos := []string{"a", "b", "c", "d"}

	err := ps.AddProtocols(p1, protos...)
	if err != nil {
		t.Fatal(err)
	}

	out, err := ps.GetProtocols(p1)
	if err != nil {
		t.Fatal(err)
	}

	if len(out) != len(protos) {
		t.Fatal("got wrong number of protocols back")
	}

	sort.Strings(out)
	for i, p := range protos {
		if out[i] != p {
			t.Fatal("got wrong protocol")
		}
	}

	supported, err := ps.SupportsProtocols(p1, "q", "w", "a", "y", "b")
	if err != nil {
		t.Fatal(err)
	}

	if len(supported) != 2 {
		t.Fatal("only expected 2 supported")
	}

	if supported[0] != "a" || supported[1] != "b" {
		t.Fatal("got wrong supported array: ", supported)
	}

	err = ps.SetProtocols(p1, "other")
	if err != nil {
		t.Fatal(err)
	}

	supported, err = ps.SupportsProtocols(p1, "q", "w", "a", "y", "b")
	if err != nil {
		t.Fatal(err)
	}

	if len(supported) != 0 {
		t.Fatal("none of those protocols should have been supported")
	}
}

func TestBasicPeerstore(t *testing.T) {
	ps := NewPeerstore()

	var pids []peer.ID
	addrs := getAddrs(t, 10)
	for i, a := range addrs {
		p := peer.ID(fmt.Sprint(i))
		pids = append(pids, p)
		ps.AddAddr(p, a, PermanentAddrTTL)
	}

	peers := ps.Peers()
	if len(peers) != 10 {
		t.Fatal("expected ten peers")
	}

	pinfo := ps.PeerInfo(pids[0])
	if !pinfo.Addrs[0].Equal(addrs[0]) {
		t.Fatal("stored wrong address")
	}
}
