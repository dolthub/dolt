package relay_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	. "gx/ipfs/QmbMNjK69isbpzVGKKrsnM7Sqyh3TVKAphRn5WuUhwTFbW/go-libp2p-circuit"
	pb "gx/ipfs/QmbMNjK69isbpzVGKKrsnM7Sqyh3TVKAphRn5WuUhwTFbW/go-libp2p-circuit/pb"

	bhost "gx/ipfs/QmPZRCaYeNLMo5GfcRS2rv9ZxVuXXt6MFg9dWLmgsdXKCw/go-libp2p-blankhost"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	host "gx/ipfs/QmaSxYRuMq4pkpBBG2CYaRrPx2z7NmMVEs34b9g61biQA6/go-libp2p-host"
	netutil "gx/ipfs/QmdzuGp4a9pahgXuBeReHdYGUzdVX3FUCwfmWVo5mQfkTi/go-libp2p-netutil"
)

/* TODO: add tests
- simple A -[R]-> B
- A tries to relay through R, R doesnt support relay
- A tries to relay through R to B, B doesnt support relay
- A sends too long multiaddr
- R drops stream mid-message
- A relays through R, R has no connection to B
*/

func getNetHosts(t *testing.T, ctx context.Context, n int) []host.Host {
	var out []host.Host

	for i := 0; i < n; i++ {
		netw := netutil.GenSwarmNetwork(t, ctx)
		h := bhost.NewBlankHost(netw)
		out = append(out, h)
	}

	return out
}

func connect(t *testing.T, a, b host.Host) {
	pinfo := a.Peerstore().PeerInfo(a.ID())
	err := b.Connect(context.Background(), pinfo)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBasicRelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 3)

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[1], hosts[2])

	time.Sleep(10 * time.Millisecond)

	r1, err := NewRelay(ctx, hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewRelay(ctx, hosts[1], OptHop)
	if err != nil {
		t.Fatal(err)
	}

	r3, err := NewRelay(ctx, hosts[2])
	if err != nil {
		t.Fatal(err)
	}

	msg := []byte("relay works!")
	go func() {
		list := r3.Listener()

		con, err := list.Accept()
		if err != nil {
			t.Error(err)
			return
		}

		_, err = con.Write(msg)
		if err != nil {
			t.Error(err)
			return
		}
		con.Close()
	}()

	rinfo := hosts[1].Peerstore().PeerInfo(hosts[1].ID())
	dinfo := hosts[2].Peerstore().PeerInfo(hosts[2].ID())

	rctx, rcancel := context.WithTimeout(ctx, time.Second)
	defer rcancel()

	con, err := r1.DialPeer(rctx, rinfo, dinfo)
	if err != nil {
		t.Fatal(err)
	}

	data, err := ioutil.ReadAll(con)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, msg) {
		t.Fatal("message was incorrect:", string(data))
	}
}

func TestBasicRelayDial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 3)

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[1], hosts[2])

	time.Sleep(10 * time.Millisecond)

	r1, err := NewRelay(ctx, hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewRelay(ctx, hosts[1], OptHop)
	if err != nil {
		t.Fatal(err)
	}

	r3, err := NewRelay(ctx, hosts[2])
	if err != nil {
		t.Fatal(err)
	}

	msg := []byte("relay works!")
	go func() {
		list := r3.Listener()

		con, err := list.Accept()
		if err != nil {
			t.Error(err)
			return
		}

		_, err = con.Write(msg)
		if err != nil {
			t.Error(err)
			return
		}
		con.Close()
	}()

	addr, err := ma.NewMultiaddr(fmt.Sprintf("/ipfs/%s/p2p-circuit/ipfs/%s", hosts[1].ID().Pretty(), hosts[2].ID().Pretty()))
	if err != nil {
		t.Fatal(err)
	}

	rctx, rcancel := context.WithTimeout(ctx, time.Second)
	defer rcancel()

	d := r1.Dialer()
	con, err := d.DialContext(rctx, addr)
	if err != nil {
		t.Fatal(err)
	}

	data, err := ioutil.ReadAll(con)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, msg) {
		t.Fatal("message was incorrect:", string(data))
	}
}

func TestUnspecificRelayDial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 3)

	r1, err := NewRelay(ctx, hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewRelay(ctx, hosts[1], OptHop)
	if err != nil {
		t.Fatal(err)
	}

	r3, err := NewRelay(ctx, hosts[2])
	if err != nil {
		t.Fatal(err)
	}

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[1], hosts[2])

	time.Sleep(100 * time.Millisecond)

	msg := []byte("relay works!")
	go func() {
		list := r3.Listener()

		con, err := list.Accept()
		if err != nil {
			t.Error(err)
			return
		}

		_, err = con.Write(msg)
		if err != nil {
			t.Error(err)
			return
		}
		con.Close()
	}()

	addr, err := ma.NewMultiaddr(fmt.Sprintf("/p2p-circuit/ipfs/%s", hosts[2].ID().Pretty()))
	if err != nil {
		t.Fatal(err)
	}

	rctx, rcancel := context.WithTimeout(ctx, time.Second)
	defer rcancel()

	d := r1.Dialer()
	con, err := d.DialContext(rctx, addr)
	if err != nil {
		t.Fatal(err)
	}

	data, err := ioutil.ReadAll(con)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, msg) {
		t.Fatal("message was incorrect:", string(data))
	}
}

func TestRelayThroughNonHop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 3)

	connect(t, hosts[0], hosts[1])
	connect(t, hosts[1], hosts[2])

	time.Sleep(10 * time.Millisecond)

	r1, err := NewRelay(ctx, hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewRelay(ctx, hosts[1])
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewRelay(ctx, hosts[2])
	if err != nil {
		t.Fatal(err)
	}

	rinfo := hosts[1].Peerstore().PeerInfo(hosts[1].ID())
	dinfo := hosts[2].Peerstore().PeerInfo(hosts[2].ID())

	rctx, rcancel := context.WithTimeout(ctx, time.Second)
	defer rcancel()

	_, err = r1.DialPeer(rctx, rinfo, dinfo)
	if err == nil {
		t.Fatal("expected error")
	}

	rerr, ok := err.(RelayError)
	if !ok {
		t.Fatalf("expected RelayError: %#v", err)
	}

	if rerr.Code != pb.CircuitRelay_HOP_CANT_SPEAK_RELAY {
		t.Fatal("expected 'HOP_CANT_SPEAK_RELAY' error")
	}
}

func TestRelayNoDestConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 3)

	connect(t, hosts[0], hosts[1])

	time.Sleep(10 * time.Millisecond)

	r1, err := NewRelay(ctx, hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewRelay(ctx, hosts[1], OptHop)
	if err != nil {
		t.Fatal(err)
	}

	rinfo := hosts[1].Peerstore().PeerInfo(hosts[1].ID())
	dinfo := hosts[2].Peerstore().PeerInfo(hosts[2].ID())

	rctx, rcancel := context.WithTimeout(ctx, time.Second)
	defer rcancel()

	_, err = r1.DialPeer(rctx, rinfo, dinfo)
	if err == nil {
		t.Fatal("expected error")
	}

	rerr, ok := err.(RelayError)
	if !ok {
		t.Fatalf("expected RelayError: %#v", err)
	}

	if rerr.Code != pb.CircuitRelay_HOP_NO_CONN_TO_DST {
		t.Fatal("expected 'HOP_NO_CONN_TO_DST' error")
	}
}

func TestActiveRelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 3)

	connect(t, hosts[0], hosts[1])

	time.Sleep(10 * time.Millisecond)

	r1, err := NewRelay(ctx, hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewRelay(ctx, hosts[1], OptHop, OptActive)
	if err != nil {
		t.Fatal(err)
	}

	r3, err := NewRelay(ctx, hosts[2])
	if err != nil {
		t.Fatal(err)
	}

	msg := []byte("relay works!")
	go func() {
		list := r3.Listener()

		con, err := list.Accept()
		if err != nil {
			t.Error(err)
			return
		}

		_, err = con.Write(msg)
		if err != nil {
			t.Error(err)
			return
		}
		con.Close()
	}()

	rinfo := hosts[1].Peerstore().PeerInfo(hosts[1].ID())
	dinfo := hosts[2].Peerstore().PeerInfo(hosts[2].ID())

	rctx, rcancel := context.WithTimeout(ctx, time.Second)
	defer rcancel()

	con, err := r1.DialPeer(rctx, rinfo, dinfo)
	if err != nil {
		t.Fatal(err)
	}

	data, err := ioutil.ReadAll(con)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, msg) {
		t.Fatal("message was incorrect:", string(data))
	}
}

func TestRelayCanHop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getNetHosts(t, ctx, 2)

	connect(t, hosts[0], hosts[1])

	time.Sleep(10 * time.Millisecond)

	r1, err := NewRelay(ctx, hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewRelay(ctx, hosts[1], OptHop)
	if err != nil {
		t.Fatal(err)
	}

	canhop, err := r1.CanHop(ctx, hosts[1].ID())
	if err != nil {
		t.Fatal(err)
	}

	if !canhop {
		t.Fatal("Relay can't hop")
	}
}
