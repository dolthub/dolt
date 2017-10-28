package swarm

import (
	"testing"
	"time"

	"context"
	inet "gx/ipfs/QmNa31VPzC561NWwRsJLE7nGYZYuuD2QfpK2b1q9BK54J1/go-libp2p-net"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
)

func streamsSame(a, b inet.Stream) bool {
	sa := a.(*Stream)
	sb := b.(*Stream)
	return sa.Stream() == sb.Stream()
}

func TestNotifications(t *testing.T) {
	const swarmSize = 5

	ctx := context.Background()
	swarms := makeSwarms(ctx, t, swarmSize)
	defer func() {
		for _, s := range swarms {
			s.Close()
		}
	}()

	timeout := 5 * time.Second

	// signup notifs
	notifiees := make([]*netNotifiee, len(swarms))
	for i, swarm := range swarms {
		n := newNetNotifiee(swarmSize)
		swarm.Notify(n)
		notifiees[i] = n
	}

	connectSwarms(t, ctx, swarms)

	<-time.After(time.Millisecond)
	// should've gotten 5 by now.

	// test everyone got the correct connection opened calls
	for i, s := range swarms {
		n := notifiees[i]
		notifs := make(map[peer.ID][]inet.Conn)
		for j, s2 := range swarms {
			if i == j {
				continue
			}

			// this feels a little sketchy, but its probably okay
			for len(s.ConnectionsToPeer(s2.LocalPeer())) != len(notifs[s2.LocalPeer()]) {
				select {
				case c := <-n.connected:
					nfp := notifs[c.RemotePeer()]
					notifs[c.RemotePeer()] = append(nfp, c)
				case <-time.After(timeout):
					t.Fatal("timeout")
				}
			}
		}

		for p, cons := range notifs {
			expect := s.ConnectionsToPeer(p)
			if len(expect) != len(cons) {
				t.Fatal("got different number of connections")
			}

			for _, c := range cons {
				var found bool
				for _, c2 := range expect {
					if c == c2 {
						found = true
						break
					}
				}

				if !found {
					t.Fatal("connection not found!")
				}
			}
		}
	}

	complement := func(c inet.Conn) (*Swarm, *netNotifiee, *Conn) {
		for i, s := range swarms {
			for _, c2 := range s.Connections() {
				if c.LocalMultiaddr().Equal(c2.RemoteMultiaddr()) &&
					c2.LocalMultiaddr().Equal(c.RemoteMultiaddr()) {
					return s, notifiees[i], c2
				}
			}
		}
		t.Fatal("complementary conn not found", c)
		return nil, nil, nil
	}

	testOCStream := func(n *netNotifiee, s inet.Stream) {
		var s2 inet.Stream
		select {
		case s2 = <-n.openedStream:
			t.Log("got notif for opened stream")
		case <-time.After(timeout):
			t.Fatal("timeout")
		}
		if !streamsSame(s, s2) {
			t.Fatal("got incorrect stream", s.Conn(), s2.Conn())
		}

		select {
		case s2 = <-n.closedStream:
			t.Log("got notif for closed stream")
		case <-time.After(timeout):
			t.Fatal("timeout")
		}
		if !streamsSame(s, s2) {
			t.Fatal("got incorrect stream", s.Conn(), s2.Conn())
		}
	}

	streams := make(chan inet.Stream)
	for _, s := range swarms {
		s.SetStreamHandler(func(s inet.Stream) {
			streams <- s
			s.Reset()
		})
	}

	// open a streams in each conn
	for i, s := range swarms {
		for _, c := range s.Connections() {
			_, n2, _ := complement(c)

			st1, err := c.NewStream()
			if err != nil {
				t.Error(err)
			} else {
				st1.Write([]byte("hello"))
				st1.Reset()
				testOCStream(notifiees[i], st1)
				st2 := <-streams
				testOCStream(n2, st2)
			}
		}
	}

	// close conns
	for i, s := range swarms {
		n := notifiees[i]
		for _, c := range s.Connections() {
			_, n2, c2 := complement(c)
			c.Close()
			c2.Close()

			var c3, c4 inet.Conn
			select {
			case c3 = <-n.disconnected:
			case <-time.After(timeout):
				t.Fatal("timeout")
			}
			if c != c3 {
				t.Fatal("got incorrect conn", c, c3)
			}

			select {
			case c4 = <-n2.disconnected:
			case <-time.After(timeout):
				t.Fatal("timeout")
			}
			if c2 != c4 {
				t.Fatal("got incorrect conn", c, c2)
			}
		}
	}
}

type netNotifiee struct {
	listen       chan ma.Multiaddr
	listenClose  chan ma.Multiaddr
	connected    chan inet.Conn
	disconnected chan inet.Conn
	openedStream chan inet.Stream
	closedStream chan inet.Stream
}

func newNetNotifiee(buffer int) *netNotifiee {
	return &netNotifiee{
		listen:       make(chan ma.Multiaddr, buffer),
		listenClose:  make(chan ma.Multiaddr, buffer),
		connected:    make(chan inet.Conn, buffer),
		disconnected: make(chan inet.Conn, buffer),
		openedStream: make(chan inet.Stream, buffer),
		closedStream: make(chan inet.Stream, buffer),
	}
}

func (nn *netNotifiee) Listen(n inet.Network, a ma.Multiaddr) {
	nn.listen <- a
}
func (nn *netNotifiee) ListenClose(n inet.Network, a ma.Multiaddr) {
	nn.listenClose <- a
}
func (nn *netNotifiee) Connected(n inet.Network, v inet.Conn) {
	nn.connected <- v
}
func (nn *netNotifiee) Disconnected(n inet.Network, v inet.Conn) {
	nn.disconnected <- v
}
func (nn *netNotifiee) OpenedStream(n inet.Network, v inet.Stream) {
	nn.openedStream <- v
}
func (nn *netNotifiee) ClosedStream(n inet.Network, v inet.Stream) {
	nn.closedStream <- v
}
