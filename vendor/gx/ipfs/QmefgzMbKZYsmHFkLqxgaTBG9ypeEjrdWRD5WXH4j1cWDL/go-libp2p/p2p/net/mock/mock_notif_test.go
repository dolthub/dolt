package mocknet

import (
	"context"
	"testing"
	"time"

	inet "gx/ipfs/QmNa31VPzC561NWwRsJLE7nGYZYuuD2QfpK2b1q9BK54J1/go-libp2p-net"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
)

func TestNotifications(t *testing.T) {
	const swarmSize = 5

	mn, err := FullMeshLinked(context.Background(), swarmSize)
	if err != nil {
		t.Fatal(err)
	}

	timeout := 10 * time.Second

	// signup notifs
	nets := mn.Nets()
	notifiees := make([]*netNotifiee, len(nets))
	for i, pn := range nets {
		n := newNetNotifiee(swarmSize)
		pn.Notify(n)
		notifiees[i] = n
	}

	// connect all but self
	if err := mn.ConnectAllButSelf(); err != nil {
		t.Fatal(err)
	}

	// test everyone got the correct connection opened calls
	for i, s := range nets {
		n := notifiees[i]
		notifs := make(map[peer.ID][]inet.Conn)
		for j, s2 := range nets {
			if i == j {
				continue
			}

			// this feels a little sketchy, but its probably okay
			for len(s.ConnsToPeer(s2.LocalPeer())) != len(notifs[s2.LocalPeer()]) {
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
			expect := s.ConnsToPeer(p)
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

	complement := func(c inet.Conn) (inet.Network, *netNotifiee, *conn) {
		for i, s := range nets {
			for _, c2 := range s.Conns() {
				if c2.(*conn).rconn == c {
					return s, notifiees[i], c2.(*conn)
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
		if s != nil && s != s2 {
			t.Fatalf("got incorrect stream %p %p", s, s2)
		}

		select {
		case s2 = <-n.closedStream:
			t.Log("got notif for closed stream")
		case <-time.After(timeout):
			t.Fatal("timeout")
		}
		if s != nil && s != s2 {
			t.Fatalf("got incorrect stream %p %p", s, s2)
		}
	}

	for _, s := range nets {
		s.SetStreamHandler(func(s inet.Stream) {
			s.Close()
		})
	}

	// there's one stream per conn that we need to drain....
	// unsure where these are coming from
	for i := range nets {
		n := notifiees[i]
		for j := 0; j < len(nets)-1; j++ {
			testOCStream(n, nil)
		}
	}

	streams := make(chan inet.Stream)
	for _, s := range nets {
		s.SetStreamHandler(func(s inet.Stream) {
			streams <- s
			s.Close()
		})
	}

	// open a streams in each conn
	for i, s := range nets {
		conns := s.Conns()
		for _, c := range conns {
			_, n2, c2 := complement(c)
			st1, err := c.NewStream()
			if err != nil {
				t.Error(err)
			} else {
				t.Logf("%s %s <--%p--> %s %s", c.LocalPeer(), c.LocalMultiaddr(), st1, c.RemotePeer(), c.RemoteMultiaddr())
				// st1.Write([]byte("hello"))
				st1.Close()
				st2 := <-streams
				t.Logf("%s %s <--%p--> %s %s", c2.LocalPeer(), c2.LocalMultiaddr(), st2, c2.RemotePeer(), c2.RemoteMultiaddr())
				testOCStream(notifiees[i], st1)
				testOCStream(n2, st2)
			}
		}
	}

	// close conns
	for i, s := range nets {
		n := notifiees[i]
		for _, c := range s.Conns() {
			_, n2, c2 := complement(c)
			c.(*conn).Close()
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
