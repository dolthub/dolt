package dht

import (
	"context"
	"io"

	inet "gx/ipfs/QmNa31VPzC561NWwRsJLE7nGYZYuuD2QfpK2b1q9BK54J1/go-libp2p-net"
	mstream "gx/ipfs/QmTnsezaB1wWNRHeHnYrm8K4d5i9wtyj3GsqjC3Rt5b5v5/go-multistream"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

// netNotifiee defines methods to be used with the IpfsDHT
type netNotifiee IpfsDHT

func (nn *netNotifiee) DHT() *IpfsDHT {
	return (*IpfsDHT)(nn)
}

type peerTracker struct {
	refcount int
	cancel   func()
}

func (nn *netNotifiee) Connected(n inet.Network, v inet.Conn) {
	dht := nn.DHT()
	select {
	case <-dht.Process().Closing():
		return
	default:
	}

	dht.plk.Lock()
	defer dht.plk.Unlock()

	conn, ok := nn.peers[v.RemotePeer()]
	if ok {
		conn.refcount++
		return
	}

	ctx, cancel := context.WithCancel(dht.Context())

	nn.peers[v.RemotePeer()] = &peerTracker{
		refcount: 1,
		cancel:   cancel,
	}

	// Note: We *could* just check the peerstore to see if the remote side supports the dht
	// protocol, but its not clear that that information will make it into the peerstore
	// by the time this notification is sent. So just to be very careful, we brute force this
	// and open a new stream
	go nn.testConnection(ctx, v)

}

func (nn *netNotifiee) testConnection(ctx context.Context, v inet.Conn) {
	dht := nn.DHT()
	for {
		s, err := dht.host.NewStream(ctx, v.RemotePeer(), ProtocolDHT, ProtocolDHTOld)

		switch err {
		case nil:
			s.Close()
			dht.plk.Lock()

			// Check if canceled under the lock.
			if ctx.Err() == nil {
				dht.Update(dht.Context(), v.RemotePeer())
			}

			dht.plk.Unlock()
		case io.EOF:
			if ctx.Err() == nil {
				// Connection died but we may still have *an* open connection (context not canceled) so try again.
				continue
			}
		case mstream.ErrNotSupported:
			// Client mode only, don't bother adding them to our routing table
		default:
			// real error? thats odd
			log.Errorf("checking dht client type: %s", err)
		}
		return
	}
}

func (nn *netNotifiee) Disconnected(n inet.Network, v inet.Conn) {
	dht := nn.DHT()
	select {
	case <-dht.Process().Closing():
		return
	default:
	}

	p := v.RemotePeer()

	func() {
		dht.plk.Lock()
		defer dht.plk.Unlock()

		conn, ok := nn.peers[p]
		if !ok {
			// Unmatched disconnects are fine. It just means that we were
			// already connected when we registered the listener.
			return
		}
		conn.refcount -= 1
		if conn.refcount == 0 {
			delete(nn.peers, p)
			conn.cancel()
			dht.routingTable.Remove(p)
		}
	}()

	dht.smlk.Lock()
	defer dht.smlk.Unlock()
	ms, ok := dht.strmap[p]
	if !ok {
		return
	}
	delete(dht.strmap, p)

	// Do this asynchronously as ms.lk can block for a while.
	go func() {
		ms.lk.Lock()
		defer ms.lk.Unlock()
		ms.invalidate()
	}()
}

func (nn *netNotifiee) OpenedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) ClosedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) Listen(n inet.Network, a ma.Multiaddr)      {}
func (nn *netNotifiee) ListenClose(n inet.Network, a ma.Multiaddr) {}
