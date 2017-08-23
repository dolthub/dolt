package dht

import (
	"io"

	mstream "gx/ipfs/QmTnsezaB1wWNRHeHnYrm8K4d5i9wtyj3GsqjC3Rt5b5v5/go-multistream"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	inet "gx/ipfs/QmahYsGWry85Y7WUe2SX5G4JkH2zifEQAUtJVLZ24aC9DF/go-libp2p-net"
)

// netNotifiee defines methods to be used with the IpfsDHT
type netNotifiee IpfsDHT

func (nn *netNotifiee) DHT() *IpfsDHT {
	return (*IpfsDHT)(nn)
}

func (nn *netNotifiee) Connected(n inet.Network, v inet.Conn) {
	dht := nn.DHT()
	select {
	case <-dht.Process().Closing():
		return
	default:
	}

	// Note: We *could* just check the peerstore to see if the remote side supports the dht
	// protocol, but its not clear that that information will make it into the peerstore
	// by the time this notification is sent. So just to be very careful, we brute force this
	// and open a new stream
	s, err := dht.host.NewStream(dht.Context(), v.RemotePeer(), ProtocolDHT, ProtocolDHTOld)
	switch err {
	case nil:
		s.Close()
		// connected fine? full dht node
		dht.Update(dht.Context(), v.RemotePeer())
	case mstream.ErrNotSupported:
		// Client mode only, don't bother adding them to our routing table
	case io.EOF:
		// This is kindof an error, but it happens someone often so make it a warning
		log.Warningf("checking dht client type: %s", err)
	default:
		// real error? thats odd
		log.Errorf("checking dht client type: %s", err)
	}
}

func (nn *netNotifiee) Disconnected(n inet.Network, v inet.Conn) {
	dht := nn.DHT()
	select {
	case <-dht.Process().Closing():
		return
	default:
	}
	dht.routingTable.Remove(v.RemotePeer())
}

func (nn *netNotifiee) OpenedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) ClosedStream(n inet.Network, v inet.Stream) {}
func (nn *netNotifiee) Listen(n inet.Network, a ma.Multiaddr)      {}
func (nn *netNotifiee) ListenClose(n inet.Network, a ma.Multiaddr) {}
