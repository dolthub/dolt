package blankhost

import (
	"context"
	"io"

	inet "gx/ipfs/QmNa31VPzC561NWwRsJLE7nGYZYuuD2QfpK2b1q9BK54J1/go-libp2p-net"
	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	mstream "gx/ipfs/QmTnsezaB1wWNRHeHnYrm8K4d5i9wtyj3GsqjC3Rt5b5v5/go-multistream"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ifconnmgr "gx/ipfs/QmYkCrTwivapqdB3JbwvwvxymseahVkcm46ThRMAA24zCr/go-libp2p-interface-connmgr"
	protocol "gx/ipfs/QmZNkThpqfVXs9GNbexPrfBbXSLNYeKrE7jwFM2oqHbyqN/go-libp2p-protocol"
	host "gx/ipfs/Qmc1XhrFEiSeBNn3mpfg6gEuYCt5im2gYmNVmncsvmpeAk/go-libp2p-host"
)

var log = logging.Logger("blankhost")

// BlankHost is the thinnest implementation of the host.Host interface
type BlankHost struct {
	n    inet.Network
	mux  *mstream.MultistreamMuxer
	cmgr ifconnmgr.ConnManager
}

func NewBlankHost(n inet.Network) *BlankHost {
	bh := &BlankHost{
		n:    n,
		cmgr: &ifconnmgr.NullConnMgr{},
		mux:  mstream.NewMultistreamMuxer(),
	}

	n.SetStreamHandler(bh.newStreamHandler)
	return bh
}

var _ host.Host = (*BlankHost)(nil)

func (bh *BlankHost) Addrs() []ma.Multiaddr {
	addrs, err := bh.n.InterfaceListenAddresses()
	if err != nil {
		log.Debug("error retrieving network interface addrs: ", err)
		return nil
	}

	return addrs
}

func (bh *BlankHost) Close() error {
	return bh.n.Close()
}

func (bh *BlankHost) Connect(ctx context.Context, pi pstore.PeerInfo) error {
	// absorb addresses into peerstore
	bh.Peerstore().AddAddrs(pi.ID, pi.Addrs, pstore.TempAddrTTL)

	cs := bh.n.ConnsToPeer(pi.ID)
	if len(cs) > 0 {
		return nil
	}

	_, err := bh.Network().DialPeer(ctx, pi.ID)
	return err
}

func (bh *BlankHost) Peerstore() pstore.Peerstore {
	return bh.n.Peerstore()
}

func (bh *BlankHost) ID() peer.ID {
	return bh.n.LocalPeer()
}

func (bh *BlankHost) NewStream(ctx context.Context, p peer.ID, protos ...protocol.ID) (inet.Stream, error) {
	s, err := bh.n.NewStream(ctx, p)
	if err != nil {
		return nil, err
	}

	var protoStrs []string
	for _, pid := range protos {
		protoStrs = append(protoStrs, string(pid))
	}

	selected, err := mstream.SelectOneOf(protoStrs, s)
	if err != nil {
		s.Close()
		return nil, err
	}

	selpid := protocol.ID(selected)
	s.SetProtocol(selpid)
	bh.Peerstore().AddProtocols(p, selected)

	return s, nil
}

func (bh *BlankHost) RemoveStreamHandler(p protocol.ID) {
	bh.Mux().RemoveHandler(string(p))
}

func (bh *BlankHost) SetStreamHandler(pid protocol.ID, handler inet.StreamHandler) {
	bh.Mux().AddHandler(string(pid), func(p string, rwc io.ReadWriteCloser) error {
		is := rwc.(inet.Stream)
		is.SetProtocol(protocol.ID(p))
		handler(is)
		return nil
	})
}

func (bh *BlankHost) SetStreamHandlerMatch(pid protocol.ID, m func(string) bool, handler inet.StreamHandler) {
	bh.Mux().AddHandlerWithFunc(string(pid), m, func(p string, rwc io.ReadWriteCloser) error {
		is := rwc.(inet.Stream)
		is.SetProtocol(protocol.ID(p))
		handler(is)
		return nil
	})
}

// newStreamHandler is the remote-opened stream handler for inet.Network
func (h *BlankHost) newStreamHandler(s inet.Stream) {

	protoID, handle, err := h.Mux().Negotiate(s)
	if err != nil {
		log.Warning("protocol mux failed: %s", err)
		s.Close()
		return
	}

	s.SetProtocol(protocol.ID(protoID))

	go handle(protoID, s)
}

// TODO: i'm not sure this really needs to be here
func (bh *BlankHost) Mux() *mstream.MultistreamMuxer {
	return bh.mux
}

// TODO: also not sure this fits... Might be better ways around this (leaky abstractions)
func (bh *BlankHost) Network() inet.Network {
	return bh.n
}

func (bh *BlankHost) ConnManager() ifconnmgr.ConnManager {
	return bh.cmgr
}
