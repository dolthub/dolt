package ipfsaddr

import (
	"errors"

	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"

	path "github.com/ipfs/go-ipfs/path"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	circuit "gx/ipfs/Qmf7GSJ4omRJsvA9uzTqzbnVhq4RWLPzjzW4xJzUta4dKE/go-libp2p-circuit"
)

var log = logging.Logger("ipfsaddr")

// ErrInvalidAddr signals an address is not a valid IPFS address.
var ErrInvalidAddr = errors.New("invalid IPFS address")

type IPFSAddr interface {
	ID() peer.ID
	Multiaddr() ma.Multiaddr
	Transport() ma.Multiaddr
	String() string
	Equal(b interface{}) bool
}

type ipfsAddr struct {
	ma ma.Multiaddr
	id peer.ID
}

func (a ipfsAddr) ID() peer.ID {
	return a.id
}

func (a ipfsAddr) Multiaddr() ma.Multiaddr {
	return a.ma
}

func (a ipfsAddr) Transport() ma.Multiaddr {
	return Transport(a)
}

func (a ipfsAddr) String() string {
	return a.ma.String()
}

func (a ipfsAddr) Equal(b interface{}) bool {
	if ib, ok := b.(IPFSAddr); ok {
		return a.Multiaddr().Equal(ib.Multiaddr())
	}
	if mb, ok := b.(ma.Multiaddr); ok {
		return a.Multiaddr().Equal(mb)
	}
	return false
}

// ParseString parses a string representation of an address into an IPFSAddr
func ParseString(str string) (a IPFSAddr, err error) {
	if str == "" {
		return nil, ErrInvalidAddr
	}

	m, err := ma.NewMultiaddr(str)
	if err != nil {
		return nil, err
	}

	return ParseMultiaddr(m)
}

// ParseMultiaddr parses a multiaddr into an IPFSAddr
func ParseMultiaddr(m ma.Multiaddr) (a IPFSAddr, err error) {
	// never panic.
	defer func() {
		if r := recover(); r != nil {
			log.Debug("recovered from panic: ", r)
			a = nil
			err = ErrInvalidAddr
		}
	}()

	if m == nil {
		return nil, ErrInvalidAddr
	}

	// make sure it's an IPFS addr
	parts := ma.Split(m)
	if len(parts) < 1 {
		return nil, ErrInvalidAddr
	}
	ipfspart := parts[len(parts)-1] // last part
	if ipfspart.Protocols()[0].Code != ma.P_IPFS {
		return nil, ErrInvalidAddr
	}

	// make sure 'ipfs id' parses as a peer.ID
	peerIdParts := path.SplitList(ipfspart.String())
	peerIdStr := peerIdParts[len(peerIdParts)-1]
	id, err := peer.IDB58Decode(peerIdStr)
	if err != nil {
		return nil, err
	}

	return ipfsAddr{ma: m, id: id}, nil
}

func Transport(iaddr IPFSAddr) (maddr ma.Multiaddr) {
	maddr = iaddr.Multiaddr()

	// /ipfs/QmId is part of the transport address for p2p-circuit
	// TODO clean up the special case
	// we need a consistent way of composing and consumig multiaddrs
	// so that we don't have to do this
	_, err := maddr.ValueForProtocol(circuit.P_CIRCUIT)
	if err == nil {
		return maddr
	}

	split := ma.Split(maddr)
	maddr = ma.Join(split[:len(split)-1]...)
	return
}
