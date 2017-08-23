package config

import (
	"errors"
	"fmt"

	iaddr "github.com/ipfs/go-ipfs/thirdparty/ipfsaddr"
)

// DefaultBootstrapAddresses are the hardcoded bootstrap addresses
// for IPFS. they are nodes run by the IPFS team. docs on these later.
// As with all p2p networks, bootstrap is an important security concern.
//
// NOTE: This is here -- and not inside cmd/ipfs/init.go -- because of an
// import dependency issue. TODO: move this into a config/default/ package.
var DefaultBootstrapAddresses = []string{
	"/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",            // mars.i.ipfs.io
	"/ip4/104.236.176.52/tcp/4001/ipfs/QmSoLnSGccFuZQJzRadHn95W2CrSFmZuTdDWP8HXaHca9z",            // neptune.i.ipfs.io
	"/ip4/104.236.179.241/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",           // pluto.i.ipfs.io
	"/ip4/162.243.248.213/tcp/4001/ipfs/QmSoLueR4xBeUbY9WZ9xGUUxunbKWcrNFTDAadQJmocnWm",           // uranus.i.ipfs.io
	"/ip4/128.199.219.111/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",           // saturn.i.ipfs.io
	"/ip4/104.236.76.40/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",             // venus.i.ipfs.io
	"/ip4/178.62.158.247/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",            // earth.i.ipfs.io
	"/ip4/178.62.61.185/tcp/4001/ipfs/QmSoLMeWqB7YGVLJN3pNLQpmmEk35v6wYtsMGLzSr5QBU3",             // mercury.i.ipfs.io
	"/ip4/104.236.151.122/tcp/4001/ipfs/QmSoLju6m7xTh3DuokvT3886QRYqxAzb1kShaanJgW36yx",           // jupiter.i.ipfs.io
	"/ip6/2604:a880:1:20::1f9:9001/tcp/4001/ipfs/QmSoLnSGccFuZQJzRadHn95W2CrSFmZuTdDWP8HXaHca9z",  // neptune.i.ipfs.io
	"/ip6/2604:a880:1:20::203:d001/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",  // pluto.i.ipfs.io
	"/ip6/2604:a880:0:1010::23:d001/tcp/4001/ipfs/QmSoLueR4xBeUbY9WZ9xGUUxunbKWcrNFTDAadQJmocnWm", // uranus.i.ipfs.io
	"/ip6/2400:6180:0:d0::151:6001/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",  // saturn.i.ipfs.io
	"/ip6/2604:a880:800:10::4a:5001/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64", // venus.i.ipfs.io
	"/ip6/2a03:b0c0:0:1010::23:1001/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd", // earth.i.ipfs.io
	"/ip6/2a03:b0c0:1:d0::e7:1/tcp/4001/ipfs/QmSoLMeWqB7YGVLJN3pNLQpmmEk35v6wYtsMGLzSr5QBU3",      // mercury.i.ipfs.io
	"/ip6/2604:a880:1:20::1d9:6001/tcp/4001/ipfs/QmSoLju6m7xTh3DuokvT3886QRYqxAzb1kShaanJgW36yx",  // jupiter.i.ipfs.io
}

// BootstrapPeer is a peer used to bootstrap the network.
type BootstrapPeer iaddr.IPFSAddr

// ErrInvalidPeerAddr signals an address is not a valid peer address.
var ErrInvalidPeerAddr = errors.New("invalid peer address")

func (c *Config) BootstrapPeers() ([]BootstrapPeer, error) {
	return ParseBootstrapPeers(c.Bootstrap)
}

// DefaultBootstrapPeers returns the (parsed) set of default bootstrap peers.
// if it fails, it returns a meaningful error for the user.
// This is here (and not inside cmd/ipfs/init) because of module dependency problems.
func DefaultBootstrapPeers() ([]BootstrapPeer, error) {
	ps, err := ParseBootstrapPeers(DefaultBootstrapAddresses)
	if err != nil {
		return nil, fmt.Errorf(`failed to parse hardcoded bootstrap peers: %s
This is a problem with the ipfs codebase. Please report it to the dev team.`, err)
	}
	return ps, nil
}

func (c *Config) SetBootstrapPeers(bps []BootstrapPeer) {
	c.Bootstrap = BootstrapPeerStrings(bps)
}

func ParseBootstrapPeer(addr string) (BootstrapPeer, error) {
	ia, err := iaddr.ParseString(addr)
	if err != nil {
		return nil, err
	}
	return BootstrapPeer(ia), err
}

func ParseBootstrapPeers(addrs []string) ([]BootstrapPeer, error) {
	peers := make([]BootstrapPeer, len(addrs))
	var err error
	for i, addr := range addrs {
		peers[i], err = ParseBootstrapPeer(addr)
		if err != nil {
			return nil, err
		}
	}
	return peers, nil
}

func BootstrapPeerStrings(bps []BootstrapPeer) []string {
	bpss := make([]string, len(bps))
	for i, p := range bps {
		bpss[i] = p.String()
	}
	return bpss
}
