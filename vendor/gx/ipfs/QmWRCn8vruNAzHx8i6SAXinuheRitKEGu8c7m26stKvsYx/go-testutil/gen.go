package testutil

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ptest "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer/test"
	ci "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
)

// ZeroLocalTCPAddress is the "zero" tcp local multiaddr. This means:
//   /ip4/127.0.0.1/tcp/0
var ZeroLocalTCPAddress ma.Multiaddr

func init() {
	// initialize ZeroLocalTCPAddress
	maddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	if err != nil {
		panic(err)
	}
	ZeroLocalTCPAddress = maddr
}

func RandTestKeyPair(bits int) (ci.PrivKey, ci.PubKey, error) {
	return ptest.RandTestKeyPair(bits)
}

func SeededTestKeyPair(seed int64) (ci.PrivKey, ci.PubKey, error) {
	return ptest.SeededTestKeyPair(seed)
}

// RandPeerID generates random "valid" peer IDs. it does not NEED to generate
// keys because it is as if we lost the key right away. fine to read randomness
// and hash it. to generate proper keys and corresponding PeerID, use:
//  sk, pk, _ := testutil.RandKeyPair()
//  id, _ := peer.IDFromPublicKey(pk)
func RandPeerID() (peer.ID, error) {
	buf := make([]byte, 16)
	rand.Read(buf)
	h, _ := mh.Sum(buf, mh.SHA2_256, -1)
	return peer.ID(h), nil
}

func RandCidV0() (*cid.Cid, error) {
	buf := make([]byte, 16)
	rand.Read(buf)
	return cid.NewCidV0(buf), nil
}

func RandPeerIDFatal(t testing.TB) peer.ID {
	p, err := RandPeerID()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// RandLocalTCPAddress returns a random multiaddr. it suppresses errors
// for nice composability-- do check the address isn't nil.
//
// NOTE: for real network tests, use ZeroLocalTCPAddress so the kernel
// assigns an unused TCP port. otherwise you may get clashes. This
// function remains here so that p2p/net/mock (which does not touch the
// real network) can assign different addresses to peers.
func RandLocalTCPAddress() ma.Multiaddr {

	// chances are it will work out, but it **might** fail if the port is in use
	// most ports above 10000 aren't in use by long running processes, so yay.
	// (maybe there should be a range of "loopback" ports that are guaranteed
	// to be open for the process, but naturally can only talk to self.)

	lastPort.Lock()
	if lastPort.port == 0 {
		lastPort.port = 10000 + SeededRand.Intn(50000)
	}
	port := lastPort.port
	lastPort.port++
	lastPort.Unlock()

	addr := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)
	maddr, _ := ma.NewMultiaddr(addr)
	return maddr
}

var lastPort = struct {
	port int
	sync.Mutex
}{}

// PeerNetParams is a struct to bundle together the four things
// you need to run a connection with a peer: id, 2keys, and addr.
type PeerNetParams struct {
	ID      peer.ID
	PrivKey ci.PrivKey
	PubKey  ci.PubKey
	Addr    ma.Multiaddr
}

func (p *PeerNetParams) checkKeys() error {
	if !p.ID.MatchesPrivateKey(p.PrivKey) {
		return errors.New("p.ID does not match p.PrivKey")
	}

	if !p.ID.MatchesPublicKey(p.PubKey) {
		return errors.New("p.ID does not match p.PubKey")
	}

	buf := new(bytes.Buffer)
	buf.Write([]byte("hello world. this is me, I swear."))
	b := buf.Bytes()

	sig, err := p.PrivKey.Sign(b)
	if err != nil {
		return fmt.Errorf("sig signing failed: %s", err)
	}

	sigok, err := p.PubKey.Verify(b, sig)
	if err != nil {
		return fmt.Errorf("sig verify failed: %s", err)
	}
	if !sigok {
		return fmt.Errorf("sig verify failed: sig invalid")
	}

	return nil // ok. move along.
}

func RandPeerNetParamsOrFatal(t *testing.T) PeerNetParams {
	p, err := RandPeerNetParams()
	if err != nil {
		t.Fatal(err)
		return PeerNetParams{} // TODO return nil
	}
	return *p
}

func RandPeerNetParams() (*PeerNetParams, error) {
	var p PeerNetParams
	var err error
	p.Addr = ZeroLocalTCPAddress
	p.PrivKey, p.PubKey, err = RandTestKeyPair(512)
	if err != nil {
		return nil, err
	}
	p.ID, err = peer.IDFromPublicKey(p.PubKey)
	if err != nil {
		return nil, err
	}
	if err := p.checkKeys(); err != nil {
		return nil, err
	}
	return &p, nil
}
