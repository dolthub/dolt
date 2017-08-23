package secio

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	msgio "gx/ipfs/QmRQhVisS8dmPbjBUthVkenn81pBxrx1GxE281csJhm2vL/go-msgio"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	pb "gx/ipfs/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M/go-libp2p-secio/pb"
	ci "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
)

var log = logging.Logger("secio")

// ErrUnsupportedKeyType is returned when a private key cast/type switch fails.
var ErrUnsupportedKeyType = errors.New("unsupported key type")

// ErrClosed signals the closing of a connection.
var ErrClosed = errors.New("connection closed")

// ErrEcho is returned when we're attempting to handshake with the same keys and nonces.
var ErrEcho = errors.New("same keys and nonces. one side talking to self")

// HandshakeTimeout governs how long the handshake will be allowed to take place for.
// Making this number large means there could be many bogus connections waiting to
// timeout in flight. Typical handshakes take ~3RTTs, so it should be completed within
// seconds across a typical planet in the solar system.
var HandshakeTimeout = time.Second * 30

// nonceSize is the size of our nonces (in bytes)
const nonceSize = 16

// secureSession encapsulates all the parameters needed for encrypting
// and decrypting traffic from an insecure channel.
type secureSession struct {
	ctx context.Context

	secure    msgio.ReadWriteCloser
	insecure  io.ReadWriteCloser
	insecureM msgio.ReadWriter

	localKey   ci.PrivKey
	localPeer  peer.ID
	remotePeer peer.ID

	local  encParams
	remote encParams

	sharedSecret []byte

	handshakeMu   sync.Mutex // guards handshakeDone + handshakeErr
	handshakeDone bool
	handshakeErr  error
}

func (s *secureSession) Loggable() map[string]interface{} {
	m := make(map[string]interface{})
	m["localPeer"] = s.localPeer.Pretty()
	m["remotePeer"] = s.remotePeer.Pretty()
	m["established"] = (s.secure != nil)
	return m
}

func newSecureSession(ctx context.Context, local peer.ID, key ci.PrivKey, insecure io.ReadWriteCloser) (*secureSession, error) {
	s := &secureSession{localPeer: local, localKey: key}

	switch {
	case s.localPeer == "":
		return nil, errors.New("no local id provided")
	case s.localKey == nil:
		return nil, errors.New("no local private key provided")
	case !s.localPeer.MatchesPrivateKey(s.localKey):
		return nil, fmt.Errorf("peer.ID does not match PrivateKey")
	case insecure == nil:
		return nil, fmt.Errorf("insecure ReadWriter is nil")
	}

	s.ctx = ctx
	s.insecure = insecure
	s.insecureM = msgio.NewReadWriter(insecure)
	return s, nil
}

func (s *secureSession) Handshake() error {
	s.handshakeMu.Lock()
	defer s.handshakeMu.Unlock()

	if s.handshakeErr != nil {
		return s.handshakeErr
	}

	if !s.handshakeDone {
		s.handshakeErr = s.runHandshake()
		s.handshakeDone = true
	}
	return s.handshakeErr
}

func hashSha256(data []byte) mh.Multihash {
	h, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		// this error can be safely ignored (panic) because multihash only fails
		// from the selection of hash function. If the fn + length are valid, it
		// won't error.
		panic("multihash failed to hash using SHA2_256.")
	}
	return h
}

// runHandshake performs initial communication over insecure channel to share
// keys, IDs, and initiate communication, assigning all necessary params.
// requires the duplex channel to be a msgio.ReadWriter (for framed messaging)
func (s *secureSession) runHandshake() error {
	defer func() { s.ctx = nil }()                              // clear to save memory
	ctx, cancel := context.WithTimeout(s.ctx, HandshakeTimeout) // remove
	defer cancel()

	// =============================================================================
	// step 1. Propose -- propose cipher suite + send pubkeys + nonce

	// Generate and send Hello packet.
	// Hello = (rand, PublicKey, Supported)
	nonceOut := make([]byte, nonceSize)
	_, err := rand.Read(nonceOut)
	if err != nil {
		return err
	}

	defer log.EventBegin(ctx, "secureHandshake", s).Done()

	s.local.permanentPubKey = s.localKey.GetPublic()
	myPubKeyBytes, err := s.local.permanentPubKey.Bytes()
	if err != nil {
		return err
	}

	proposeOut := new(pb.Propose)
	proposeOut.Rand = nonceOut
	proposeOut.Pubkey = myPubKeyBytes
	proposeOut.Exchanges = &SupportedExchanges
	proposeOut.Ciphers = &SupportedCiphers
	proposeOut.Hashes = &SupportedHashes

	// log.Debugf("1.0 Propose: nonce:%s exchanges:%s ciphers:%s hashes:%s",
	// 	nonceOut, SupportedExchanges, SupportedCiphers, SupportedHashes)

	// Send Propose packet (respects ctx)
	proposeOutBytes, err := writeMsgCtx(ctx, s.insecureM, proposeOut)
	if err != nil {
		return err
	}

	// Receive + Parse their Propose packet and generate an Exchange packet.
	proposeIn := new(pb.Propose)
	proposeInBytes, err := readMsgCtx(ctx, s.insecureM, proposeIn)
	if err != nil {
		return err
	}

	// log.Debugf("1.0.1 Propose recv: nonce:%s exchanges:%s ciphers:%s hashes:%s",
	// 	proposeIn.GetRand(), proposeIn.GetExchanges(), proposeIn.GetCiphers(), proposeIn.GetHashes())

	// =============================================================================
	// step 1.1 Identify -- get identity from their key

	// get remote identity
	s.remote.permanentPubKey, err = ci.UnmarshalPublicKey(proposeIn.GetPubkey())
	if err != nil {
		return err
	}

	// get peer id
	s.remotePeer, err = peer.IDFromPublicKey(s.remote.permanentPubKey)
	if err != nil {
		return err
	}

	log.Debugf("1.1 Identify: %s Remote Peer Identified as %s", s.localPeer, s.remotePeer)

	// =============================================================================
	// step 1.2 Selection -- select/agree on best encryption parameters

	// to determine order, use cmp(H(remote_pubkey||local_rand), H(local_pubkey||remote_rand)).
	oh1 := hashSha256(append(proposeIn.GetPubkey(), nonceOut...))
	oh2 := hashSha256(append(myPubKeyBytes, proposeIn.GetRand()...))
	order := bytes.Compare(oh1, oh2)
	if order == 0 {
		return ErrEcho // talking to self (same socket. must be reuseport + dialing self)
	}

	s.local.curveT, err = selectBest(order, SupportedExchanges, proposeIn.GetExchanges())
	if err != nil {
		return err
	}

	s.local.cipherT, err = selectBest(order, SupportedCiphers, proposeIn.GetCiphers())
	if err != nil {
		return err
	}

	s.local.hashT, err = selectBest(order, SupportedHashes, proposeIn.GetHashes())
	if err != nil {
		return err
	}

	// we use the same params for both directions (must choose same curve)
	// WARNING: if they dont SelectBest the same way, this won't work...
	s.remote.curveT = s.local.curveT
	s.remote.cipherT = s.local.cipherT
	s.remote.hashT = s.local.hashT

	// log.Debugf("1.2 selection: exchange:%s cipher:%s hash:%s",
	// 	s.local.curveT, s.local.cipherT, s.local.hashT)

	// =============================================================================
	// step 2. Exchange -- exchange (signed) ephemeral keys. verify signatures.

	// Generate EphemeralPubKey
	var genSharedKey ci.GenSharedKey
	s.local.ephemeralPubKey, genSharedKey, err = ci.GenerateEKeyPair(s.local.curveT)

	// Gather corpus to sign.
	selectionOut := new(bytes.Buffer)
	selectionOut.Write(proposeOutBytes)
	selectionOut.Write(proposeInBytes)
	selectionOut.Write(s.local.ephemeralPubKey)
	selectionOutBytes := selectionOut.Bytes()

	// log.Debugf("2.0 exchange: %v", selectionOutBytes)
	exchangeOut := new(pb.Exchange)
	exchangeOut.Epubkey = s.local.ephemeralPubKey
	exchangeOut.Signature, err = s.localKey.Sign(selectionOutBytes)
	if err != nil {
		return err
	}

	// Send Propose packet (respects ctx)
	if _, err := writeMsgCtx(ctx, s.insecureM, exchangeOut); err != nil {
		return err
	}

	// Receive + Parse their Exchange packet.
	exchangeIn := new(pb.Exchange)
	if _, err := readMsgCtx(ctx, s.insecureM, exchangeIn); err != nil {
		return err
	}

	// =============================================================================
	// step 2.1. Verify -- verify their exchange packet is good.

	// get their ephemeral pub key
	s.remote.ephemeralPubKey = exchangeIn.GetEpubkey()

	selectionIn := new(bytes.Buffer)
	selectionIn.Write(proposeInBytes)
	selectionIn.Write(proposeOutBytes)
	selectionIn.Write(s.remote.ephemeralPubKey)
	selectionInBytes := selectionIn.Bytes()
	// log.Debugf("2.0.1 exchange recv: %v", selectionInBytes)

	// u.POut("Remote Peer Identified as %s\n", s.remote)
	sigOK, err := s.remote.permanentPubKey.Verify(selectionInBytes, exchangeIn.GetSignature())
	if err != nil {
		// log.Error("2.1 Verify: failed: %s", err)
		return err
	}

	if !sigOK {
		err := errors.New("Bad signature!")
		// log.Error("2.1 Verify: failed: %s", err)
		return err
	}
	// log.Debugf("2.1 Verify: signature verified.")

	// =============================================================================
	// step 2.2. Keys -- generate keys for mac + encryption

	// OK! seems like we're good to go.
	s.sharedSecret, err = genSharedKey(exchangeIn.GetEpubkey())
	if err != nil {
		return err
	}

	// generate two sets of keys (stretching)
	k1, k2 := ci.KeyStretcher(s.local.cipherT, s.local.hashT, s.sharedSecret)

	// use random nonces to decide order.
	switch {
	case order > 0:
		// just break
	case order < 0:
		k1, k2 = k2, k1 // swap
	default:
		// we should've bailed before this. but if not, bail here.
		return ErrEcho
	}
	s.local.keys = k1
	s.remote.keys = k2

	// log.Debug("2.2 keys:\n\tshared: %v\n\tk1: %v\n\tk2: %v",
	// 	s.sharedSecret, s.local.keys, s.remote.keys)

	// =============================================================================
	// step 2.3. MAC + Cipher -- prepare MAC + cipher

	if err := s.local.makeMacAndCipher(); err != nil {
		return err
	}

	if err := s.remote.makeMacAndCipher(); err != nil {
		return err
	}

	// log.Debug("2.3 mac + cipher.")

	// =============================================================================
	// step 3. Finish -- send expected message to verify encryption works (send local nonce)

	// setup ETM ReadWriter
	w := NewETMWriter(s.insecure, s.local.cipher, s.local.mac)
	r := NewETMReader(s.insecure, s.remote.cipher, s.remote.mac)
	s.secure = msgio.Combine(w, r).(msgio.ReadWriteCloser)

	// log.Debug("3.0 finish. sending: %v", proposeIn.GetRand())
	// send their Nonce.
	if _, err := s.secure.Write(proposeIn.GetRand()); err != nil {
		return fmt.Errorf("Failed to write Finish nonce: %s", err)
	}

	// read our Nonce
	nonceOut2 := make([]byte, len(nonceOut))
	if _, err := io.ReadFull(s.secure, nonceOut2); err != nil {
		return fmt.Errorf("Failed to read Finish nonce: %s", err)
	}

	// log.Debug("3.0 finish.\n\texpect: %v\n\tactual: %v", nonceOut, nonceOut2)
	if !bytes.Equal(nonceOut, nonceOut2) {
		return fmt.Errorf("Failed to read our encrypted nonce: %s != %s", nonceOut2, nonceOut)
	}

	// Whew! ok, that's all folks.
	return nil
}
