// Package secio is used to encrypt `go-libp2p-conn` connections. Connections wrapped by secio use secure sessions provided by this package to encrypt all traffic. A TLS-like handshake is used to setup the communication channel.
package secio

import (
	"context"
	"io"

	msgio "gx/ipfs/QmRQhVisS8dmPbjBUthVkenn81pBxrx1GxE281csJhm2vL/go-msgio"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ci "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
)

// SessionGenerator constructs secure communication sessions for a peer.
type SessionGenerator struct {
	LocalID    peer.ID
	PrivateKey ci.PrivKey
}

// NewSession takes an insecure io.ReadWriter, sets up a TLS-like
// handshake with the other side, and returns a secure session.
// The handshake isn't run until the connection is read or written to.
// See the source for the protocol details and security implementation.
// The provided Context is only needed for the duration of this function.
func (sg *SessionGenerator) NewSession(ctx context.Context, insecure io.ReadWriteCloser) (Session, error) {
	return newSecureSession(ctx, sg.LocalID, sg.PrivateKey, insecure)
}

// Session provides the necessary functionality to wrap a connection
// and tunnel it through a secure channel via the provided ReadWriter.
type Session interface {
	// ReadWriter returns the encrypted communication channel
	ReadWriter() msgio.ReadWriteCloser

	// LocalPeer retrieves the local peer.
	LocalPeer() peer.ID

	// LocalPrivateKey retrieves the local private key
	LocalPrivateKey() ci.PrivKey

	// RemotePeer retrieves the remote peer.
	RemotePeer() peer.ID

	// RemotePublicKey retrieves the remote's public key
	// which was received during the handshake.
	RemotePublicKey() ci.PubKey

	// Close closes the secure session
	Close() error
}

// ReadWriter returns the encrypted communication channel
func (s *secureSession) ReadWriter() msgio.ReadWriteCloser {
	if err := s.Handshake(); err != nil {
		return &closedRW{err}
	}
	return s.secure
}

// LocalPeer retrieves the local peer.
func (s *secureSession) LocalPeer() peer.ID {
	return s.localPeer
}

// LocalPrivateKey retrieves the local peer's PrivateKey
func (s *secureSession) LocalPrivateKey() ci.PrivKey {
	return s.localKey
}

// RemotePeer retrieves the remote peer.
func (s *secureSession) RemotePeer() peer.ID {
	if err := s.Handshake(); err != nil {
		return ""
	}
	return s.remotePeer
}

// RemotePublicKey retrieves the remote public key.
func (s *secureSession) RemotePublicKey() ci.PubKey {
	if err := s.Handshake(); err != nil {
		return nil
	}
	return s.remote.permanentPubKey
}

// Close closes the secure session
func (s *secureSession) Close() error {
	s.handshakeMu.Lock()
	defer s.handshakeMu.Unlock()
	if s.secure == nil {
		return s.insecure.Close() // hadn't secured yet.
	}
	return s.secure.Close()
}

// closedRW implements a stub msgio interface that's already
// closed and errored.
type closedRW struct {
	err error
}

func (c *closedRW) Read(buf []byte) (int, error) {
	return 0, c.err
}

func (c *closedRW) Write(buf []byte) (int, error) {
	return 0, c.err
}

func (c *closedRW) NextMsgLen() (int, error) {
	return 0, c.err
}

func (c *closedRW) ReadMsg() ([]byte, error) {
	return nil, c.err
}

func (c *closedRW) WriteMsg(buf []byte) error {
	return c.err
}

func (c *closedRW) Close() error {
	return c.err
}

func (c *closedRW) ReleaseMsg(m []byte) {
}
