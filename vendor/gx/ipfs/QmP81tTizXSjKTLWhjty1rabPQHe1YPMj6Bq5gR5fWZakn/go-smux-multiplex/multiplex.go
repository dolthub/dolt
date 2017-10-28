package peerstream_multiplex

import (
	"net"

	smux "gx/ipfs/QmY9JXR3FupnYAYJWK9aMr9bCpqWKcToQ1tz8DVGTrHpHw/go-stream-muxer"
	mp "gx/ipfs/Qmd7RwckBhtLfLCNDLgUrxmi6qs4Ldxjb6qwX7XmKZ8VCm/go-multiplex" // Conn is a connection to a remote peer.
)

type conn struct {
	*mp.Multiplex
}

func (c *conn) Close() error {
	return c.Multiplex.Close()
}

func (c *conn) IsClosed() bool {
	return c.Multiplex.IsClosed()
}

// OpenStream creates a new stream.
func (c *conn) OpenStream() (smux.Stream, error) {
	return c.Multiplex.NewStream()
}

// AcceptStream accepts a stream opened by the other side.
func (c *conn) AcceptStream() (smux.Stream, error) {
	return c.Multiplex.Accept()
}

// Transport is a go-peerstream transport that constructs
// multiplex-backed connections.
type Transport struct{}

// DefaultTransport has default settings for multiplex
var DefaultTransport = &Transport{}

func (t *Transport) NewConn(nc net.Conn, isServer bool) (smux.Conn, error) {
	return &conn{mp.NewMultiplex(nc, isServer)}, nil
}
