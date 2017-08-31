package peerstream_multiplex

import (
	"errors"
	"net"

	mp "gx/ipfs/QmWmMyq4ZG5yHhQkySSHfEVVUAnEJfxhTjhHZeGchmNe23/go-multiplex" // Conn is a connection to a remote peer.
	smux "gx/ipfs/QmeZBgYBHvxMukGK5ojg28BCNLB9SeXqT7XXg6o7r2GbJy/go-stream-muxer"
)

var ErrUseServe = errors.New("not implemented, use Serve")

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

// Serve starts listening for incoming requests and handles them
// using given StreamHandler
func (c *conn) Serve(handler smux.StreamHandler) {
	for {
		s, err := c.AcceptStream()
		if err != nil {
			return
		}
		go handler(s)
	}
}

// Transport is a go-peerstream transport that constructs
// multiplex-backed connections.
type Transport struct{}

// DefaultTransport has default settings for multiplex
var DefaultTransport = &Transport{}

func (t *Transport) NewConn(nc net.Conn, isServer bool) (smux.Conn, error) {
	return &conn{mp.NewMultiplex(nc, isServer)}, nil
}
