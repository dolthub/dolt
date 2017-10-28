package streammux

import (
	"io"
	"net"
	"time"
)

// Stream is a bidirectional io pipe within a connection
type Stream interface {
	io.Reader
	io.Writer
	io.Closer

	Reset() error

	SetDeadline(time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

// NoOpHandler do nothing. close streams as soon as they are opened.
var NoOpHandler = func(s Stream) { s.Close() }

// Conn is a stream-multiplexing connection to a remote peer.
type Conn interface {
	io.Closer

	// IsClosed returns whether a connection is fully closed, so it can
	// be garbage collected.
	IsClosed() bool

	// OpenStream creates a new stream.
	OpenStream() (Stream, error)

	// AcceptStream accepts a stream opened by the other side.
	AcceptStream() (Stream, error)
}

// Transport constructs go-stream-muxer compatible connections.
type Transport interface {

	// NewConn constructs a new connection
	NewConn(c net.Conn, isServer bool) (Conn, error)
}
