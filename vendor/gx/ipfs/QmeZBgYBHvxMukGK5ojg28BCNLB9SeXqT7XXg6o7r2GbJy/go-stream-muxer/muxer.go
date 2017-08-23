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

	SetDeadline(time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

// StreamHandler is a function that handles streams
// (usually those opened by the remote side)
type StreamHandler func(Stream)

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

	// Serve starts a loop, accepting incoming requests and calling
	// `StreamHandler with them. (Use _instead of_ accept. not both.)
	Serve(StreamHandler)
}

// Transport constructs go-stream-muxer compatible connections.
type Transport interface {

	// NewConn constructs a new connection
	NewConn(c net.Conn, isServer bool) (Conn, error)
}
