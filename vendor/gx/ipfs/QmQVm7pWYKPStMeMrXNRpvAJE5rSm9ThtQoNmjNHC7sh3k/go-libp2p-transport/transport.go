package transport

import (
	"context"
	"net"
	"time"

	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	manet "gx/ipfs/QmX3U3YXCQ6UYBxq2LVWF8dARS1hPUTEYLrSx654Qyxyw6/go-multiaddr-net"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

var log = logging.Logger("transport")

// Conn is an extension of the net.Conn interface that provides multiaddr
// information, and an accessor for the transport used to create the conn
type Conn interface {
	manet.Conn

	Transport() Transport
}

// Transport represents any device by which you can connect to and accept
// connections from other peers. The built-in transports provided are TCP and UTP
// but many more can be implemented, sctp, audio signals, sneakernet, UDT, a
// network of drones carrying usb flash drives, and so on.
type Transport interface {
	Dialer(laddr ma.Multiaddr, opts ...DialOpt) (Dialer, error)
	Listen(laddr ma.Multiaddr) (Listener, error)
	Matches(ma.Multiaddr) bool
}

// Dialer is an abstraction that is normally filled by an object containing
// information/options around how to perform the dial. An example would be
// setting TCP dial timeout for all dials made, or setting the local address
// that we dial out from.
type Dialer interface {
	Dial(raddr ma.Multiaddr) (Conn, error)
	DialContext(ctx context.Context, raddr ma.Multiaddr) (Conn, error)
	Matches(ma.Multiaddr) bool
}

// Listener is an interface closely resembling the net.Listener interface.  The
// only real difference is that Accept() returns Conn's of the type in this
// package, and also exposes a Multiaddr method as opposed to a regular Addr
// method
type Listener interface {
	Accept() (Conn, error)
	Close() error
	Addr() net.Addr
	Multiaddr() ma.Multiaddr
}

type ConnWrap struct {
	manet.Conn
	Tpt Transport
}

func (cw *ConnWrap) Transport() Transport {
	return cw.Tpt
}

// DialOpt is an option used for configuring dialer behaviour
type DialOpt interface{}

type TimeoutOpt time.Duration
type ReuseportOpt bool

var ReusePorts ReuseportOpt = true
