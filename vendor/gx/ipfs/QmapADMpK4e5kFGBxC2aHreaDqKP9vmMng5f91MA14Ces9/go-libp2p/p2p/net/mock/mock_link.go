package mocknet

import (
	//	"fmt"
	"net"
	"sync"
	"time"

	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	inet "gx/ipfs/QmahYsGWry85Y7WUe2SX5G4JkH2zifEQAUtJVLZ24aC9DF/go-libp2p-net"
)

// link implements mocknet.Link
// and, for simplicity, inet.Conn
type link struct {
	mock        *mocknet
	nets        []*peernet
	opts        LinkOptions
	ratelimiter *ratelimiter
	// this could have addresses on both sides.

	sync.RWMutex
}

func newLink(mn *mocknet, opts LinkOptions) *link {
	l := &link{mock: mn,
		opts:        opts,
		ratelimiter: NewRatelimiter(opts.Bandwidth)}
	return l
}

func (l *link) newConnPair(dialer *peernet) (*conn, *conn) {
	l.RLock()
	defer l.RUnlock()

	c1 := newConn(l.nets[0], l.nets[1], l)
	c2 := newConn(l.nets[1], l.nets[0], l)
	c1.rconn = c2
	c2.rconn = c1

	if dialer == c1.net {
		return c1, c2
	}
	return c2, c1
}

func (l *link) newStreamPair() (*stream, *stream) {
	a, b := net.Pipe()

	s1 := NewStream(a)
	s2 := NewStream(b)
	return s1, s2
}

func (l *link) Networks() []inet.Network {
	l.RLock()
	defer l.RUnlock()

	cp := make([]inet.Network, len(l.nets))
	for i, n := range l.nets {
		cp[i] = n
	}
	return cp
}

func (l *link) Peers() []peer.ID {
	l.RLock()
	defer l.RUnlock()

	cp := make([]peer.ID, len(l.nets))
	for i, n := range l.nets {
		cp[i] = n.peer
	}
	return cp
}

func (l *link) SetOptions(o LinkOptions) {
	l.opts = o
	l.ratelimiter.UpdateBandwidth(l.opts.Bandwidth)
}

func (l *link) Options() LinkOptions {
	return l.opts
}

func (l *link) GetLatency() time.Duration {
	return l.opts.Latency
}

func (l *link) RateLimit(dataSize int) time.Duration {
	return l.ratelimiter.Limit(dataSize)
}
