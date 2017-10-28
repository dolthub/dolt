package peerstream

import (
	"errors"
	"fmt"
	"net"
	"sync"

	tpt "gx/ipfs/QmQVm7pWYKPStMeMrXNRpvAJE5rSm9ThtQoNmjNHC7sh3k/go-libp2p-transport"
	smux "gx/ipfs/QmY9JXR3FupnYAYJWK9aMr9bCpqWKcToQ1tz8DVGTrHpHw/go-stream-muxer"
)

// ConnHandler is a function which receives a Conn. It allows
// clients to set a function to receive newly accepted
// connections. It works like StreamHandler, but is usually
// less useful than usual as most services will only use
// Streams. It is safe to pass or store the *Conn elsewhere.
// Note: the ConnHandler is called sequentially, so spawn
// goroutines or pass the Conn. See EchoHandler.
type ConnHandler func(s *Conn)

// SelectConn selects a connection out of list. It allows
// delegation of decision making to clients. Clients can
// make SelectConn functons that check things connection
// qualities -- like latency andbandwidth -- or pick from
// a logical set of connections.
type SelectConn func([]*Conn) *Conn

// ErrInvalidConnSelected signals that a connection selected
// with a SelectConn function is invalid. This may be due to
// the Conn not being part of the original set given to the
// function, or the value being nil.
var ErrInvalidConnSelected = errors.New("invalid selected connection")

// ErrNoConnections signals that no connections are available
var ErrNoConnections = errors.New("no connections")

// Conn is a Swarm-associated connection.
type Conn struct {
	smuxConn smux.Conn
	netConn  tpt.Conn // underlying connection

	swarm  *Swarm
	groups groupSet

	streams    map[*Stream]struct{}
	streamLock sync.RWMutex

	closed    bool
	closeLock sync.Mutex

	closing     bool
	closingLock sync.Mutex
}

func newConn(nconn tpt.Conn, tconn smux.Conn, s *Swarm) *Conn {
	return &Conn{
		netConn:  nconn,
		smuxConn: tconn,
		swarm:    s,
		groups:   groupSet{m: make(map[Group]struct{})},
		streams:  make(map[*Stream]struct{}),
	}
}

// String returns a string representation of the Conn.
func (c *Conn) String() string {
	c.streamLock.RLock()
	ls := len(c.streams)
	c.streamLock.RUnlock()
	f := "<peerstream.Conn %d streams %s <--> %s>"
	return fmt.Sprintf(f, ls, c.netConn.LocalAddr(), c.netConn.RemoteAddr())
}

// Swarm returns the Swarm associated with this Conn.
func (c *Conn) Swarm() *Swarm {
	return c.swarm
}

// NetConn returns the underlying net.Conn.
func (c *Conn) NetConn() net.Conn {
	return c.netConn
}

// Conn returns the underlying transport Connection we use
// Warning: modifying this object is undefined.
func (c *Conn) Conn() smux.Conn {
	return c.smuxConn
}

// Groups returns the Groups this Conn belongs to.
func (c *Conn) Groups() []Group {
	return c.groups.Groups()
}

// InGroup returns whether this Conn belongs to a Group.
func (c *Conn) InGroup(g Group) bool {
	return c.groups.Has(g)
}

// AddGroup assigns given Group to Conn.
func (c *Conn) AddGroup(g Group) {
	c.groups.Add(g)
}

// NewStream returns a stream associated with this Conn.
func (c *Conn) NewStream() (*Stream, error) {
	return c.swarm.NewStreamWithConn(c)
}

// Streams returns the slice of all streams associated to this Conn.
func (c *Conn) Streams() []*Stream {
	c.streamLock.RLock()
	defer c.streamLock.RUnlock()

	streams := make([]*Stream, 0, len(c.streams))
	for s := range c.streams {
		streams = append(streams, s)
	}
	return streams
}

// GoClose spawns off a goroutine to close the connection iff the connection is
// not already being closed and returns immediately
func (c *Conn) GoClose() {
	c.closingLock.Lock()
	defer c.closingLock.Unlock()
	if c.closing {
		return
	}
	c.closing = true

	go c.Close()
}

// Close closes this connection
func (c *Conn) Close() error {
	c.closeLock.Lock()
	defer c.closeLock.Unlock()
	if c.closed == true {
		return nil
	}

	c.closingLock.Lock()
	c.closing = true
	c.closingLock.Unlock()

	c.closed = true

	// reset streams
	streams := c.Streams()
	for _, s := range streams {
		s.Reset()
	}

	// close underlying connection
	c.swarm.removeConn(c)
	var err error
	if c.smuxConn != nil {
		err = c.smuxConn.Close()
	} else {
		err = c.netConn.Close()
	}
	c.swarm.notifyAll(func(n Notifiee) {
		n.Disconnected(c)
	})
	return err
}

// ConnsWithGroup narrows down a set of connections to those in a given group.
func ConnsWithGroup(g Group, conns []*Conn) []*Conn {
	var out []*Conn
	for _, c := range conns {
		if c.InGroup(g) {
			out = append(out, c)
		}
	}
	return out
}

// ConnInConns returns true if a connection belongs to the
// conns slice.
func ConnInConns(c1 *Conn, conns []*Conn) bool {
	for _, c2 := range conns {
		if c2 == c1 {
			return true
		}
	}
	return false
}

// ------------------------------------------------------------------
// All the connection setup logic here, in one place.
// these are mostly *Swarm methods, but i wanted a less-crowded place
// for them.
// ------------------------------------------------------------------

// addConn is the internal version of AddConn. we need the server bool
// as spdystream requires it.
func (s *Swarm) addConn(netConn tpt.Conn, isServer bool) (*Conn, error) {
	c, err := s.setupConn(netConn, isServer)
	if err != nil {
		return nil, err
	}

	s.ConnHandler()(c)

	if c.smuxConn != nil {
		// go listen for incoming streams on this connection
		go func() {
			for {
				str, err := c.smuxConn.AcceptStream()
				if err != nil {
					break
				}
				go func() {
					stream := s.setupStream(str, c)
					s.StreamHandler()(stream) // call our handler
				}()
			}
		}()
	}

	s.notifyAll(func(n Notifiee) {
		n.Connected(c)
	})
	return c, nil
}

// setupConn adds the relevant connection to the map, first checking if it
// was already there.
func (s *Swarm) setupConn(netConn tpt.Conn, isServer bool) (*Conn, error) {
	if netConn == nil {
		return nil, errors.New("nil conn")
	}

	// first, check if we already have it, to avoid constructing it
	// if it is already there
	s.connLock.Lock()
	for c := range s.conns {
		if c.netConn == netConn {
			s.connLock.Unlock()
			return c, nil
		}
	}
	s.connLock.Unlock()
	// construct the connection without hanging onto the lock
	// (as there could be deadlock if so.)

	var ssConn smux.Conn
	if s.transport != nil {
		// create a new stream muxer connection
		c, err := s.transport.NewConn(netConn, isServer)
		if err != nil {
			return nil, err
		}

		ssConn = c
	}

	// take the lock to add it to the map.
	s.connLock.Lock()
	defer s.connLock.Unlock()

	// check for it again as it may have been added already. (TOCTTOU)
	for c := range s.conns {
		if c.netConn == netConn {
			return c, nil
		}
	}

	// add the connection
	c := newConn(netConn, ssConn, s)
	s.conns[c] = struct{}{}
	return c, nil
}

// createStream is the internal function that creates a new stream. assumes
// all validation has happened.
func (s *Swarm) createStream(c *Conn) (*Stream, error) {

	// Create a new smux.Stream
	smuxStream, err := c.smuxConn.OpenStream()
	if err != nil {
		return nil, err
	}

	return s.setupStream(smuxStream, c), nil
}

// newStream is the internal function that creates a new stream. assumes
// all validation has happened.
func (s *Swarm) setupStream(smuxStream smux.Stream, c *Conn) *Stream {

	// create a new stream
	stream := newStream(smuxStream, c)

	// add it to our streams maps
	s.streamLock.Lock()
	c.streamLock.Lock()
	s.streams[stream] = struct{}{}
	c.streams[stream] = struct{}{}
	s.streamLock.Unlock()
	c.streamLock.Unlock()

	s.notifyAll(func(n Notifiee) {
		n.OpenedStream(stream)
	})
	return stream
}

// TODO: Really, we need to either not track them here or, possibly, add a
// notification system to go-stream-muxer (shudder).
// Alternatively, we could garbage collect them like we do connections but then
// we'd need a way to determine which connections are open (we'd need IsClosed)
// methods.
func (s *Swarm) removeStream(stream *Stream, reset bool) error {
	// remove from our maps
	s.streamLock.Lock()
	_, isOpen := s.streams[stream]
	if isOpen {
		stream.conn.streamLock.Lock()
		delete(s.streams, stream)
		delete(stream.conn.streams, stream)
		stream.conn.streamLock.Unlock()
	}
	s.streamLock.Unlock()

	var err error
	if reset {
		err = stream.smuxStream.Reset()
	} else {
		err = stream.smuxStream.Close()
	}
	if isOpen {
		s.notifyAll(func(n Notifiee) {
			n.ClosedStream(stream)
		})
	}
	return err
}

func (s *Swarm) removeConn(conn *Conn) {
	// remove from our maps
	s.connLock.Lock()
	delete(s.conns, conn)
	s.connLock.Unlock()
}
