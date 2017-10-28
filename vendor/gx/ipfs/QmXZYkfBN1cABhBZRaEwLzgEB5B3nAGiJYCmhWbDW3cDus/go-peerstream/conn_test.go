package peerstream

import (
	"errors"
	"net"
	"sync"
	"testing"

	tpt "gx/ipfs/QmQVm7pWYKPStMeMrXNRpvAJE5rSm9ThtQoNmjNHC7sh3k/go-libp2p-transport"
	smux "gx/ipfs/QmY9JXR3FupnYAYJWK9aMr9bCpqWKcToQ1tz8DVGTrHpHw/go-stream-muxer"
)

type fakeconn struct {
	tpt.Conn
}

func (f *fakeconn) Close() error {
	return nil
}

var _ net.Conn = new(fakeconn)

func newFakeSmuxConn() *fakeSmuxConn {
	return &fakeSmuxConn{
		closed: make(chan struct{}),
	}
}

type fakeSmuxConn struct {
	closeLock sync.Mutex
	closed    chan struct{}
}

func (fsc *fakeSmuxConn) IsClosed() bool {
	select {
	case <-fsc.closed:
		return true
	default:
		return false
	}
}

// AcceptStream accepts a stream opened by the other side.
func (fsc *fakeSmuxConn) AcceptStream() (smux.Stream, error) {
	<-fsc.closed
	return nil, errors.New("connection closed")
}

func (fsc *fakeSmuxConn) OpenStream() (smux.Stream, error) {
	if fsc.IsClosed() {
		return nil, errors.New("connection closed")
	}
	return &fakeSmuxStream{conn: fsc, closed: make(chan struct{})}, nil
}

func (fsc *fakeSmuxConn) Close() error {
	fsc.closeLock.Lock()
	defer fsc.closeLock.Unlock()
	if fsc.IsClosed() {
		return errors.New("already closed")
	}
	close(fsc.closed)
	return nil
}

var _ smux.Conn = (*fakeSmuxConn)(nil)

func TestConnBasic(t *testing.T) {
	s := NewSwarm(fakeTransport{func(c net.Conn, isServer bool) (smux.Conn, error) {
		return newFakeSmuxConn(), nil
	}})
	nc := new(fakeconn)
	c, err := s.AddConn(nc)
	if err != nil {
		t.Fatal(err)
	}
	if c.Swarm() != s {
		t.Fatalf("incorrect swarm returned from connection")
	}
	if sc, ok := c.Conn().(*fakeSmuxConn); !ok {
		t.Fatalf("expected a fakeSmuxConn, got %v", sc)
	}

	if c.NetConn() != nc {
		t.Fatalf("expected %v, got %v", nc, c.NetConn())
	}
}

func TestConnsWithGroup(t *testing.T) {
	s := NewSwarm(nil)
	a := newConn(nil, newFakeSmuxConn(), s)
	b := newConn(nil, newFakeSmuxConn(), s)
	c := newConn(nil, newFakeSmuxConn(), s)
	g := "foo"

	b.Conn().Close()
	c.Conn().Close()

	s.conns[a] = struct{}{}
	s.conns[b] = struct{}{}
	s.conns[c] = struct{}{}

	a.AddGroup(g)
	b.AddGroup(g)
	c.AddGroup(g)

	conns := s.ConnsWithGroup(g)
	if len(conns) != 1 {
		t.Fatal("should have only gotten one")
	}
	groups := a.Groups()
	if len(groups) != 1 {
		t.Fatal("should have only gotten one")
	}
	if groups[0] != g {
		t.Fatalf("expected group '%v', got group '%v'", g, groups[0])
	}

	b.closingLock.Lock()
	defer b.closingLock.Unlock()
	c.closingLock.Lock()
	defer c.closingLock.Unlock()
	if !b.closing {
		t.Fatal("b should at least be closing")
	}

	if !c.closing {
		t.Fatal("c should at least be closing")
	}
}

func TestConnIdx(t *testing.T) {
	s := NewSwarm(nil)
	c, err := s.AddConn(new(fakeconn))
	if err != nil {
		t.Fatal(err)
	}

	g := "foo"
	g2 := "bar"

	if len(s.ConnsWithGroup(g)) != 0 {
		t.Fatal("should have gotten none")
	}

	c.AddGroup(g)
	if !c.InGroup(g) {
		t.Fatal("should be in the appropriate group")
	}
	if len(s.ConnsWithGroup(g)) != 1 {
		t.Fatal("should have only gotten one")
	}

	c.Close()
	if !c.InGroup(g) {
		t.Fatal("should still be in the appropriate group")
	}
	if len(s.ConnsWithGroup(g)) != 0 {
		t.Fatal("should have gotten none")
	}

	c.AddGroup(g2)
	if !c.InGroup(g2) {
		t.Fatal("should now be in group 2")
	}
	if c.InGroup("bla") {
		t.Fatal("should not be in arbitrary groups")
	}
	if len(s.ConnsWithGroup(g)) != 0 {
		t.Fatal("should still have gotten none")
	}
	if len(s.ConnsWithGroup(g2)) != 0 {
		t.Fatal("should still have gotten none")
	}
	if len(s.connIdx) != 0 {
		t.Fatal("should have an empty index")
	}
	if len(s.conns) != 0 {
		t.Fatal("should not be holding any connections")
	}
}

func TestAddConnWithGroups(t *testing.T) {
	s := NewSwarm(nil)

	g := "foo"
	g2 := "bar"
	g3 := "baz"

	c, err := s.AddConn(new(fakeconn), g, g2)
	if !c.InGroup(g) || !c.InGroup(g2) || c.InGroup(g3) {
		t.Fatal("should be in the appropriate groups")
	}
	if err != nil {
		t.Fatal(err)
	}

	if len(s.ConnsWithGroup(g)) != 1 {
		t.Fatal("should have gotten one")
	}

	if len(s.ConnsWithGroup(g2)) != 1 {
		t.Fatal("should have gotten one")
	}

	if len(s.ConnsWithGroup(g3)) != 0 {
		t.Fatal("should have gotten none")
	}

	c.Close()
	if len(s.ConnsWithGroup(g)) != 0 {
		t.Fatal("should have gotten none")
	}

	if len(s.ConnsWithGroup(g2)) != 0 {
		t.Fatal("should have gotten none")
	}

	if len(s.ConnsWithGroup(g3)) != 0 {
		t.Fatal("should still have gotten none")
	}

	if len(s.connIdx) != 0 {
		t.Fatal("should have an empty index")
	}
	if len(s.conns) != 0 {
		t.Fatal("should not be holding any connections")
	}
}
