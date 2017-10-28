package peerstream

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	smux "gx/ipfs/QmY9JXR3FupnYAYJWK9aMr9bCpqWKcToQ1tz8DVGTrHpHw/go-stream-muxer"
)

type fakeTransport struct {
	f func(c net.Conn, isServer bool) (smux.Conn, error)
}

func (f fakeTransport) NewConn(c net.Conn, isServer bool) (smux.Conn, error) {
	return (f.f)(c, isServer)
}

type myNotifee struct {
	conns  map[*Conn]bool
	failed bool
}

func (mn *myNotifee) Connected(c *Conn) {
	_, ok := mn.conns[c]
	if ok {
		fmt.Println("got connected notif for already connected peer")
		mn.failed = true
		return
	}

	mn.conns[c] = true
	time.Sleep(time.Millisecond * 5)
}

func (mn *myNotifee) Disconnected(c *Conn) {
	_, ok := mn.conns[c]
	if !ok {
		fmt.Println("got disconnected notif for unknown peer")
		mn.failed = true
		return
	}

	delete(mn.conns, c)
}

func (mn *myNotifee) OpenedStream(*Stream) {}
func (mn *myNotifee) ClosedStream(*Stream) {}

func TestNotificationOrdering(t *testing.T) {
	s := NewSwarm(nil)
	notifiee := &myNotifee{conns: make(map[*Conn]bool)}

	s.Notify(notifiee)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				nc := new(fakeconn)
				c, err := s.AddConn(nc)
				if err != nil {
					t.Error(err)
				}
				c.Close()
			}

		}()
	}

	wg.Wait()
	if notifiee.failed {
		t.Fatal("we've got problems")
	}
}

func TestBasicSwarm(t *testing.T) {
	s := NewSwarm(fakeTransport{func(c net.Conn, isServer bool) (smux.Conn, error) {
		return newFakeSmuxConn(), nil
	}})
	c, err := s.AddConn(new(fakeconn), "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	if !c.InGroup("foo") || !c.InGroup("bar") || c.InGroup("baz") {
		t.Fatal("conn should be in groups bar and baz")
	}
	conns := s.Conns()
	connsInGroup := s.ConnsWithGroup("bar")
	if len(conns) != 1 || len(connsInGroup) != 1 {
		t.Fatal("expected one conn")
	}
	if conns[0] != c || connsInGroup[0] != c {
		t.Fatal("expected our conn")
	}

	st, err := c.NewStream()
	if err != nil {
		t.Fatal(err)
	}
	if !st.InGroup("foo") || !st.InGroup("bar") || st.InGroup("baz") {
		t.Fatal("stream should be in groups bar and baz")
	}
	streams := s.Streams()
	streamsInGroup := s.StreamsWithGroup("bar")
	if len(streams) != 1 || len(streamsInGroup) != 1 {
		t.Fatal("expected one stream")
	}
	if streams[0] != st || streamsInGroup[0] != st {
		t.Fatal("expected our stream")
	}

	// Make sure these don't crash or deadlock.
	s.String()
	s.String()
	s.Dump()
	s.Dump()

	if s.String() == "" {
		t.Fatal("got no 'String' from swarm")
	}

	if s.Dump() == "" {
		t.Fatal("got no 'Dump' from swarm")
	}
}
