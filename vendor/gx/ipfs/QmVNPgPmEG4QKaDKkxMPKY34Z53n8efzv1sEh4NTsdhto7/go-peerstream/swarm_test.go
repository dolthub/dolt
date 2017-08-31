package peerstream

import (
	"fmt"
	"sync"
	"testing"
	"time"

	tpt "gx/ipfs/QmQVm7pWYKPStMeMrXNRpvAJE5rSm9ThtQoNmjNHC7sh3k/go-libp2p-transport"
)

type fakeconn struct {
	tpt.Conn
}

func (f *fakeconn) Close() error {
	return nil
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
