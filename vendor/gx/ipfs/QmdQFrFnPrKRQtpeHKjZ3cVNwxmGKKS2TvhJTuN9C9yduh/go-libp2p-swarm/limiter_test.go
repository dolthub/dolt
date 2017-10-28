package swarm

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	mafmt "gx/ipfs/QmZQa5J7j7kd44GGC4aKX8J9JGGzCMqwGzcEFqGV1YD57A/mafmt"
	iconn "gx/ipfs/QmfQAY7YU4fQi3sjGLs1hwkM2Aq7dxgDyoMjaKN4WBWvcB/go-libp2p-interface-conn"
)

func mustAddr(t *testing.T, s string) ma.Multiaddr {
	a, err := ma.NewMultiaddr(s)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func addrWithPort(t *testing.T, p int) ma.Multiaddr {
	return mustAddr(t, fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", p))
}

// in these tests I use addresses with tcp ports over a certain number to
// signify 'good' addresses that will succeed, and addresses below that number
// will fail. This lets us more easily test these different scenarios.
func tcpPortOver(a ma.Multiaddr, n int) bool {
	port, err := a.ValueForProtocol(ma.P_TCP)
	if err != nil {
		panic(err)
	}

	pnum, err := strconv.Atoi(port)
	if err != nil {
		panic(err)
	}

	return pnum > n
}

func tryDialAddrs(ctx context.Context, l *dialLimiter, p peer.ID, addrs []ma.Multiaddr, res chan dialResult) {
	for _, a := range addrs {
		l.AddDialJob(&dialJob{
			ctx:  ctx,
			peer: p,
			addr: a,
			resp: res,
		})
	}
}

func hangDialFunc(hang chan struct{}) dialfunc {
	return func(ctx context.Context, p peer.ID, a ma.Multiaddr) (iconn.Conn, error) {
		if mafmt.UTP.Matches(a) {
			return iconn.Conn(nil), nil
		}

		if tcpPortOver(a, 10) {
			return iconn.Conn(nil), nil
		}

		<-hang
		return nil, fmt.Errorf("test bad dial")
	}
}

func TestLimiterBasicDials(t *testing.T) {
	hang := make(chan struct{})
	defer close(hang)

	l := newDialLimiterWithParams(hangDialFunc(hang), concurrentFdDials, 4)

	bads := []ma.Multiaddr{addrWithPort(t, 1), addrWithPort(t, 2), addrWithPort(t, 3), addrWithPort(t, 4)}
	good := addrWithPort(t, 20)

	resch := make(chan dialResult)
	pid := peer.ID("testpeer")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tryDialAddrs(ctx, l, pid, bads, resch)

	l.AddDialJob(&dialJob{
		ctx:  ctx,
		peer: pid,
		addr: good,
		resp: resch,
	})

	select {
	case <-resch:
		t.Fatal("no dials should have completed!")
	case <-time.After(time.Millisecond * 100):
	}

	// complete a single hung dial
	hang <- struct{}{}

	select {
	case r := <-resch:
		if r.Err == nil {
			t.Fatal("should have gotten failed dial result")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dial completion")
	}

	select {
	case r := <-resch:
		if r.Err != nil {
			t.Fatal("expected second result to be success!")
		}
	case <-time.After(time.Second):
	}
}

func TestFDLimiting(t *testing.T) {
	hang := make(chan struct{})
	defer close(hang)
	l := newDialLimiterWithParams(hangDialFunc(hang), 16, 5)

	bads := []ma.Multiaddr{addrWithPort(t, 1), addrWithPort(t, 2), addrWithPort(t, 3), addrWithPort(t, 4)}
	pids := []peer.ID{"testpeer1", "testpeer2", "testpeer3", "testpeer4"}
	goodTCP := addrWithPort(t, 20)

	ctx := context.Background()
	resch := make(chan dialResult)

	// take all fd limit tokens with hang dials
	for _, pid := range pids {
		tryDialAddrs(ctx, l, pid, bads, resch)
	}

	// these dials should work normally, but will hang because we have taken
	// up all the fd limiting
	for _, pid := range pids {
		l.AddDialJob(&dialJob{
			ctx:  ctx,
			peer: pid,
			addr: goodTCP,
			resp: resch,
		})
	}

	select {
	case <-resch:
		t.Fatal("no dials should have completed!")
	case <-time.After(time.Millisecond * 100):
	}

	pid5 := peer.ID("testpeer5")
	utpaddr := mustAddr(t, "/ip4/127.0.0.1/udp/7777/utp")

	// This should complete immediately since utp addresses arent blocked by fd rate limiting
	l.AddDialJob(&dialJob{ctx: ctx, peer: pid5, addr: utpaddr, resp: resch})

	select {
	case res := <-resch:
		if res.Err != nil {
			t.Fatal("should have gotten successful response")
		}
	case <-time.After(time.Second * 5):
		t.Fatal("timeout waiting for utp addr success")
	}
}

func TestTokenRedistribution(t *testing.T) {
	var lk sync.Mutex
	hangchs := make(map[peer.ID]chan struct{})
	df := func(ctx context.Context, p peer.ID, a ma.Multiaddr) (iconn.Conn, error) {
		if tcpPortOver(a, 10) {
			return (iconn.Conn)(nil), nil
		}

		lk.Lock()
		ch := hangchs[p]
		lk.Unlock()
		<-ch
		return nil, fmt.Errorf("test bad dial")
	}
	l := newDialLimiterWithParams(df, 8, 4)

	bads := []ma.Multiaddr{addrWithPort(t, 1), addrWithPort(t, 2), addrWithPort(t, 3), addrWithPort(t, 4)}
	pids := []peer.ID{"testpeer1", "testpeer2"}

	ctx := context.Background()
	resch := make(chan dialResult)

	// take all fd limit tokens with hang dials
	for _, pid := range pids {
		hangchs[pid] = make(chan struct{})
	}

	for _, pid := range pids {
		tryDialAddrs(ctx, l, pid, bads, resch)
	}

	good := mustAddr(t, "/ip4/127.0.0.1/tcp/1001")

	// add a good dial job for peer 1
	l.AddDialJob(&dialJob{
		ctx:  ctx,
		peer: pids[1],
		addr: good,
		resp: resch,
	})

	select {
	case <-resch:
		t.Fatal("no dials should have completed!")
	case <-time.After(time.Millisecond * 100):
	}

	// unblock one dial for peer 0
	hangchs[pids[0]] <- struct{}{}

	select {
	case res := <-resch:
		if res.Err == nil {
			t.Fatal("should have only been a failure here")
		}
	case <-time.After(time.Millisecond * 100):
		t.Fatal("expected a dial failure here")
	}

	select {
	case <-resch:
		t.Fatal("no more dials should have completed!")
	case <-time.After(time.Millisecond * 100):
	}

	// add a bad dial job to peer 0 to fill their rate limiter
	// and test that more dials for this peer won't interfere with peer 1's successful dial incoming
	l.AddDialJob(&dialJob{
		ctx:  ctx,
		peer: pids[0],
		addr: addrWithPort(t, 7),
		resp: resch,
	})

	hangchs[pids[1]] <- struct{}{}

	// now one failed dial from peer 1 should get through and fail
	// which will in turn unblock the successful dial on peer 1
	select {
	case res := <-resch:
		if res.Err == nil {
			t.Fatal("should have only been a failure here")
		}
	case <-time.After(time.Millisecond * 100):
		t.Fatal("expected a dial failure here")
	}

	select {
	case res := <-resch:
		if res.Err != nil {
			t.Fatal("should have succeeded!")
		}
	case <-time.After(time.Millisecond * 100):
		t.Fatal("should have gotten successful dial")
	}
}

func TestStressLimiter(t *testing.T) {
	df := func(ctx context.Context, p peer.ID, a ma.Multiaddr) (iconn.Conn, error) {
		if tcpPortOver(a, 1000) {
			return iconn.Conn(nil), nil
		}

		time.Sleep(time.Millisecond * time.Duration(5+rand.Intn(100)))
		return nil, fmt.Errorf("test bad dial")
	}

	l := newDialLimiterWithParams(df, 20, 5)

	var bads []ma.Multiaddr
	for i := 0; i < 100; i++ {
		bads = append(bads, addrWithPort(t, i))
	}

	addresses := append(bads, addrWithPort(t, 2000))
	success := make(chan struct{})

	for i := 0; i < 20; i++ {
		go func(id peer.ID) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			resp := make(chan dialResult)
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			for _, i := range rand.Perm(len(addresses)) {
				l.AddDialJob(&dialJob{
					addr: addresses[i],
					ctx:  ctx,
					peer: id,
					resp: resp,
				})
			}

			for res := range resp {
				if res.Err == nil {
					success <- struct{}{}
					return
				}
			}
		}(peer.ID(fmt.Sprintf("testpeer%d", i)))
	}

	for i := 0; i < 20; i++ {
		select {
		case <-success:
		case <-time.After(time.Second * 5):
			t.Fatal("expected a success within five seconds")
		}
	}
}

func TestFDLimitUnderflow(t *testing.T) {
	dials := 0

	df := func(ctx context.Context, p peer.ID, a ma.Multiaddr) (iconn.Conn, error) {
		dials++

		timeout := make(chan bool, 1)
		go func() {
			time.Sleep(time.Second * 5)
			timeout <- true
		}()

		select {
		case <-ctx.Done():
		case <-timeout:
		}

		return nil, fmt.Errorf("df timed out")
	}

	l := newDialLimiterWithParams(df, 20, 3)

	var addrs []ma.Multiaddr
	for i := 0; i <= 1000; i++ {
		addrs = append(addrs, addrWithPort(t, i))
	}

	for i := 0; i < 1000; i++ {
		go func(id peer.ID, i int) {
			ctx, cancel := context.WithCancel(context.Background())

			resp := make(chan dialResult)
			l.AddDialJob(&dialJob{
				addr: addrs[i],
				ctx:  ctx,
				peer: id,
				resp: resp,
			})

			//cancel first 60 after 1s, next 60 after 2s
			if i > 60 {
				time.Sleep(time.Second * 1)
			}
			if i < 120 {
				time.Sleep(time.Second * 1)
				cancel()
				return
			}
			defer cancel()

			for res := range resp {
				if res.Err != nil {
					return
				}
				t.Fatal("got dial res, shouldn't")
			}
		}(peer.ID(fmt.Sprintf("testpeer%d", i%20)), i)
	}

	time.Sleep(time.Second * 3)

	if l.fdConsuming < 0 {
		t.Fatalf("l.fdConsuming < 0")
	}
}
