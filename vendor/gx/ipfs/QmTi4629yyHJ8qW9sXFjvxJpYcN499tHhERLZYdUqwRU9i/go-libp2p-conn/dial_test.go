package conn

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	transport "gx/ipfs/QmQVm7pWYKPStMeMrXNRpvAJE5rSm9ThtQoNmjNHC7sh3k/go-libp2p-transport"
	ipnet "gx/ipfs/QmQq9YzmdFdWNTDdArueGyD7L5yyiRQigrRHJnTGkxcEjT/go-libp2p-interface-pnet"
	grc "gx/ipfs/QmTd4Jgb4nbJq5uR55KJgGLyHWmM3dovS21D1HcwRneSLu/gorocheck"
	msmux "gx/ipfs/QmTnsezaB1wWNRHeHnYrm8K4d5i9wtyj3GsqjC3Rt5b5v5/go-multistream"
	tu "gx/ipfs/QmWRCn8vruNAzHx8i6SAXinuheRitKEGu8c7m26stKvsYx/go-testutil"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ic "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
	tcpt "gx/ipfs/QmbrUTiVDSK3WGePN18qVjpGYmvXQt6YVPyyGoXWx593uq/go-tcp-transport"
	iconn "gx/ipfs/QmfQAY7YU4fQi3sjGLs1hwkM2Aq7dxgDyoMjaKN4WBWvcB/go-libp2p-interface-conn"
)

func goroFilter(r *grc.Goroutine) bool {
	return strings.Contains(r.Function, "go-log.") || strings.Contains(r.Stack[0], "testing.(*T).Run")
}

func echoListen(ctx context.Context, listener iconn.Listener) {
	for {
		c, err := listener.Accept()
		if err != nil {

			select {
			case <-ctx.Done():
				return
			default:
			}

			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				<-time.After(time.Microsecond * 10)
				continue
			}

			log.Debugf("echoListen: listener appears to be closing")
			return
		}

		go echo(c.(iconn.Conn))
	}
}

func echo(c iconn.Conn) {
	io.Copy(c, c)
}

func setupSecureConn(t *testing.T, ctx context.Context) (a, b iconn.Conn, p1, p2 tu.PeerNetParams) {
	return setupConn(t, ctx, true)
}

func setupSingleConn(t *testing.T, ctx context.Context) (a, b iconn.Conn, p1, p2 tu.PeerNetParams) {
	return setupConn(t, ctx, false)
}

func Listen(ctx context.Context, addr ma.Multiaddr, local peer.ID, sk ic.PrivKey) (iconn.Listener, error) {
	list, err := tcpt.NewTCPTransport().Listen(addr)
	if err != nil {
		return nil, err
	}

	return WrapTransportListener(ctx, list, local, sk)
}

func dialer(t *testing.T, a ma.Multiaddr) transport.Dialer {
	tpt := tcpt.NewTCPTransport()
	tptd, err := tpt.Dialer(a)
	if err != nil {
		t.Fatal(err)
	}

	return tptd
}

func setupConn(t *testing.T, ctx context.Context, secure bool) (a, b iconn.Conn, p1, p2 tu.PeerNetParams) {

	p1 = tu.RandPeerNetParamsOrFatal(t)
	p2 = tu.RandPeerNetParamsOrFatal(t)

	key1 := p1.PrivKey
	key2 := p2.PrivKey
	if !secure {
		key1 = nil
		key2 = nil
	}
	l1, err := Listen(ctx, p1.Addr, p1.ID, key1)
	if err != nil {
		t.Fatal(err)
	}
	p1.Addr = l1.Multiaddr() // Addr has been determined by kernel.

	d2 := &Dialer{
		LocalPeer:  p2.ID,
		PrivateKey: key2,
	}

	d2.AddDialer(dialer(t, p2.Addr))

	var c2 iconn.Conn

	done := make(chan error)
	go func() {
		defer close(done)

		var err error
		c2, err = d2.Dial(ctx, p1.Addr, p1.ID)
		if err != nil {
			done <- err
			return
		}

		// if secure, need to read + write, as that's what triggers the handshake.
		if secure {
			if err := sayHello(c2); err != nil {
				done <- err
			}
		}
	}()

	c1, err := l1.Accept()
	if err != nil {
		t.Fatal("failed to accept", err)
	}

	// if secure, need to read + write, as that's what triggers the handshake.
	if secure {
		if err := sayHello(c1); err != nil {
			done <- err
		}
	}

	if err := <-done; err != nil {
		t.Fatal(err)
	}

	return c1.(iconn.Conn), c2, p1, p2
}

func sayHello(c net.Conn) error {
	h := []byte("hello")
	if _, err := c.Write(h); err != nil {
		return err
	}
	if _, err := c.Read(h); err != nil {
		return err
	}
	if string(h) != "hello" {
		return fmt.Errorf("did not get hello")
	}
	return nil
}

func testDialer(t *testing.T, secure bool) {
	// t.Skip("Skipping in favor of another test")

	p1 := tu.RandPeerNetParamsOrFatal(t)
	p2 := tu.RandPeerNetParamsOrFatal(t)

	key1 := p1.PrivKey
	key2 := p2.PrivKey
	if !secure {
		key1 = nil
		key2 = nil
		t.Log("testing insecurely")
	} else {
		t.Log("testing securely")
	}

	ctx, cancel := context.WithCancel(context.Background())
	l1, err := Listen(ctx, p1.Addr, p1.ID, key1)
	if err != nil {
		t.Fatal(err)
	}
	p1.Addr = l1.Multiaddr() // Addr has been determined by kernel.

	d2 := &Dialer{
		LocalPeer:  p2.ID,
		PrivateKey: key2,
	}
	d2.AddDialer(dialer(t, p2.Addr))

	go echoListen(ctx, l1)

	c, err := d2.Dial(ctx, p1.Addr, p1.ID)
	if err != nil {
		t.Fatal("error dialing peer", err)
	}

	// fmt.Println("sending")
	mc := msgioWrap(c)
	mc.WriteMsg([]byte("beep"))
	mc.WriteMsg([]byte("boop"))
	out, err := mc.ReadMsg()
	if err != nil {
		t.Fatal(err)
	}

	// fmt.Println("recving", string(out))
	data := string(out)
	if data != "beep" {
		t.Error("unexpected conn output", data)
	}

	out, err = mc.ReadMsg()
	if err != nil {
		t.Fatal(err)
	}

	data = string(out)
	if string(out) != "boop" {
		t.Error("unexpected conn output", data)
	}

	// fmt.Println("closing")
	c.Close()
	l1.Close()
	cancel()
}

func TestDialerInsecure(t *testing.T) {
	// t.Skip("Skipping in favor of another test")
	testDialer(t, false)
}

func TestDialerSecure(t *testing.T) {
	// t.Skip("Skipping in favor of another test")
	testDialer(t, true)
}

func testDialerCloseEarly(t *testing.T, secure bool) {
	// t.Skip("Skipping in favor of another test")

	p1 := tu.RandPeerNetParamsOrFatal(t)
	p2 := tu.RandPeerNetParamsOrFatal(t)

	key1 := p1.PrivKey
	if !secure {
		key1 = nil
		t.Log("testing insecurely")
	} else {
		t.Log("testing securely")
	}

	ctx, cancel := context.WithCancel(context.Background())
	l1, err := Listen(ctx, p1.Addr, p1.ID, key1)
	if err != nil {
		t.Fatal(err)
	}
	p1.Addr = l1.Multiaddr() // Addr has been determined by kernel.

	// lol nesting
	d2 := &Dialer{
		LocalPeer:  p2.ID,
		PrivateKey: p2.PrivKey, //-- dont give it key. we'll just close the conn.
	}
	d2.AddDialer(dialer(t, p2.Addr))

	errs := make(chan error, 100)
	done := make(chan struct{}, 1)
	gotclosed := make(chan struct{}, 1)
	go func() {
		defer func() { done <- struct{}{} }()

		c, err := l1.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "closed") {
				gotclosed <- struct{}{}
				return
			}
			errs <- err
		}

		if _, err := c.Write([]byte("hello")); err != nil {
			gotclosed <- struct{}{}
			return
		}

		errs <- fmt.Errorf("wrote to conn")
	}()

	c, err := d2.Dial(ctx, p1.Addr, p1.ID)
	if err != nil {
		t.Fatal(err)
	}
	c.Close() // close it early.

	readerrs := func() {
		for {
			select {
			case e := <-errs:
				t.Error(e)
			default:
				return
			}
		}
	}
	readerrs()

	l1.Close()
	<-done
	cancel()
	readerrs()
	close(errs)

	select {
	case <-gotclosed:
	default:
		t.Error("did not get closed")
	}
}

// we dont do a handshake with singleConn, so cant "close early."
// func TestDialerCloseEarlyInsecure(t *testing.T) {
// 	// t.Skip("Skipping in favor of another test")
// 	testDialerCloseEarly(t, false)
// }

func TestDialerCloseEarlySecure(t *testing.T) {
	// t.Skip("Skipping in favor of another test")
	testDialerCloseEarly(t, true)
}

func TestMultistreamHeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p1 := tu.RandPeerNetParamsOrFatal(t)

	l1, err := Listen(ctx, p1.Addr, p1.ID, p1.PrivKey)
	if err != nil {
		t.Fatal(err)
	}

	p1.Addr = l1.Multiaddr() // Addr has been determined by kernel.

	go func() {
		_, _ = l1.Accept()
	}()

	con, err := net.Dial("tcp", l1.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer con.Close()

	err = msmux.SelectProtoOrFail(SecioTag, con)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFailedAccept(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p1 := tu.RandPeerNetParamsOrFatal(t)

	l1, err := Listen(ctx, p1.Addr, p1.ID, p1.PrivKey)
	if err != nil {
		t.Fatal(err)
	}

	p1.Addr = l1.Multiaddr() // Addr has been determined by kernel.

	done := make(chan struct{})
	go func() {
		defer close(done)
		con, err := net.Dial("tcp", l1.Addr().String())
		if err != nil {
			t.Error("first dial failed: ", err)
		}

		// write some garbage
		con.Write(bytes.Repeat([]byte{255}, 1000))

		con.Close()

		con, err = net.Dial("tcp", l1.Addr().String())
		if err != nil {
			t.Error("second dial failed: ", err)
		}
		defer con.Close()

		err = msmux.SelectProtoOrFail(SecioTag, con)
		if err != nil {
			t.Error("msmux select failed: ", err)
		}
	}()

	c, err := l1.Accept()
	if err != nil {
		t.Fatal("connections after a failed accept should still work: ", err)
	}

	c.Close()
	<-done
}

func TestHangingAccept(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p1 := tu.RandPeerNetParamsOrFatal(t)

	l1, err := Listen(ctx, p1.Addr, p1.ID, p1.PrivKey)
	if err != nil {
		t.Fatal(err)
	}

	p1.Addr = l1.Multiaddr() // Addr has been determined by kernel.

	done := make(chan struct{})
	go func() {
		defer close(done)
		con, err := net.Dial("tcp", l1.Addr().String())
		if err != nil {
			t.Error("first dial failed: ", err)
		}
		// hang this connection
		defer con.Close()

		// ensure that the first conn hits first
		time.Sleep(time.Millisecond * 50)

		con2, err := net.Dial("tcp", l1.Addr().String())
		if err != nil {
			t.Error("second dial failed: ", err)
		}
		defer con2.Close()

		err = msmux.SelectProtoOrFail(SecioTag, con2)
		if err != nil {
			t.Error("msmux select failed: ", err)
		}

		_, err = con2.Write([]byte("test"))
		if err != nil {
			t.Error("con write failed: ", err)
		}
	}()

	c, err := l1.Accept()
	if err != nil {
		t.Fatal("connections after a failed accept should still work: ", err)
	}

	c.Close()
	<-done
}

// This test kicks off N (=300) concurrent dials, which wait d (=20ms) seconds before failing.
// That wait holds up the handshake (multistream AND crypto), which will happen BEFORE
// l1.Accept() returns a connection. This test checks that the handshakes all happen
// concurrently in the listener side, and not sequentially. This ensures that a hanging dial
// will not block the listener from accepting other dials concurrently.
func TestConcurrentAccept(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p1 := tu.RandPeerNetParamsOrFatal(t)

	l1, err := Listen(ctx, p1.Addr, p1.ID, p1.PrivKey)
	if err != nil {
		t.Fatal(err)
	}

	n := 300
	delay := time.Millisecond * 20
	if runtime.GOOS == "darwin" {
		n = 100
	}

	p1.Addr = l1.Multiaddr() // Addr has been determined by kernel.

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			con, err := net.Dial("tcp", l1.Addr().String())
			if err != nil {
				log.Error(err)
				t.Error("first dial failed: ", err)
				return
			}
			// hang this connection
			defer con.Close()

			time.Sleep(delay)
			err = msmux.SelectProtoOrFail(SecioTag, con)
			if err != nil {
				t.Error(err)
			}
		}()
	}

	before := time.Now()
	for i := 0; i < n; i++ {
		c, err := l1.Accept()
		if err != nil {
			t.Fatal("connections after a failed accept should still work: ", err)
		}

		c.Close()
	}

	limit := delay * time.Duration(n)
	took := time.Since(before)
	if took > limit {
		t.Fatal("took too long!")
	}
	log.Errorf("took: %s (less than %s)", took, limit)
	l1.Close()
	wg.Wait()
	cancel()

	time.Sleep(time.Millisecond * 100)

	err = grc.CheckForLeaks(goroFilter)
	if err != nil {
		t.Fatal(err)
	}
}

func TestConnectionTimeouts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	old := NegotiateReadTimeout
	NegotiateReadTimeout = time.Second * 5
	defer func() { NegotiateReadTimeout = old }()

	p1 := tu.RandPeerNetParamsOrFatal(t)

	l1, err := Listen(ctx, p1.Addr, p1.ID, p1.PrivKey)
	if err != nil {
		t.Fatal(err)
	}

	n := 100
	if runtime.GOOS == "darwin" {
		n = 50
	}

	p1.Addr = l1.Multiaddr() // Addr has been determined by kernel.

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			con, err := net.Dial("tcp", l1.Addr().String())
			if err != nil {
				log.Error(err)
				t.Error("first dial failed: ", err)
				return
			}
			defer con.Close()

			// hang this connection until timeout
			io.ReadFull(con, make([]byte, 1000))
		}()
	}

	// wait to make sure the hanging dials have started
	time.Sleep(time.Millisecond * 50)

	good_n := 20
	for i := 0; i < good_n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			con, err := net.Dial("tcp", l1.Addr().String())
			if err != nil {
				log.Error(err)
				t.Error("first dial failed: ", err)
				return
			}
			defer con.Close()

			// dial these ones through
			err = msmux.SelectProtoOrFail(SecioTag, con)
			if err != nil {
				t.Error(err)
			}
		}()
	}

	before := time.Now()
	for i := 0; i < good_n; i++ {
		c, err := l1.Accept()
		if err != nil {
			t.Fatal("connections during hung dials should still work: ", err)
		}

		c.Close()
	}

	took := time.Since(before)

	if took > time.Second*5 {
		t.Fatal("hanging dials shouldnt block good dials")
	}

	wg.Wait()

	go func() {
		con, err := net.Dial("tcp", l1.Addr().String())
		if err != nil {
			log.Error(err)
			t.Error("first dial failed: ", err)
			return
		}
		defer con.Close()

		// dial these ones through
		err = msmux.SelectProtoOrFail(SecioTag, con)
		if err != nil {
			t.Error(err)
		}
	}()

	// make sure we can dial in still after a bunch of timeouts
	con, err := l1.Accept()
	if err != nil {
		t.Fatal(err)
	}

	con.Close()
	l1.Close()
	cancel()

	time.Sleep(time.Millisecond * 100)

	err = grc.CheckForLeaks(goroFilter)
	if err != nil {
		t.Fatal(err)
	}
}

func TestForcePNet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ipnet.ForcePrivateNetwork = true
	defer func() {
		ipnet.ForcePrivateNetwork = false
	}()

	p := tu.RandPeerNetParamsOrFatal(t)
	list, err := tcpt.NewTCPTransport().Listen(p.Addr)
	if err != nil {
		t.Fatal(err)
	}

	_, err = WrapTransportListenerWithProtector(ctx, list, p.ID, p.PrivKey, nil)
	if err != ipnet.ErrNotInPrivateNetwork {
		t.Fatal("Wrong error, expected error lack of protector")
	}
}

type fakeProtector struct {
	used bool
}

func (f *fakeProtector) Fingerprint() []byte {
	return make([]byte, 32)
}

func (f *fakeProtector) Protect(c transport.Conn) (transport.Conn, error) {
	f.used = true
	return &rot13Crypt{c}, nil
}

type rot13Crypt struct {
	transport.Conn
}

func (r *rot13Crypt) Read(b []byte) (int, error) {
	n, err := r.Conn.Read(b)
	if err != nil {
		return n, err
	}

	for i, _ := range b {
		b[i] = byte((uint8(b[i]) - 13) & 0xff)
	}
	return n, err
}

func (r *rot13Crypt) Write(b []byte) (int, error) {
	for i, _ := range b {
		b[i] = byte((uint8(b[i]) + 13) & 0xff)
	}
	return r.Conn.Write(b)
}

func TestPNetIsUsed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p1 := tu.RandPeerNetParamsOrFatal(t)
	p2 := tu.RandPeerNetParamsOrFatal(t)

	p1Protec := &fakeProtector{}

	list, err := tcpt.NewTCPTransport().Listen(p1.Addr)
	if err != nil {
		t.Fatal(err)
	}

	l1, err := WrapTransportListenerWithProtector(ctx, list, p1.ID, p1.PrivKey, p1Protec)
	if err != nil {
		t.Fatal(err)
	}
	p1.Addr = l1.Multiaddr() // Addr has been determined by kernel.

	d2 := NewDialer(p2.ID, p2.PrivKey, nil)
	d2.Protector = &fakeProtector{}

	d2.AddDialer(dialer(t, p2.Addr))
	_, err = d2.Dial(ctx, p1.Addr, p1.ID)
	if err != nil {
		t.Fatal(err)
	}

	_, err = l1.Accept()
	if err != nil {
		t.Fatal(err)
	}

	if !p1Protec.used {
		t.Error("Listener did not use protector for the connection")
	}

	if !d2.Protector.(*fakeProtector).used {
		t.Error("Dialer did not use protector for the connection")
	}
}
