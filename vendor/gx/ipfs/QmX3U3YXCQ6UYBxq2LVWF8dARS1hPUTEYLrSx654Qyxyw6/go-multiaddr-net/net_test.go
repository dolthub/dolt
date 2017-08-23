package manet

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"testing"

	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

func newMultiaddr(t *testing.T, m string) ma.Multiaddr {
	maddr, err := ma.NewMultiaddr(m)
	if err != nil {
		t.Fatal("failed to construct multiaddr:", m, err)
	}
	return maddr
}

func TestDial(t *testing.T) {

	listener, err := net.Listen("tcp", "127.0.0.1:4321")
	if err != nil {
		t.Fatal("failed to listen")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {

		cB, err := listener.Accept()
		if err != nil {
			t.Fatal("failed to accept")
		}

		// echo out
		buf := make([]byte, 1024)
		for {
			_, err := cB.Read(buf)
			if err != nil {
				break
			}
			cB.Write(buf)
		}

		wg.Done()
	}()

	maddr := newMultiaddr(t, "/ip4/127.0.0.1/tcp/4321")
	cA, err := Dial(maddr)
	if err != nil {
		t.Fatal("failed to dial")
	}

	buf := make([]byte, 1024)
	if _, err := cA.Write([]byte("beep boop")); err != nil {
		t.Fatal("failed to write:", err)
	}

	if _, err := cA.Read(buf); err != nil {
		t.Fatal("failed to read:", buf, err)
	}

	if !bytes.Equal(buf[:9], []byte("beep boop")) {
		t.Fatal("failed to echo:", buf)
	}

	maddr2 := cA.RemoteMultiaddr()
	if !maddr2.Equal(maddr) {
		t.Fatal("remote multiaddr not equal:", maddr, maddr2)
	}

	cA.Close()
	wg.Wait()
}

func TestListen(t *testing.T) {

	maddr := newMultiaddr(t, "/ip4/127.0.0.1/tcp/4322")
	listener, err := Listen(maddr)
	if err != nil {
		t.Fatal("failed to listen")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {

		cB, err := listener.Accept()
		if err != nil {
			t.Fatal("failed to accept")
		}

		if !cB.LocalMultiaddr().Equal(maddr) {
			t.Fatal("local multiaddr not equal:", maddr, cB.LocalMultiaddr())
		}

		// echo out
		buf := make([]byte, 1024)
		for {
			_, err := cB.Read(buf)
			if err != nil {
				break
			}
			cB.Write(buf)
		}

		wg.Done()
	}()

	cA, err := net.Dial("tcp", "127.0.0.1:4322")
	if err != nil {
		t.Fatal("failed to dial")
	}

	buf := make([]byte, 1024)
	if _, err := cA.Write([]byte("beep boop")); err != nil {
		t.Fatal("failed to write:", err)
	}

	if _, err := cA.Read(buf); err != nil {
		t.Fatal("failed to read:", buf, err)
	}

	if !bytes.Equal(buf[:9], []byte("beep boop")) {
		t.Fatal("failed to echo:", buf)
	}

	maddr2, err := FromNetAddr(cA.RemoteAddr())
	if err != nil {
		t.Fatal("failed to convert", err)
	}
	if !maddr2.Equal(maddr) {
		t.Fatal("remote multiaddr not equal:", maddr, maddr2)
	}

	cA.Close()
	wg.Wait()
}

func TestListenAddrs(t *testing.T) {

	test := func(addr, resaddr string, succeed bool) {
		if resaddr == "" {
			resaddr = addr
		}

		maddr := newMultiaddr(t, addr)
		l, err := Listen(maddr)
		if !succeed {
			if err == nil {
				t.Fatal("succeeded in listening", addr)
			}
			return
		}
		if succeed && err != nil {
			t.Error("failed to listen", addr, err)
		}
		if l == nil {
			t.Error("failed to listen", addr, succeed, err)
		}
		if l.Multiaddr().String() != resaddr {
			t.Error("listen addr did not resolve properly", l.Multiaddr().String(), resaddr, succeed, err)
		}

		if err = l.Close(); err != nil {
			t.Fatal("failed to close listener", addr, err)
		}
	}

	test("/ip4/127.0.0.1/tcp/4324", "", true)
	test("/ip4/127.0.0.1/udp/4325", "", false)
	test("/ip4/127.0.0.1/udp/4326/udt", "", false)
	test("/ip4/0.0.0.0/tcp/4324", "", true)
	test("/ip4/0.0.0.0/udp/4325", "", false)
	test("/ip4/0.0.0.0/udp/4326/udt", "", false)
	test("/ip6/::1/tcp/4324", "", true)
	test("/ip6/::1/udp/4325", "", false)
	test("/ip6/::1/udp/4326/udt", "", false)
	test("/ip6/::/tcp/4324", "", true)
	test("/ip6/::/udp/4325", "", false)
	test("/ip6/::/udp/4326/udt", "", false)
}

func TestListenAndDial(t *testing.T) {

	maddr := newMultiaddr(t, "/ip4/127.0.0.1/tcp/4323")
	listener, err := Listen(maddr)
	if err != nil {
		t.Fatal("failed to listen")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {

		cB, err := listener.Accept()
		if err != nil {
			t.Fatal("failed to accept")
		}

		if !cB.LocalMultiaddr().Equal(maddr) {
			t.Fatal("local multiaddr not equal:", maddr, cB.LocalMultiaddr())
		}

		// echo out
		buf := make([]byte, 1024)
		for {
			_, err := cB.Read(buf)
			if err != nil {
				break
			}
			cB.Write(buf)
		}

		wg.Done()
	}()

	cA, err := Dial(newMultiaddr(t, "/ip4/127.0.0.1/tcp/4323"))
	if err != nil {
		t.Fatal("failed to dial")
	}

	buf := make([]byte, 1024)
	if _, err := cA.Write([]byte("beep boop")); err != nil {
		t.Fatal("failed to write:", err)
	}

	if _, err := cA.Read(buf); err != nil {
		t.Fatal("failed to read:", buf, err)
	}

	if !bytes.Equal(buf[:9], []byte("beep boop")) {
		t.Fatal("failed to echo:", buf)
	}

	maddr2 := cA.RemoteMultiaddr()
	if !maddr2.Equal(maddr) {
		t.Fatal("remote multiaddr not equal:", maddr, maddr2)
	}

	cA.Close()
	wg.Wait()
}

func TestIPLoopback(t *testing.T) {
	if IP4Loopback.String() != "/ip4/127.0.0.1" {
		t.Error("IP4Loopback incorrect:", IP4Loopback)
	}

	if IP6Loopback.String() != "/ip6/::1" {
		t.Error("IP6Loopback incorrect:", IP6Loopback)
	}

	if IP6LinkLocalLoopback.String() != "/ip6/fe80::1" {
		t.Error("IP6LinkLocalLoopback incorrect:", IP6Loopback)
	}

	if !IsIPLoopback(IP4Loopback) {
		t.Error("IsIPLoopback failed (IP4Loopback)")
	}

	if !IsIPLoopback(IP6Loopback) {
		t.Error("IsIPLoopback failed (IP6Loopback)")
	}

	if !IsIPLoopback(IP6LinkLocalLoopback) {
		t.Error("IsIPLoopback failed (IP6LinkLocalLoopback)")
	}
}

func TestIPUnspecified(t *testing.T) {
	if IP4Unspecified.String() != "/ip4/0.0.0.0" {
		t.Error("IP4Unspecified incorrect:", IP4Unspecified)
	}

	if IP6Unspecified.String() != "/ip6/::" {
		t.Error("IP6Unspecified incorrect:", IP6Unspecified)
	}

	if !IsIPUnspecified(IP4Unspecified) {
		t.Error("IsIPUnspecified failed (IP4Unspecified)")
	}

	if !IsIPUnspecified(IP6Unspecified) {
		t.Error("IsIPUnspecified failed (IP6Unspecified)")
	}
}

func TestIP6LinkLocal(t *testing.T) {
	if !IsIP6LinkLocal(IP6LinkLocalLoopback) {
		t.Error("IsIP6LinkLocal failed (IP6LinkLocalLoopback)")
	}

	for a := 0; a < 65536; a++ {
		isLinkLocal := (a == 0xfe80)
		m := newMultiaddr(t, fmt.Sprintf("/ip6/%x::1", a))
		if IsIP6LinkLocal(m) != isLinkLocal {
			t.Errorf("IsIP6LinkLocal failed (%s != %v)", m, isLinkLocal)
		}
	}
}

func TestConvertNetAddr(t *testing.T) {
	m1 := newMultiaddr(t, "/ip4/1.2.3.4/tcp/4001")

	n1, err := ToNetAddr(m1)
	if err != nil {
		t.Fatal(err)
	}

	m2, err := FromNetAddr(n1)
	if err != nil {
		t.Fatal(err)
	}

	if m1.String() != m2.String() {
		t.Fatal("ToNetAddr + FromNetAddr did not work")
	}
}

func TestWrapNetConn(t *testing.T) {
	// test WrapNetConn nil
	if _, err := WrapNetConn(nil); err == nil {
		t.Error("WrapNetConn(nil) should return an error")
	}

	checkErr := func(err error, s string) {
		if err != nil {
			t.Fatal(s, err)
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	checkErr(err, "failed to listen")

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(1)
	go func() {
		defer wg.Done()
		cB, err := listener.Accept()
		checkErr(err, "failed to accept")
		cB.Close()
	}()

	cA, err := net.Dial("tcp", listener.Addr().String())
	checkErr(err, "failed to dial")
	defer cA.Close()

	lmaddr, err := FromNetAddr(cA.LocalAddr())
	checkErr(err, "failed to get local addr")
	rmaddr, err := FromNetAddr(cA.RemoteAddr())
	checkErr(err, "failed to get remote addr")

	mcA, err := WrapNetConn(cA)
	checkErr(err, "failed to wrap conn")

	if mcA.LocalAddr().String() != cA.LocalAddr().String() {
		t.Error("wrapped conn local addr differs")
	}
	if mcA.RemoteAddr().String() != cA.RemoteAddr().String() {
		t.Error("wrapped conn remote addr differs")
	}
	if mcA.LocalMultiaddr().String() != lmaddr.String() {
		t.Error("wrapped conn local maddr differs")
	}
	if mcA.RemoteMultiaddr().String() != rmaddr.String() {
		t.Error("wrapped conn remote maddr differs")
	}
}

func TestAddrMatch(t *testing.T) {

	test := func(m ma.Multiaddr, input, expect []ma.Multiaddr) {
		actual := AddrMatch(m, input)
		testSliceEqual(t, expect, actual)
	}

	a := []ma.Multiaddr{
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234"),
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/2345"),
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234/tcp/2345"),
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234/tcp/2345"),
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234/udp/1234"),
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234/udp/1234"),
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234/ip6/::1"),
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234/ip6/::1"),
		newMultiaddr(t, "/ip6/::1/tcp/1234"),
		newMultiaddr(t, "/ip6/::1/tcp/2345"),
		newMultiaddr(t, "/ip6/::1/tcp/1234/tcp/2345"),
		newMultiaddr(t, "/ip6/::1/tcp/1234/tcp/2345"),
		newMultiaddr(t, "/ip6/::1/tcp/1234/udp/1234"),
		newMultiaddr(t, "/ip6/::1/tcp/1234/udp/1234"),
		newMultiaddr(t, "/ip6/::1/tcp/1234/ip6/::1"),
		newMultiaddr(t, "/ip6/::1/tcp/1234/ip6/::1"),
	}

	test(a[0], a, []ma.Multiaddr{
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234"),
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/2345"),
	})
	test(a[2], a, []ma.Multiaddr{
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234/tcp/2345"),
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234/tcp/2345"),
	})
	test(a[4], a, []ma.Multiaddr{
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234/udp/1234"),
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234/udp/1234"),
	})
	test(a[6], a, []ma.Multiaddr{
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234/ip6/::1"),
		newMultiaddr(t, "/ip4/1.2.3.4/tcp/1234/ip6/::1"),
	})
	test(a[8], a, []ma.Multiaddr{
		newMultiaddr(t, "/ip6/::1/tcp/1234"),
		newMultiaddr(t, "/ip6/::1/tcp/2345"),
	})
	test(a[10], a, []ma.Multiaddr{
		newMultiaddr(t, "/ip6/::1/tcp/1234/tcp/2345"),
		newMultiaddr(t, "/ip6/::1/tcp/1234/tcp/2345"),
	})
	test(a[12], a, []ma.Multiaddr{
		newMultiaddr(t, "/ip6/::1/tcp/1234/udp/1234"),
		newMultiaddr(t, "/ip6/::1/tcp/1234/udp/1234"),
	})
	test(a[14], a, []ma.Multiaddr{
		newMultiaddr(t, "/ip6/::1/tcp/1234/ip6/::1"),
		newMultiaddr(t, "/ip6/::1/tcp/1234/ip6/::1"),
	})

}

func testSliceEqual(t *testing.T, a, b []ma.Multiaddr) {
	if len(a) != len(b) {
		t.Error("differ", a, b)
	}
	for i, addrA := range a {
		if !addrA.Equal(b[i]) {
			t.Error("differ", a, b)
		}
	}
}

func TestInterfaceAddressesWorks(t *testing.T) {
	_, err := InterfaceMultiaddrs()
	if err != nil {
		t.Fatal(err)
	}
}
