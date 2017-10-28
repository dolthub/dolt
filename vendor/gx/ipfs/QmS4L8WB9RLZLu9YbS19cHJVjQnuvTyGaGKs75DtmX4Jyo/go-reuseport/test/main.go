package main

import (
	"fmt"
	"io"
	"net"
	"os"

	reuse "gx/ipfs/QmS4L8WB9RLZLu9YbS19cHJVjQnuvTyGaGKs75DtmX4Jyo/go-reuseport"
	resolve "gx/ipfs/Qma73Sqt13DzHzveZ667BBfraM7MuMrPepUXjmv812JhRS/go-net-resolve-addr"
)

func main() {

	l1, err := reuse.Listen("tcp", "0.0.0.0:11111")
	maybeDie(err)
	fmt.Printf("listening on %s\n", l1.Addr())

	l2, err := reuse.Listen("tcp", "0.0.0.0:22222")
	maybeDie(err)
	fmt.Printf("listening on %s\n", l2.Addr())

	a1, err := resolve.ResolveAddr("dial", "tcp", "127.0.0.1:11111")
	maybeDie(err)

	a3, err := resolve.ResolveAddr("dial", "tcp", "127.0.0.1:33333")
	maybeDie(err)

	d1 := reuse.Dialer{net.Dialer{LocalAddr: a1}}
	d2 := reuse.Dialer{net.Dialer{LocalAddr: a3}}

	go func() {
		l2to1foo, err := l2.Accept()
		maybeDie(err)
		fmt.Printf("%s accepted conn from %s\n", addrStr(l2.Addr()), addrStr(l2to1foo.RemoteAddr()))

		fmt.Println("safe")

		l1to2bar, err := l1.Accept()
		maybeDie(err)
		fmt.Printf("%s accepted conn from %s\n", addrStr(l1.Addr()), addrStr(l1to2bar.RemoteAddr()))

		io.Copy(l1to2bar, l2to1foo)
	}()

	d1to2foo, err := d1.Dial("tcp4", "127.0.0.1:22222")
	maybeDie(err)
	fmt.Printf("dialing from %s to %s\n", d1.D.LocalAddr, "127.0.0.1:22222")

	d2to1bar, err := d2.Dial("tcp4", "127.0.0.1:11111")
	maybeDie(err)
	fmt.Printf("dialing from %s to %s\n", d2.D.LocalAddr, "127.0.0.1:11111")

	go io.Copy(d1to2foo, os.Stdin)
	io.Copy(os.Stdout, d2to1bar)
}

func die(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err)
	os.Exit(-1)
}

func maybeDie(err error) {
	if err != nil {
		die(err)
	}
}

func addrStr(a net.Addr) string {
	return fmt.Sprintf("%s/%s", a.Network(), a)
}
