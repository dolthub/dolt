// package main provides an implementation of netcat using the secio package.
// This means the channel is encrypted (and MACed).
// It is meant to exercise the spipe package.
// Usage:
//    seccat [<local address>] <remote address>
//    seccat -l <local address>
//
// Address format is: [host]:port
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"

	context "context"
	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	secio "gx/ipfs/QmZfwmhbcgSDGqGaoMMYx8jxBGauZw75zPjnZAyfwPso7M/go-libp2p-secio"
	ci "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
)

var verbose = false

// Usage prints out the usage of this module.
// Assumes flags use go stdlib flag pacakage.
var Usage = func() {
	text := `seccat - secure netcat in Go

Usage:

  listen: %s [<local address>] <remote address>
  dial:   %s -l <local address>

Address format is Go's: [host]:port
`

	fmt.Fprintf(os.Stderr, text, os.Args[0], os.Args[0])
	flag.PrintDefaults()
}

type args struct {
	listen     bool
	verbose    bool
	debug      bool
	localAddr  string
	remoteAddr string
	// keyfile    string
	keybits int
}

func parseArgs() args {
	var a args

	// setup + parse flags
	flag.BoolVar(&a.listen, "listen", false, "listen for connections")
	flag.BoolVar(&a.listen, "l", false, "listen for connections (short)")
	flag.BoolVar(&a.verbose, "v", true, "verbose")
	flag.BoolVar(&a.debug, "debug", false, "debugging")
	// flag.StringVar(&a.keyfile, "key", "", "private key file")
	flag.IntVar(&a.keybits, "keybits", 2048, "num bits for generating private key")
	flag.Usage = Usage
	flag.Parse()
	osArgs := flag.Args()

	if len(osArgs) < 1 {
		exit("")
	}

	if a.verbose {
		out("verbose on")
	}

	if a.listen {
		a.localAddr = osArgs[0]
	} else {
		if len(osArgs) > 1 {
			a.localAddr = osArgs[0]
			a.remoteAddr = osArgs[1]
		} else {
			a.remoteAddr = osArgs[0]
		}
	}

	return a
}

func main() {
	args := parseArgs()
	verbose = args.verbose
	if args.debug {
		logging.SetDebugLogging()
	}

	go func() {
		// wait until we exit.
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGABRT)
		<-sigc
		panic("ABORT! ABORT! ABORT!")
	}()

	if err := connect(args); err != nil {
		exit("%s", err)
	}
}

func setupPeer(a args) (peer.ID, pstore.Peerstore, error) {
	if a.keybits < 1024 {
		return "", nil, errors.New("Bitsize less than 1024 is considered unsafe.")
	}

	out("generating key pair...")
	sk, pk, err := ci.GenerateKeyPair(ci.RSA, a.keybits)
	if err != nil {
		return "", nil, err
	}

	p, err := peer.IDFromPublicKey(pk)
	if err != nil {
		return "", nil, err
	}

	ps := pstore.NewPeerstore()
	ps.AddPrivKey(p, sk)
	ps.AddPubKey(p, pk)

	out("local peer id: %s", p)
	return p, ps, nil
}

func connect(args args) error {
	p, ps, err := setupPeer(args)
	if err != nil {
		return err
	}

	var conn net.Conn
	if args.listen {
		conn, err = Listen(args.localAddr)
	} else {
		conn, err = Dial(args.localAddr, args.remoteAddr)
	}
	if err != nil {
		return err
	}

	// log everything that goes through conn
	rwc := &logRW{n: "conn", rw: conn}

	// OK, let's setup the channel.
	sk := ps.PrivKey(p)
	sg := secio.SessionGenerator{LocalID: p, PrivateKey: sk}
	sess, err := sg.NewSession(context.TODO(), rwc)
	if err != nil {
		return err
	}
	out("remote peer id: %s", sess.RemotePeer())
	netcat(sess.ReadWriter().(io.ReadWriteCloser))
	return nil
}

// Listen listens and accepts one incoming UDT connection on a given port,
// and pipes all incoming data to os.Stdout.
func Listen(localAddr string) (net.Conn, error) {
	l, err := net.Listen("tcp", localAddr)
	if err != nil {
		return nil, err
	}
	out("listening at %s", l.Addr())

	c, err := l.Accept()
	if err != nil {
		return nil, err
	}
	out("accepted connection from %s", c.RemoteAddr())

	// done with listener
	l.Close()

	return c, nil
}

// Dial connects to a remote address and pipes all os.Stdin to the remote end.
// If localAddr is set, uses it to Dial from.
func Dial(localAddr, remoteAddr string) (net.Conn, error) {

	var laddr net.Addr
	var err error
	if localAddr != "" {
		laddr, err = net.ResolveTCPAddr("tcp", localAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve address %s", localAddr)
		}
	}

	if laddr != nil {
		out("dialing %s from %s", remoteAddr, laddr)
	} else {
		out("dialing %s", remoteAddr)
	}

	d := net.Dialer{LocalAddr: laddr}
	c, err := d.Dial("tcp", remoteAddr)
	if err != nil {
		return nil, err
	}
	out("connected to %s", c.RemoteAddr())

	return c, nil
}

func netcat(c io.ReadWriteCloser) {
	out("piping stdio to connection")

	done := make(chan struct{}, 2)

	go func() {
		n, _ := io.Copy(c, os.Stdin)
		out("sent %d bytes", n)
		done <- struct{}{}
	}()
	go func() {
		n, _ := io.Copy(os.Stdout, c)
		out("received %d bytes", n)
		done <- struct{}{}
	}()

	// wait until we exit.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT,
		syscall.SIGTERM, syscall.SIGQUIT)

	select {
	case <-done:
	case <-sigc:
		return
	}

	c.Close()
}
