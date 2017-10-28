package main

import (
	"fmt"
	"io"
	"os"

	tpt "github.com/libp2p/go-tcp-transport"
	yamux "github.com/whyrusleeping/go-smux-yamux"
	ps "gx/ipfs/QmTMNkpso2WRMevXC8ZxgyBhJvoEHvk24SNeUr9Mf9UM1a/go-peerstream"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

func main() {
	log("creating a new swarm with Yamux transport") // create a new Swarm
	swarm := ps.NewSwarm(yamux.DefaultTransport)
	defer swarm.Close()

	// tell swarm what to do with a new incoming streams.
	// EchoHandler just echos back anything they write.
	log("setup EchoHandler")
	swarm.SetStreamHandler(ps.EchoHandler)

	// Okay, let's try listening on some transports
	log("listening at localhost:12001")
	addr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/12001")
	tr1 := tpt.NewTCPTransport()
	l1, err := tr1.Listen(addr)
	if err != nil {
		panic(err)
	}

	log("listening at localhost:12002")
	addr2, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/12002")
	tr2 := tpt.NewTCPTransport()
	l2, err := tr2.Listen(addr2)

	if err != nil {
		panic(err)
	}

	// tell swarm to accept incoming connections on these
	// listeners. Swarm will start accepting new connections.
	if _, err := swarm.AddListener(l1); err != nil {
		panic(err)
	}
	if _, err := swarm.AddListener(l2); err != nil {
		panic(err)
	}

	// ok, let's try some outgoing connections
	log("dialing localhost:12001")
	dlr1, err := tr1.Dialer(addr2)
	if err != nil {
		panic(err)
	}
	nc1, err := dlr1.Dial(addr)
	if err != nil {
		panic(err)
	}

	log("dialing localhost:12002")
	dlr2, err := tr2.Dialer(addr)
	if err != nil {
		panic(err)
	}
	nc2, err := dlr2.Dial(addr2)
	if err != nil {
		panic(err)
	}

	// add them to the swarm
	c1, err := swarm.AddConn(nc1)
	if err != nil {
		panic(err)
	}
	c2, err := swarm.AddConn(nc2)
	if err != nil {
		panic(err)
	}

	// Swarm treats listeners as sources of new connections and does
	// not distinguish between outgoing or incoming connections.
	// It provides the peerstream.Conn to the StreamHandler so you can
	// distinguish between them however you wish.

	// now let's try opening some streams!
	// You can specify what connection you want to use
	log("opening stream with NewStreamWithConn(c1)")
	s1, err := swarm.NewStreamWithConn(c1)
	if err != nil {
		panic(err)
	}

	// Or, you can specify a SelectConn function that picks between all
	// (it calls NewStreamWithConn underneath the hood)
	log("opening stream with NewStreamSelectConn(.)")
	s2, err := swarm.NewStreamSelectConn(func(conns []*ps.Conn) *ps.Conn {
		if len(conns) > 0 {
			return conns[0]
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Or, you can bind connections to ConnGroup ids. You can bind a conn to
	// multiple groups. And, if conn wasn't in swarm, it calls swarm.AddConn.
	// You can use any Go `KeyType` as a group A `KeyType` as in maps...)
	swarm.AddConnToGroup(c2, 1)

	// And then use that group to select a connection. Swarm will use any
	// connection it finds in that group, using a SelectConn you can rebind:
	//   swarm.SetGroupSelectConn(1, SelectConn)
	//   swarm.SetDegaultGroupSelectConn(SelectConn)
	log("opening stream with NewStreamWithGroup(1)")
	s3, err := swarm.NewStreamWithGroup(1)
	if err != nil {
		panic(err)
	}

	// Why groups? It's because with many connections, and many transports,
	// and many Servers (or Protocols), we can use the Swarm to associate
	// a different StreamHandlers per group, and to let us create NewStreams
	// on a given group.

	// Ok, we have streams. now what. Use them! Our Streams are basically
	// streams from github.com/docker/spdystream, so they work the same
	// way:

	log("preparing the streams")
	for i, stream := range []*ps.Stream{s1, s2, s3} {
		str := "stream %d ready:"
		fmt.Fprintf(stream, str, i)

		buf := make([]byte, len(str))
		log(fmt.Sprintf("reading from stream %d", i))
		stream.Read(buf)
		fmt.Println(string(buf))
	}

	log("let's test the streams")
	log("enter some text below:\n")
	go io.Copy(os.Stdout, s1)
	go io.Copy(os.Stdout, s2)
	go io.Copy(os.Stdout, s3)
	io.Copy(io.MultiWriter(s1, s2, s3), os.Stdin)
}

func log(s string) {
	fmt.Fprintf(os.Stderr, s+"\n")
}
