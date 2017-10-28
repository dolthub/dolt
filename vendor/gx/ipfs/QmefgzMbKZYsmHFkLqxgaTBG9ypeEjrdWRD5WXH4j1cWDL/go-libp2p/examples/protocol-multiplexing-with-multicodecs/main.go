package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	inet "gx/ipfs/QmNa31VPzC561NWwRsJLE7nGYZYuuD2QfpK2b1q9BK54J1/go-libp2p-net"
	ps "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	crypto "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
	host "gx/ipfs/Qmc1XhrFEiSeBNn3mpfg6gEuYCt5im2gYmNVmncsvmpeAk/go-libp2p-host"
	swarm "gx/ipfs/QmdQFrFnPrKRQtpeHKjZ3cVNwxmGKKS2TvhJTuN9C9yduh/go-libp2p-swarm"

	multicodec "github.com/multiformats/go-multicodec"
	json "github.com/multiformats/go-multicodec/json"
	bhost "gx/ipfs/QmefgzMbKZYsmHFkLqxgaTBG9ypeEjrdWRD5WXH4j1cWDL/go-libp2p/p2p/host/basic"
)

const proto = "/example/1.0.0"

// Message is a serializable/encodable object that we will send
// on a Stream.
type Message struct {
	Msg    string
	Index  int
	HangUp bool
}

// streamWrap wraps a libp2p stream. We encode/decode whenever we
// write/read from a stream, so we can just carry the encoders
// and bufios with us
type WrappedStream struct {
	stream inet.Stream
	enc    multicodec.Encoder
	dec    multicodec.Decoder
	w      *bufio.Writer
	r      *bufio.Reader
}

// wrapStream takes a stream and complements it with r/w bufios and
// decoder/encoder. In order to write raw data to the stream we can use
// wrap.w.Write(). To encode something into it we can wrap.enc.Encode().
// Finally, we should wrap.w.Flush() to actually send the data. Handling
// incoming data works similarly with wrap.r.Read() for raw-reading and
// wrap.dec.Decode() to decode.
func WrapStream(s inet.Stream) *WrappedStream {
	reader := bufio.NewReader(s)
	writer := bufio.NewWriter(s)
	// This is where we pick our specific multicodec. In order to change the
	// codec, we only need to change this place.
	// See https://godoc.org/github.com/multiformats/go-multicodec/json
	dec := json.Multicodec(false).Decoder(reader)
	enc := json.Multicodec(false).Encoder(writer)
	return &WrappedStream{
		stream: s,
		r:      reader,
		w:      writer,
		enc:    enc,
		dec:    dec,
	}
}

// messages that will be sent between the hosts.
var conversationMsgs = []string{
	"Hello!",
	"Hey!",
	"How are you doing?",
	"Very good! It is great that you can send data on a stream to me!",
	"Not only that, the data is encoded in a JSON object.",
	"Yeah, and we are using the multicodecs interface to encode and decode.",
	"This way we could swap it easily for, say, cbor, or msgpack!",
	"Let's leave that as an excercise for the reader...",
	"Agreed, our last message should activate the HangUp flag",
	"Yes, and the example code will close streams. So sad :(. Bye!",
}

func makeRandomHost(port int) host.Host {
	// Ignoring most errors for brevity
	// See echo example for more details and better implementation
	priv, pub, _ := crypto.GenerateKeyPair(crypto.RSA, 2048)
	pid, _ := peer.IDFromPublicKey(pub)
	listen, _ := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port))
	ps := ps.NewPeerstore()
	ps.AddPrivKey(pid, priv)
	ps.AddPubKey(pid, pub)
	n, _ := swarm.NewNetwork(context.Background(),
		[]ma.Multiaddr{listen}, pid, ps, nil)
	return bhost.New(n)
}

func main() {
	// Choose random ports between 10000-10100
	rand.Seed(666)
	port1 := rand.Intn(100) + 10000
	port2 := port1 + 1

	// Make 2 hosts
	h1 := makeRandomHost(port1)
	h2 := makeRandomHost(port2)
	h1.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), ps.PermanentAddrTTL)
	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), ps.PermanentAddrTTL)

	log.Printf("This is a conversation between %s and %s\n", h1.ID(), h2.ID())

	// Define a stream handler for host number 2
	h2.SetStreamHandler(proto, func(stream inet.Stream) {
		log.Printf("%s: Received a stream", h2.ID())
		wrappedStream := WrapStream(stream)
		defer stream.Close()
		handleStream(wrappedStream)
	})

	// Create new stream from h1 to h2 and start the conversation
	stream, err := h1.NewStream(context.Background(), h2.ID(), proto)
	if err != nil {
		log.Fatal(err)
	}
	wrappedStream := WrapStream(stream)
	// This sends the first message
	sendMessage(0, wrappedStream)
	// We keep the conversation on the created stream so we launch
	// this to handle any responses
	handleStream(wrappedStream)
	// When we are done, close the stream on our side and exit.
	stream.Close()
}

// receiveMessage reads and decodes a message from the stream
func receiveMessage(ws *WrappedStream) (*Message, error) {
	var msg Message
	err := ws.dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

// sendMessage encodes and writes a message to the stream
func sendMessage(index int, ws *WrappedStream) error {
	msg := &Message{
		Msg:    conversationMsgs[index],
		Index:  index,
		HangUp: index >= len(conversationMsgs)-1,
	}

	err := ws.enc.Encode(msg)
	// Because output is buffered with bufio, we need to flush!
	ws.w.Flush()
	return err
}

// handleStream is a for loop which receives and then sends a message
// an artificial delay of 500ms happens in-between.
// When Message.HangUp is true, it exists. This will close the stream
// on one of the sides. The other side's receiveMessage() will error
// with EOF, thus also breaking out from the loop.
func handleStream(ws *WrappedStream) {
	for {
		// Read
		msg, err := receiveMessage(ws)
		if err != nil {
			break
		}
		pid := ws.stream.Conn().LocalPeer()
		log.Printf("%s says: %s\n", pid, msg.Msg)
		time.Sleep(500 * time.Millisecond)
		if msg.HangUp {
			break
		}
		// Send response
		err = sendMessage(msg.Index+1, ws)
		if err != nil {
			break
		}
	}
}
