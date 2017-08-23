package commands

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	cmds "github.com/ipfs/go-ipfs/commands"
	core "github.com/ipfs/go-ipfs/core"
	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"

	context "context"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

const kPingTimeout = 10 * time.Second

type PingResult struct {
	Success bool
	Time    time.Duration
	Text    string
}

var PingCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Send echo request packets to IPFS hosts.",
		ShortDescription: `
'ipfs ping' is a tool to test sending data to other nodes. It finds nodes
via the routing system, sends pings, waits for pongs, and prints out round-
trip latency information.
		`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("peer ID", true, true, "ID of peer to be pinged.").EnableStdin(),
	},
	Options: []cmds.Option{
		cmds.IntOption("count", "n", "Number of ping messages to send.").Default(10),
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			outChan, ok := res.Output().(<-chan interface{})
			if !ok {
				fmt.Println(reflect.TypeOf(res.Output()))
				return nil, u.ErrCast()
			}

			marshal := func(v interface{}) (io.Reader, error) {
				obj, ok := v.(*PingResult)
				if !ok {
					return nil, u.ErrCast()
				}

				buf := new(bytes.Buffer)
				if len(obj.Text) > 0 {
					buf = bytes.NewBufferString(obj.Text + "\n")
				} else if obj.Success {
					fmt.Fprintf(buf, "Pong received: time=%.2f ms\n", obj.Time.Seconds()*1000)
				} else {
					fmt.Fprintf(buf, "Pong failed\n")
				}
				return buf, nil
			}

			return &cmds.ChannelMarshaler{
				Channel:   outChan,
				Marshaler: marshal,
				Res:       res,
			}, nil
		},
	},
	Run: func(req cmds.Request, res cmds.Response) {
		ctx := req.Context()
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		// Must be online!
		if !n.OnlineMode() {
			res.SetError(errNotOnline, cmds.ErrClient)
			return
		}

		addr, peerID, err := ParsePeerParam(req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if addr != nil {
			n.Peerstore.AddAddr(peerID, addr, pstore.TempAddrTTL) // temporary
		}

		numPings, _, err := req.Option("count").Int()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		outChan := pingPeer(ctx, n, peerID, numPings)
		res.SetOutput(outChan)
	},
	Type: PingResult{},
}

func pingPeer(ctx context.Context, n *core.IpfsNode, pid peer.ID, numPings int) <-chan interface{} {
	outChan := make(chan interface{})
	go func() {
		defer close(outChan)

		if len(n.Peerstore.Addrs(pid)) == 0 {
			// Make sure we can find the node in question
			outChan <- &PingResult{
				Text:    fmt.Sprintf("Looking up peer %s", pid.Pretty()),
				Success: true,
			}

			ctx, cancel := context.WithTimeout(ctx, kPingTimeout)
			defer cancel()
			p, err := n.Routing.FindPeer(ctx, pid)
			if err != nil {
				outChan <- &PingResult{Text: fmt.Sprintf("Peer lookup error: %s", err)}
				return
			}
			n.Peerstore.AddAddrs(p.ID, p.Addrs, pstore.TempAddrTTL)
		}

		outChan <- &PingResult{
			Text:    fmt.Sprintf("PING %s.", pid.Pretty()),
			Success: true,
		}

		ctx, cancel := context.WithTimeout(ctx, kPingTimeout*time.Duration(numPings))
		defer cancel()
		pings, err := n.Ping.Ping(ctx, pid)
		if err != nil {
			log.Debugf("Ping error: %s", err)
			outChan <- &PingResult{
				Success: false,
				Text:    fmt.Sprintf("Ping error: %s", err),
			}
			return
		}

		var done bool
		var total time.Duration
		for i := 0; i < numPings && !done; i++ {
			select {
			case <-ctx.Done():
				done = true
				break
			case t, ok := <-pings:
				if !ok {
					done = true
					break
				}

				outChan <- &PingResult{
					Success: true,
					Time:    t,
				}
				total += t
				time.Sleep(time.Second)
			}
		}
		averagems := total.Seconds() * 1000 / float64(numPings)
		outChan <- &PingResult{
			Success: true,
			Text:    fmt.Sprintf("Average latency: %.2fms", averagems),
		}
	}()
	return outChan
}

func ParsePeerParam(text string) (ma.Multiaddr, peer.ID, error) {
	// to be replaced with just multiaddr parsing, once ptp is a multiaddr protocol
	idx := strings.LastIndex(text, "/")
	if idx == -1 {
		pid, err := peer.IDB58Decode(text)
		if err != nil {
			return nil, "", err
		}

		return nil, pid, nil
	}

	addrS := text[:idx]
	peeridS := text[idx+1:]

	var maddr ma.Multiaddr
	var pid peer.ID

	// make sure addrS parses as a multiaddr.
	if len(addrS) > 0 {
		var err error
		maddr, err = ma.NewMultiaddr(addrS)
		if err != nil {
			return nil, "", err
		}
	}

	// make sure idS parses as a peer.ID
	var err error
	pid, err = peer.IDB58Decode(peeridS)
	if err != nil {
		return nil, "", err
	}

	return maddr, pid, nil
}
