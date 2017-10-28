package commands

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	cmds "github.com/ipfs/go-ipfs/commands"
	core "github.com/ipfs/go-ipfs/core"
	blocks "gx/ipfs/QmSn9Td7xgxm9EV7iEjTckpUWmWApggzPxu7eFGWkkpwin/go-block-format"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
	floodsub "gx/ipfs/QmUUSLfvihARhCxxgnjW4hmycJpPvzNu12Aaz6JWVdfnLg/go-libp2p-floodsub"
)

var PubsubCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "An experimental publish-subscribe system on ipfs.",
		ShortDescription: `
ipfs pubsub allows you to publish messages to a given topic, and also to
subscribe to new messages on a given topic.

This is an experimental feature. It is not intended in its current state
to be used in a production environment.

To use, the daemon must be run with '--enable-pubsub-experiment'.
`,
	},
	Subcommands: map[string]*cmds.Command{
		"pub":   PubsubPubCmd,
		"sub":   PubsubSubCmd,
		"ls":    PubsubLsCmd,
		"peers": PubsubPeersCmd,
	},
}

var PubsubSubCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Subscribe to messages on a given topic.",
		ShortDescription: `
ipfs pubsub sub subscribes to messages on a given topic.

This is an experimental feature. It is not intended in its current state
to be used in a production environment.

To use, the daemon must be run with '--enable-pubsub-experiment'.
`,
		LongDescription: `
ipfs pubsub sub subscribes to messages on a given topic.

This is an experimental feature. It is not intended in its current state
to be used in a production environment.

To use, the daemon must be run with '--enable-pubsub-experiment'.

This command outputs data in the following encodings:
  * "json"
(Specified by the "--encoding" or "--enc" flag)
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("topic", true, false, "String name of topic to subscribe to."),
	},
	Options: []cmds.Option{
		cmds.BoolOption("discover", "try to discover other peers subscribed to the same topic"),
	},
	Run: func(req cmds.Request, res cmds.Response) {
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

		if n.Floodsub == nil {
			res.SetError(fmt.Errorf("experimental pubsub feature not enabled. Run daemon with --enable-pubsub-experiment to use."), cmds.ErrNormal)
			return
		}

		topic := req.Arguments()[0]
		sub, err := n.Floodsub.Subscribe(topic)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		out := make(chan interface{})
		res.SetOutput((<-chan interface{})(out))

		go func() {
			defer sub.Cancel()
			defer close(out)

			out <- floodsub.Message{}

			for {
				msg, err := sub.Next(req.Context())
				if err == io.EOF || err == context.Canceled {
					return
				} else if err != nil {
					res.SetError(err, cmds.ErrNormal)
					return
				}

				out <- msg
			}
		}()

		discover, _, _ := req.Option("discover").Bool()
		if discover {
			go func() {
				blk := blocks.NewBlock([]byte("floodsub:" + topic))
				cid, err := n.Blocks.AddBlock(blk)
				if err != nil {
					log.Error("pubsub discovery: ", err)
					return
				}

				connectToPubSubPeers(req.Context(), n, cid)
			}()
		}
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: getPsMsgMarshaler(func(m *floodsub.Message) (io.Reader, error) {
			return bytes.NewReader(m.Data), nil
		}),
		"ndpayload": getPsMsgMarshaler(func(m *floodsub.Message) (io.Reader, error) {
			m.Data = append(m.Data, '\n')
			return bytes.NewReader(m.Data), nil
		}),
		"lenpayload": getPsMsgMarshaler(func(m *floodsub.Message) (io.Reader, error) {
			buf := make([]byte, 8)

			n := binary.PutUvarint(buf, uint64(len(m.Data)))
			return io.MultiReader(bytes.NewReader(buf[:n]), bytes.NewReader(m.Data)), nil
		}),
	},
	Type: floodsub.Message{},
}

func connectToPubSubPeers(ctx context.Context, n *core.IpfsNode, cid *cid.Cid) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	provs := n.Routing.FindProvidersAsync(ctx, cid, 10)
	wg := &sync.WaitGroup{}
	for p := range provs {
		wg.Add(1)
		go func(pi pstore.PeerInfo) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(ctx, time.Second*10)
			defer cancel()
			err := n.PeerHost.Connect(ctx, pi)
			if err != nil {
				log.Info("pubsub discover: ", err)
				return
			}
			log.Info("connected to pubsub peer:", pi.ID)
		}(p)
	}

	wg.Wait()
}

func getPsMsgMarshaler(f func(m *floodsub.Message) (io.Reader, error)) func(cmds.Response) (io.Reader, error) {
	return func(res cmds.Response) (io.Reader, error) {
		outChan, ok := res.Output().(<-chan interface{})
		if !ok {
			return nil, u.ErrCast()
		}

		marshal := func(v interface{}) (io.Reader, error) {
			obj, ok := v.(*floodsub.Message)
			if !ok {
				return nil, u.ErrCast()
			}
			if obj.Message == nil {
				return strings.NewReader(""), nil
			}

			return f(obj)
		}

		return &cmds.ChannelMarshaler{
			Channel:   outChan,
			Marshaler: marshal,
			Res:       res,
		}, nil
	}
}

var PubsubPubCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Publish a message to a given pubsub topic.",
		ShortDescription: `
ipfs pubsub pub publishes a message to a specified topic.

This is an experimental feature. It is not intended in its current state
to be used in a production environment.

To use, the daemon must be run with '--enable-pubsub-experiment'.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("topic", true, false, "Topic to publish to."),
		cmds.StringArg("data", true, true, "Payload of message to publish.").EnableStdin(),
	},
	Run: func(req cmds.Request, res cmds.Response) {
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

		if n.Floodsub == nil {
			res.SetError(fmt.Errorf("experimental pubsub feature not enabled. Run daemon with --enable-pubsub-experiment to use."), cmds.ErrNormal)
			return
		}

		topic := req.Arguments()[0]

		for _, data := range req.Arguments()[1:] {
			if err := n.Floodsub.Publish(topic, []byte(data)); err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}
		}
	},
}

var PubsubLsCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "List subscribed topics by name.",
		ShortDescription: `
ipfs pubsub ls lists out the names of topics you are currently subscribed to.

This is an experimental feature. It is not intended in its current state
to be used in a production environment.

To use, the daemon must be run with '--enable-pubsub-experiment'.
`,
	},
	Run: func(req cmds.Request, res cmds.Response) {
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

		if n.Floodsub == nil {
			res.SetError(fmt.Errorf("experimental pubsub feature not enabled. Run daemon with --enable-pubsub-experiment to use."), cmds.ErrNormal)
			return
		}

		res.SetOutput(&stringList{n.Floodsub.GetTopics()})
	},
	Type: stringList{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: stringListMarshaler,
	},
}

var PubsubPeersCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "List peers we are currently pubsubbing with.",
		ShortDescription: `
ipfs pubsub peers with no arguments lists out the pubsub peers you are
currently connected to. If given a topic, it will list connected
peers who are subscribed to the named topic.

This is an experimental feature. It is not intended in its current state
to be used in a production environment.

To use, the daemon must be run with '--enable-pubsub-experiment'.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("topic", false, false, "topic to list connected peers of"),
	},
	Run: func(req cmds.Request, res cmds.Response) {
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

		if n.Floodsub == nil {
			res.SetError(fmt.Errorf("experimental pubsub feature not enabled. Run daemon with --enable-pubsub-experiment to use."), cmds.ErrNormal)
			return
		}

		var topic string
		if len(req.Arguments()) == 1 {
			topic = req.Arguments()[0]
		}

		var out []string
		for _, p := range n.Floodsub.ListPeers(topic) {
			out = append(out, p.Pretty())
		}
		res.SetOutput(&stringList{out})
	},
	Type: stringList{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: stringListMarshaler,
	},
}
