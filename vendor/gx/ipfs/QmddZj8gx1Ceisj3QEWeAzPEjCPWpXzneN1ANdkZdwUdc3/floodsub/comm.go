package floodsub

import (
	"bufio"
	"context"
	"io"

	pb "gx/ipfs/QmddZj8gx1Ceisj3QEWeAzPEjCPWpXzneN1ANdkZdwUdc3/floodsub/pb"

	ggio "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/io"
	proto "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/proto"
	inet "gx/ipfs/QmahYsGWry85Y7WUe2SX5G4JkH2zifEQAUtJVLZ24aC9DF/go-libp2p-net"
)

// get the initial RPC containing all of our subscriptions to send to new peers
func (p *PubSub) getHelloPacket() *RPC {
	var rpc RPC
	for t := range p.myTopics {
		as := &pb.RPC_SubOpts{
			Topicid:   proto.String(t),
			Subscribe: proto.Bool(true),
		}
		rpc.Subscriptions = append(rpc.Subscriptions, as)
	}
	return &rpc
}

func (p *PubSub) handleNewStream(s inet.Stream) {
	defer s.Close()

	r := ggio.NewDelimitedReader(s, 1<<20)
	for {
		rpc := new(RPC)
		err := r.ReadMsg(&rpc.RPC)
		if err != nil {
			if err != io.EOF {
				log.Errorf("error reading rpc from %s: %s", s.Conn().RemotePeer(), err)
			}
			return
		}

		rpc.from = s.Conn().RemotePeer()
		select {
		case p.incoming <- rpc:
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *PubSub) handleSendingMessages(ctx context.Context, s inet.Stream, outgoing <-chan *RPC) {
	var dead bool
	bufw := bufio.NewWriter(s)
	wc := ggio.NewDelimitedWriter(bufw)

	writeMsg := func(msg proto.Message) error {
		err := wc.WriteMsg(msg)
		if err != nil {
			return err
		}

		return bufw.Flush()
	}

	defer wc.Close()
	for {
		select {
		case rpc, ok := <-outgoing:
			if !ok {
				return
			}
			if dead {
				// continue in order to drain messages
				continue
			}

			err := writeMsg(&rpc.RPC)
			if err != nil {
				log.Warningf("writing message to %s: %s", s.Conn().RemotePeer(), err)
				dead = true
				go func() {
					p.peerDead <- s.Conn().RemotePeer()
				}()
			}

		case <-ctx.Done():
			return
		}
	}
}

func rpcWithSubs(subs ...*pb.RPC_SubOpts) *RPC {
	return &RPC{
		RPC: pb.RPC{
			Subscriptions: subs,
		},
	}
}

func rpcWithMessages(msgs ...*pb.Message) *RPC {
	return &RPC{RPC: pb.RPC{Publish: msgs}}
}
