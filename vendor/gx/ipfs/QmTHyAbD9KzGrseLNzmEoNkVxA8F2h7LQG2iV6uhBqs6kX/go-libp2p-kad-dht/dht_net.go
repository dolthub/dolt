package dht

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "gx/ipfs/QmTHyAbD9KzGrseLNzmEoNkVxA8F2h7LQG2iV6uhBqs6kX/go-libp2p-kad-dht/pb"
	ctxio "gx/ipfs/QmTKsRYeY4simJyf37K93juSq75Lo8MVCDJ7owjmf46u8W/go-context/io"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ggio "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/io"
	inet "gx/ipfs/QmahYsGWry85Y7WUe2SX5G4JkH2zifEQAUtJVLZ24aC9DF/go-libp2p-net"
)

var dhtReadMessageTimeout = time.Minute
var ErrReadTimeout = fmt.Errorf("timed out reading response")

// handleNewStream implements the inet.StreamHandler
func (dht *IpfsDHT) handleNewStream(s inet.Stream) {
	go dht.handleNewMessage(s)
}

func (dht *IpfsDHT) handleNewMessage(s inet.Stream) {
	defer s.Close()

	ctx := dht.Context()
	cr := ctxio.NewReader(ctx, s) // ok to use. we defer close stream in this func
	cw := ctxio.NewWriter(ctx, s) // ok to use. we defer close stream in this func
	r := ggio.NewDelimitedReader(cr, inet.MessageSizeMax)
	w := ggio.NewDelimitedWriter(cw)
	mPeer := s.Conn().RemotePeer()

	for {
		// receive msg
		pmes := new(pb.Message)
		if err := r.ReadMsg(pmes); err != nil {
			log.Debugf("Error unmarshaling data: %s", err)
			return
		}

		// update the peer (on valid msgs only)
		dht.updateFromMessage(ctx, mPeer, pmes)

		// get handler for this msg type.
		handler := dht.handlerForMsgType(pmes.GetType())
		if handler == nil {
			log.Debug("got back nil handler from handlerForMsgType")
			return
		}

		// dispatch handler.
		rpmes, err := handler(ctx, mPeer, pmes)
		if err != nil {
			log.Debugf("handle message error: %s", err)
			return
		}

		// if nil response, return it before serializing
		if rpmes == nil {
			log.Debug("got back nil response from request")
			continue
		}

		// send out response msg
		if err := w.WriteMsg(rpmes); err != nil {
			log.Debugf("send response error: %s", err)
			return
		}
	}
}

// sendRequest sends out a request, but also makes sure to
// measure the RTT for latency measurements.
func (dht *IpfsDHT) sendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {

	ms := dht.messageSenderForPeer(p)

	start := time.Now()

	rpmes, err := ms.SendRequest(ctx, pmes)
	if err != nil {
		return nil, err
	}

	// update the peer (on valid msgs only)
	dht.updateFromMessage(ctx, p, rpmes)

	dht.peerstore.RecordLatency(p, time.Since(start))
	log.Event(ctx, "dhtReceivedMessage", dht.self, p, rpmes)
	return rpmes, nil
}

// sendMessage sends out a message
func (dht *IpfsDHT) sendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {

	ms := dht.messageSenderForPeer(p)

	if err := ms.SendMessage(ctx, pmes); err != nil {
		return err
	}
	log.Event(ctx, "dhtSentMessage", dht.self, p, pmes)
	return nil
}

func (dht *IpfsDHT) updateFromMessage(ctx context.Context, p peer.ID, mes *pb.Message) error {
	dht.Update(ctx, p)
	return nil
}

func (dht *IpfsDHT) messageSenderForPeer(p peer.ID) *messageSender {
	dht.smlk.Lock()
	defer dht.smlk.Unlock()

	ms, ok := dht.strmap[p]
	if !ok {
		ms = dht.newMessageSender(p)
		dht.strmap[p] = ms
	}

	return ms
}

type messageSender struct {
	s   inet.Stream
	r   ggio.ReadCloser
	w   ggio.WriteCloser
	lk  sync.Mutex
	p   peer.ID
	dht *IpfsDHT

	singleMes int
}

func (dht *IpfsDHT) newMessageSender(p peer.ID) *messageSender {
	return &messageSender{p: p, dht: dht}
}

func (ms *messageSender) prep() error {
	if ms.s != nil {
		return nil
	}

	nstr, err := ms.dht.host.NewStream(ms.dht.ctx, ms.p, ProtocolDHT, ProtocolDHTOld)
	if err != nil {
		return err
	}

	ms.r = ggio.NewDelimitedReader(nstr, inet.MessageSizeMax)
	ms.w = ggio.NewDelimitedWriter(nstr)
	ms.s = nstr

	return nil
}

// streamReuseTries is the number of times we will try to reuse a stream to a
// given peer before giving up and reverting to the old one-message-per-stream
// behaviour.
const streamReuseTries = 3

func (ms *messageSender) SendMessage(ctx context.Context, pmes *pb.Message) error {
	ms.lk.Lock()
	defer ms.lk.Unlock()
	if err := ms.prep(); err != nil {
		return err
	}

	if err := ms.writeMessage(pmes); err != nil {
		return err
	}

	if ms.singleMes > streamReuseTries {
		ms.s.Close()
		ms.s = nil
	}

	return nil
}

func (ms *messageSender) writeMessage(pmes *pb.Message) error {
	err := ms.w.WriteMsg(pmes)
	if err != nil {
		// If the other side isnt expecting us to be reusing streams, we're gonna
		// end up erroring here. To make sure things work seamlessly, lets retry once
		// before continuing

		log.Infof("error writing message: ", err)
		ms.s.Close()
		ms.s = nil
		if err := ms.prep(); err != nil {
			return err
		}

		if err := ms.w.WriteMsg(pmes); err != nil {
			return err
		}

		// keep track of this happening. If it happens a few times, its
		// likely we can assume the otherside will never support stream reuse
		ms.singleMes++
	}
	return nil
}

func (ms *messageSender) SendRequest(ctx context.Context, pmes *pb.Message) (*pb.Message, error) {
	ms.lk.Lock()
	defer ms.lk.Unlock()
	if err := ms.prep(); err != nil {
		return nil, err
	}

	if err := ms.writeMessage(pmes); err != nil {
		return nil, err
	}

	log.Event(ctx, "dhtSentMessage", ms.dht.self, ms.p, pmes)

	mes := new(pb.Message)
	if err := ms.ctxReadMsg(ctx, mes); err != nil {
		ms.s.Close()
		ms.s = nil
		return nil, err
	}

	if ms.singleMes > streamReuseTries {
		ms.s.Close()
		ms.s = nil
	}

	return mes, nil
}

func (ms *messageSender) ctxReadMsg(ctx context.Context, mes *pb.Message) error {
	errc := make(chan error, 1)
	go func(r ggio.ReadCloser) {
		errc <- r.ReadMsg(mes)
	}(ms.r)

	t := time.NewTimer(dhtReadMessageTimeout)
	defer t.Stop()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ErrReadTimeout
	}
}
