package supernode

import (
	"bytes"
	"context"
	"errors"
	"time"

	proxy "github.com/ipfs/go-ipfs/routing/supernode/proxy"

	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	routing "gx/ipfs/QmPjTrrSfE6TzLv6ya6VWhGcCgPrUAdcgrDcQyRDX2VyW1/go-libp2p-routing"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	loggables "gx/ipfs/QmT4PgCNdv73hnFAqzHqwW44q7M9PWpykSswHDxndquZbc/go-libp2p-loggables"
	cid "gx/ipfs/QmTprEaAA2A9bst5XH7exuyi5KzNMK3SEDNN8rBDnKWcUS/go-cid"
	"gx/ipfs/QmW8Rgju5JrSMHP7RDNdiwwXyenRqAbtSaPfdQKQC7ZdH6/go-libp2p-host"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	proto "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/proto"
	dhtpb "gx/ipfs/QmZSEstKChFezMwTcAWH5UQxvRckmnvgVZjFeSnGLDHU3Y/go-libp2p-kad-dht/pb"
	pb "gx/ipfs/QmbxkgUceEcuSZ4ZdBA3x74VUDSSYjHYmmeEqkjxbtZ6Jg/go-libp2p-record/pb"
)

var log = logging.Logger("supernode")

type Client struct {
	peerhost  host.Host
	peerstore pstore.Peerstore
	proxy     proxy.Proxy
	local     peer.ID
}

// TODO take in datastore/cache
func NewClient(px proxy.Proxy, h host.Host, ps pstore.Peerstore, local peer.ID) (*Client, error) {
	return &Client{
		proxy:     px,
		local:     local,
		peerstore: ps,
		peerhost:  h,
	}, nil
}

func (c *Client) FindProvidersAsync(ctx context.Context, k *cid.Cid, max int) <-chan pstore.PeerInfo {
	logging.ContextWithLoggable(ctx, loggables.Uuid("findProviders"))
	defer log.EventBegin(ctx, "findProviders", k).Done()
	ch := make(chan pstore.PeerInfo)
	go func() {
		defer close(ch)
		request := dhtpb.NewMessage(dhtpb.Message_GET_PROVIDERS, k.KeyString(), 0)
		response, err := c.proxy.SendRequest(ctx, request)
		if err != nil {
			log.Debug(err)
			return
		}
		for _, p := range dhtpb.PBPeersToPeerInfos(response.GetProviderPeers()) {
			select {
			case <-ctx.Done():
				log.Debug(ctx.Err())
				return
			case ch <- *p:
			}
		}
	}()
	return ch
}

func (c *Client) PutValue(ctx context.Context, k string, v []byte) error {
	defer log.EventBegin(ctx, "putValue").Done()
	r, err := makeRecord(c.peerstore, c.local, k, v)
	if err != nil {
		return err
	}
	pmes := dhtpb.NewMessage(dhtpb.Message_PUT_VALUE, string(k), 0)
	pmes.Record = r
	return c.proxy.SendMessage(ctx, pmes) // wrap to hide the remote
}

func (c *Client) GetValue(ctx context.Context, k string) ([]byte, error) {
	defer log.EventBegin(ctx, "getValue").Done()
	msg := dhtpb.NewMessage(dhtpb.Message_GET_VALUE, string(k), 0)
	response, err := c.proxy.SendRequest(ctx, msg) // TODO wrap to hide the remote
	if err != nil {
		return nil, err
	}
	return response.Record.GetValue(), nil
}

func (c *Client) GetValues(ctx context.Context, k string, _ int) ([]routing.RecvdVal, error) {
	defer log.EventBegin(ctx, "getValue").Done()
	msg := dhtpb.NewMessage(dhtpb.Message_GET_VALUE, string(k), 0)
	response, err := c.proxy.SendRequest(ctx, msg) // TODO wrap to hide the remote
	if err != nil {
		return nil, err
	}

	return []routing.RecvdVal{
		{
			Val:  response.Record.GetValue(),
			From: c.local,
		},
	}, nil
}

// Provide adds the given key 'k' to the content routing system. If 'brd' is
// true, it announces that content to the network.  For the supernode client,
// setting 'brd' to false makes this call a no-op
func (c *Client) Provide(ctx context.Context, k *cid.Cid, brd bool) error {
	if !brd {
		return nil
	}
	defer log.EventBegin(ctx, "provide", k).Done()
	msg := dhtpb.NewMessage(dhtpb.Message_ADD_PROVIDER, k.KeyString(), 0)
	// FIXME how is connectedness defined for the local node
	pri := []dhtpb.PeerRoutingInfo{
		{
			PeerInfo: pstore.PeerInfo{
				ID:    c.local,
				Addrs: c.peerhost.Addrs(),
			},
		},
	}
	msg.ProviderPeers = dhtpb.PeerRoutingInfosToPBPeers(pri)
	return c.proxy.SendMessage(ctx, msg) // TODO wrap to hide remote
}

func (c *Client) FindPeer(ctx context.Context, id peer.ID) (pstore.PeerInfo, error) {
	defer log.EventBegin(ctx, "findPeer", id).Done()
	request := dhtpb.NewMessage(dhtpb.Message_FIND_NODE, string(id), 0)
	response, err := c.proxy.SendRequest(ctx, request) // hide remote
	if err != nil {
		return pstore.PeerInfo{}, err
	}
	for _, p := range dhtpb.PBPeersToPeerInfos(response.GetCloserPeers()) {
		if p.ID == id {
			return *p, nil
		}
	}
	return pstore.PeerInfo{}, errors.New("could not find peer")
}

// creates and signs a record for the given key/value pair
func makeRecord(ps pstore.Peerstore, p peer.ID, k string, v []byte) (*pb.Record, error) {
	blob := bytes.Join([][]byte{[]byte(k), v, []byte(p)}, []byte{})
	sig, err := ps.PrivKey(p).Sign(blob)
	if err != nil {
		return nil, err
	}
	return &pb.Record{
		Key:       proto.String(string(k)),
		Value:     v,
		Author:    proto.String(string(p)),
		Signature: sig,
	}, nil
}

func (c *Client) Ping(ctx context.Context, id peer.ID) (time.Duration, error) {
	defer log.EventBegin(ctx, "ping", id).Done()
	return time.Nanosecond, errors.New("supernode routing does not support the ping method")
}

func (c *Client) Bootstrap(ctx context.Context) error {
	return c.proxy.Bootstrap(ctx)
}

var _ routing.IpfsRouting = &Client{}
