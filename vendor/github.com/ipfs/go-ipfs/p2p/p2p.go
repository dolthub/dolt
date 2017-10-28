package p2p

import (
	"context"
	"errors"
	"time"

	net "gx/ipfs/QmNa31VPzC561NWwRsJLE7nGYZYuuD2QfpK2b1q9BK54J1/go-libp2p-net"
	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	manet "gx/ipfs/QmX3U3YXCQ6UYBxq2LVWF8dARS1hPUTEYLrSx654Qyxyw6/go-multiaddr-net"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	pro "gx/ipfs/QmZNkThpqfVXs9GNbexPrfBbXSLNYeKrE7jwFM2oqHbyqN/go-libp2p-protocol"
	p2phost "gx/ipfs/Qmc1XhrFEiSeBNn3mpfg6gEuYCt5im2gYmNVmncsvmpeAk/go-libp2p-host"
)

// P2P structure holds information on currently running streams/listeners
type P2P struct {
	Listeners ListenerRegistry
	Streams   StreamRegistry

	identity  peer.ID
	peerHost  p2phost.Host
	peerstore pstore.Peerstore
}

// NewP2P creates new P2P struct
func NewP2P(identity peer.ID, peerHost p2phost.Host, peerstore pstore.Peerstore) *P2P {
	return &P2P{
		identity:  identity,
		peerHost:  peerHost,
		peerstore: peerstore,
	}
}

func (p2p *P2P) newStreamTo(ctx2 context.Context, p peer.ID, protocol string) (net.Stream, error) {
	ctx, cancel := context.WithTimeout(ctx2, time.Second*30) //TODO: configurable?
	defer cancel()
	err := p2p.peerHost.Connect(ctx, pstore.PeerInfo{ID: p})
	if err != nil {
		return nil, err
	}
	return p2p.peerHost.NewStream(ctx2, p, pro.ID(protocol))
}

// Dial creates new P2P stream to a remote listener
func (p2p *P2P) Dial(ctx context.Context, addr ma.Multiaddr, peer peer.ID, proto string, bindAddr ma.Multiaddr) (*ListenerInfo, error) {
	lnet, _, err := manet.DialArgs(bindAddr)
	if err != nil {
		return nil, err
	}

	listenerInfo := ListenerInfo{
		Identity: p2p.identity,
		Protocol: proto,
	}

	remote, err := p2p.newStreamTo(ctx, peer, proto)
	if err != nil {
		return nil, err
	}

	switch lnet {
	case "tcp", "tcp4", "tcp6":
		listener, err := manet.Listen(bindAddr)
		if err != nil {
			if err2 := remote.Reset(); err2 != nil {
				return nil, err2
			}
			return nil, err
		}

		listenerInfo.Address = listener.Multiaddr()
		listenerInfo.Closer = listener
		listenerInfo.Running = true

		go p2p.doAccept(&listenerInfo, remote, listener)

	default:
		return nil, errors.New("unsupported protocol: " + lnet)
	}

	return &listenerInfo, nil
}

func (p2p *P2P) doAccept(listenerInfo *ListenerInfo, remote net.Stream, listener manet.Listener) {
	defer listener.Close()

	local, err := listener.Accept()
	if err != nil {
		return
	}

	stream := StreamInfo{
		Protocol: listenerInfo.Protocol,

		LocalPeer: listenerInfo.Identity,
		LocalAddr: listenerInfo.Address,

		RemotePeer: remote.Conn().RemotePeer(),
		RemoteAddr: remote.Conn().RemoteMultiaddr(),

		Local:  local,
		Remote: remote,

		Registry: &p2p.Streams,
	}

	p2p.Streams.Register(&stream)
	stream.startStreaming()
}

// Listener wraps stream handler into a listener
type Listener interface {
	Accept() (net.Stream, error)
	Close() error
}

// P2PListener holds information on a listener
type P2PListener struct {
	peerHost p2phost.Host
	conCh    chan net.Stream
	proto    pro.ID
	ctx      context.Context
	cancel   func()
}

// Accept waits for a connection from the listener
func (il *P2PListener) Accept() (net.Stream, error) {
	select {
	case c := <-il.conCh:
		return c, nil
	case <-il.ctx.Done():
		return nil, il.ctx.Err()
	}
}

// Close closes the listener and removes stream handler
func (il *P2PListener) Close() error {
	il.cancel()
	il.peerHost.RemoveStreamHandler(il.proto)
	return nil
}

// Listen creates new P2PListener
func (p2p *P2P) registerStreamHandler(ctx2 context.Context, protocol string) (*P2PListener, error) {
	ctx, cancel := context.WithCancel(ctx2)

	list := &P2PListener{
		peerHost: p2p.peerHost,
		proto:    pro.ID(protocol),
		conCh:    make(chan net.Stream),
		ctx:      ctx,
		cancel:   cancel,
	}

	p2p.peerHost.SetStreamHandler(list.proto, func(s net.Stream) {
		select {
		case list.conCh <- s:
		case <-ctx.Done():
			s.Reset()
		}
	})

	return list, nil
}

// NewListener creates new p2p listener
func (p2p *P2P) NewListener(ctx context.Context, proto string, addr ma.Multiaddr) (*ListenerInfo, error) {
	listener, err := p2p.registerStreamHandler(ctx, proto)
	if err != nil {
		return nil, err
	}

	listenerInfo := ListenerInfo{
		Identity: p2p.identity,
		Protocol: proto,
		Address:  addr,
		Closer:   listener,
		Running:  true,
		Registry: &p2p.Listeners,
	}

	go p2p.acceptStreams(&listenerInfo, listener)

	p2p.Listeners.Register(&listenerInfo)

	return &listenerInfo, nil
}

func (p2p *P2P) acceptStreams(listenerInfo *ListenerInfo, listener Listener) {
	for listenerInfo.Running {
		remote, err := listener.Accept()
		if err != nil {
			listener.Close()
			break
		}

		local, err := manet.Dial(listenerInfo.Address)
		if err != nil {
			remote.Reset()
			continue
		}

		stream := StreamInfo{
			Protocol: listenerInfo.Protocol,

			LocalPeer: listenerInfo.Identity,
			LocalAddr: listenerInfo.Address,

			RemotePeer: remote.Conn().RemotePeer(),
			RemoteAddr: remote.Conn().RemoteMultiaddr(),

			Local:  local,
			Remote: remote,

			Registry: &p2p.Streams,
		}

		p2p.Streams.Register(&stream)
		stream.startStreaming()
	}
	p2p.Listeners.Deregister(listenerInfo.Protocol)
}

// CheckProtoExists checks whether a protocol handler is registered to
// mux handler
func (p2p *P2P) CheckProtoExists(proto string) bool {
	protos := p2p.peerHost.Mux().Protocols()

	for _, p := range protos {
		if p != proto {
			continue
		}
		return true
	}
	return false
}
