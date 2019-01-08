package p2p

import (
	"context"
	"time"

	ma "gx/ipfs/QmT4U94DnD8FRfqr21obWY32HLM5VExccPKMjQHofeYqr9/go-multiaddr"
	"gx/ipfs/QmTRhk7cgjUf2gfQ3p2M9KPECNZEW9XUrmHcFCgog4cPgB/go-libp2p-peer"
	tec "gx/ipfs/QmWHgLqrghM9zw77nF6gdvT9ExQ2RB9pLxkd8sDHZf1rWb/go-temp-err-catcher"
	"gx/ipfs/QmXuRkCR7BNQa9uqfpTiFWsTQLzmTWYg91Ja1w95gnqb6u/go-libp2p-net"
	"gx/ipfs/QmZNkThpqfVXs9GNbexPrfBbXSLNYeKrE7jwFM2oqHbyqN/go-libp2p-protocol"
	"gx/ipfs/Qmaabb1tJZ2CX5cp6MuuiGgns71NYoxdgQP6Xdid1dVceC/go-multiaddr-net"
)

// localListener manet streams and proxies them to libp2p services
type localListener struct {
	ctx context.Context

	p2p *P2P

	proto protocol.ID
	laddr ma.Multiaddr
	peer  peer.ID

	listener manet.Listener
}

// ForwardLocal creates new P2P stream to a remote listener
func (p2p *P2P) ForwardLocal(ctx context.Context, peer peer.ID, proto protocol.ID, bindAddr ma.Multiaddr) (Listener, error) {
	listener := &localListener{
		ctx:   ctx,
		p2p:   p2p,
		proto: proto,
		peer:  peer,
	}

	maListener, err := manet.Listen(bindAddr)
	if err != nil {
		return nil, err
	}

	listener.listener = maListener
	listener.laddr = maListener.Multiaddr()

	if err := p2p.ListenersLocal.Register(listener); err != nil {
		return nil, err
	}

	go listener.acceptConns()

	return listener, nil
}

func (l *localListener) dial(ctx context.Context) (net.Stream, error) {
	cctx, cancel := context.WithTimeout(ctx, time.Second*30) //TODO: configurable?
	defer cancel()

	return l.p2p.peerHost.NewStream(cctx, l.peer, l.proto)
}

func (l *localListener) acceptConns() {
	for {
		local, err := l.listener.Accept()
		if err != nil {
			if tec.ErrIsTemporary(err) {
				continue
			}
			return
		}

		go l.setupStream(local)
	}
}

func (l *localListener) setupStream(local manet.Conn) {
	remote, err := l.dial(l.ctx)
	if err != nil {
		local.Close()
		log.Warningf("failed to dial to remote %s/%s", l.peer.Pretty(), l.proto)
		return
	}

	stream := &Stream{
		Protocol: l.proto,

		OriginAddr: local.RemoteMultiaddr(),
		TargetAddr: l.TargetAddress(),
		peer:       l.peer,

		Local:  local,
		Remote: remote,

		Registry: l.p2p.Streams,
	}

	l.p2p.Streams.Register(stream)
}

func (l *localListener) close() {
	l.listener.Close()
}

func (l *localListener) Protocol() protocol.ID {
	return l.proto
}

func (l *localListener) ListenAddress() ma.Multiaddr {
	return l.laddr
}

func (l *localListener) TargetAddress() ma.Multiaddr {
	addr, err := ma.NewMultiaddr(maPrefix + l.peer.Pretty())
	if err != nil {
		panic(err)
	}
	return addr
}

func (l *localListener) key() string {
	return l.ListenAddress().String()
}
