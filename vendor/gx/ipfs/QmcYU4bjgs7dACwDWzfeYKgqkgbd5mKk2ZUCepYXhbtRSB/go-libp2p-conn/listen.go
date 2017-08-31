package conn

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	filter "gx/ipfs/QmQBB2dQLmQHJgs2gqZ3iqL2XiuCtUCvXzWt5kMXDf5Zcr/go-maddr-filter"
	transport "gx/ipfs/QmQVm7pWYKPStMeMrXNRpvAJE5rSm9ThtQoNmjNHC7sh3k/go-libp2p-transport"
	ipnet "gx/ipfs/QmQq9YzmdFdWNTDdArueGyD7L5yyiRQigrRHJnTGkxcEjT/go-libp2p-interface-pnet"
	"gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess"
	goprocessctx "gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess/context"
	msmux "gx/ipfs/QmTnsezaB1wWNRHeHnYrm8K4d5i9wtyj3GsqjC3Rt5b5v5/go-multistream"
	tec "gx/ipfs/QmWHgLqrghM9zw77nF6gdvT9ExQ2RB9pLxkd8sDHZf1rWb/go-temp-err-catcher"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ic "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
	iconn "gx/ipfs/QmfQAY7YU4fQi3sjGLs1hwkM2Aq7dxgDyoMjaKN4WBWvcB/go-libp2p-interface-conn"
)

const (
	SecioTag        = "/secio/1.0.0"
	NoEncryptionTag = "/plaintext/1.0.0"
)

var (
	connAcceptBuffer     = 32
	NegotiateReadTimeout = time.Second * 60
)

// ConnWrapper is any function that wraps a raw multiaddr connection
type ConnWrapper func(transport.Conn) transport.Conn

// listener is an object that can accept connections. It implements Listener
type listener struct {
	transport.Listener

	local  peer.ID    // LocalPeer is the identity of the local Peer
	privk  ic.PrivKey // private key to use to initialize secure conns
	protec ipnet.Protector

	filters *filter.Filters

	wrapper ConnWrapper
	catcher tec.TempErrCatcher

	proc goprocess.Process

	mux *msmux.MultistreamMuxer

	incoming chan connErr

	ctx context.Context
}

func (l *listener) teardown() error {
	defer log.Debugf("listener closed: %s %s", l.local, l.Multiaddr())
	return l.Listener.Close()
}

func (l *listener) Close() error {
	log.Debugf("listener closing: %s %s", l.local, l.Multiaddr())
	return l.proc.Close()
}

func (l *listener) String() string {
	return fmt.Sprintf("<Listener %s %s>", l.local, l.Multiaddr())
}

func (l *listener) SetAddrFilters(fs *filter.Filters) {
	l.filters = fs
}

type connErr struct {
	conn transport.Conn
	err  error
}

// Accept waits for and returns the next connection to the listener.
// Note that unfortunately this
func (l *listener) Accept() (transport.Conn, error) {
	for con := range l.incoming {
		if con.err != nil {
			return nil, con.err
		}

		c, err := newSingleConn(l.ctx, l.local, "", con.conn)
		if err != nil {
			con.conn.Close()
			if l.catcher.IsTemporary(err) {
				continue
			}
			return nil, err
		}

		if l.privk == nil || !iconn.EncryptConnections {
			log.Warning("listener %s listening INSECURELY!", l)
			return c, nil
		}
		sc, err := newSecureConn(l.ctx, l.privk, c)
		if err != nil {
			con.conn.Close()
			log.Infof("ignoring conn we failed to secure: %s %s", err, c)
			continue
		}
		return sc, nil
	}
	return nil, fmt.Errorf("listener is closed")
}

func (l *listener) Addr() net.Addr {
	return l.Listener.Addr()
}

// Multiaddr is the identity of the local Peer.
// If there is an error converting from net.Addr to ma.Multiaddr,
// the return value will be nil.
func (l *listener) Multiaddr() ma.Multiaddr {
	return l.Listener.Multiaddr()
}

// LocalPeer is the identity of the local Peer.
func (l *listener) LocalPeer() peer.ID {
	return l.local
}

func (l *listener) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"listener": map[string]interface{}{
			"peer":      l.LocalPeer(),
			"address":   l.Multiaddr(),
			"secure":    (l.privk != nil),
			"inPrivNet": (l.protec != nil),
		},
	}
}

func (l *listener) handleIncoming() {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		close(l.incoming)
	}()

	wg.Add(1)
	defer wg.Done()

	for {
		maconn, err := l.Listener.Accept()
		if err != nil {
			if l.catcher.IsTemporary(err) {
				continue
			}

			l.incoming <- connErr{err: err}
			return
		}

		log.Debugf("listener %s got connection: %s <---> %s", l, maconn.LocalMultiaddr(), maconn.RemoteMultiaddr())

		if l.filters != nil && l.filters.AddrBlocked(maconn.RemoteMultiaddr()) {
			log.Debugf("blocked connection from %s", maconn.RemoteMultiaddr())
			maconn.Close()
			continue
		}
		// If we have a wrapper func, wrap this conn
		if l.wrapper != nil {
			maconn = l.wrapper(maconn)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.protec != nil {
				pc, err := l.protec.Protect(maconn)
				if err != nil {
					maconn.Close()
					log.Warning("protector failed: ", err)
				}
				maconn = pc
			}

			maconn.SetReadDeadline(time.Now().Add(NegotiateReadTimeout))
			_, _, err = l.mux.Negotiate(maconn)
			if err != nil {
				log.Warning("incoming conn: negotiation of crypto protocol failed: ", err)
				maconn.Close()
				return
			}

			// clear read readline
			maconn.SetReadDeadline(time.Time{})

			l.incoming <- connErr{conn: maconn}
		}()
	}
}

func WrapTransportListener(ctx context.Context, ml transport.Listener, local peer.ID,
	sk ic.PrivKey) (iconn.Listener, error) {
	return WrapTransportListenerWithProtector(ctx, ml, local, sk, nil)
}

func WrapTransportListenerWithProtector(ctx context.Context, ml transport.Listener, local peer.ID,
	sk ic.PrivKey, protec ipnet.Protector) (iconn.Listener, error) {

	if protec == nil && ipnet.ForcePrivateNetwork {
		log.Error("tried to listen with no Private Network Protector but usage" +
			" of Private Networks is forced by the enviroment")
		return nil, ipnet.ErrNotInPrivateNetwork
	}

	l := &listener{
		Listener: ml,
		local:    local,
		privk:    sk,
		protec:   protec,
		mux:      msmux.NewMultistreamMuxer(),
		incoming: make(chan connErr, connAcceptBuffer),
		ctx:      ctx,
	}
	l.proc = goprocessctx.WithContextAndTeardown(ctx, l.teardown)
	l.catcher.IsTemp = func(e error) bool {
		// ignore connection breakages up to this point. but log them
		if e == io.EOF {
			log.Debugf("listener ignoring conn with EOF: %s", e)
			return true
		}

		te, ok := e.(tec.Temporary)
		if ok {
			log.Debugf("listener ignoring conn with temporary err: %s", e)
			return te.Temporary()
		}
		return false
	}

	if iconn.EncryptConnections && sk != nil {
		l.mux.AddHandler(SecioTag, nil)
	} else {
		l.mux.AddHandler(NoEncryptionTag, nil)
	}

	go l.handleIncoming()

	log.Debugf("Conn Listener on %s", l.Multiaddr())
	log.Event(ctx, "swarmListen", l)
	return l, nil
}

type ListenerConnWrapper interface {
	SetConnWrapper(ConnWrapper)
}

// SetConnWrapper assigns a maconn ConnWrapper to wrap all incoming
// connections with. MUST be set _before_ calling `Accept()`
func (l *listener) SetConnWrapper(cw ConnWrapper) {
	l.wrapper = cw
}
