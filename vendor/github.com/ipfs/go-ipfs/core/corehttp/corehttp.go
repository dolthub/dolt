/*
Package corehttp provides utilities for the webui, gateways, and other
high-level HTTP interfaces to IPFS.
*/
package corehttp

import (
	"fmt"
	"net"
	"net/http"
	"time"

	core "github.com/ipfs/go-ipfs/core"
	"gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	manet "gx/ipfs/QmX3U3YXCQ6UYBxq2LVWF8dARS1hPUTEYLrSx654Qyxyw6/go-multiaddr-net"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

var log = logging.Logger("core/server")

// ServeOption registers any HTTP handlers it provides on the given mux.
// It returns the mux to expose to future options, which may be a new mux if it
// is interested in mediating requests to future options, or the same mux
// initially passed in if not.
type ServeOption func(*core.IpfsNode, net.Listener, *http.ServeMux) (*http.ServeMux, error)

// makeHandler turns a list of ServeOptions into a http.Handler that implements
// all of the given options, in order.
func makeHandler(n *core.IpfsNode, l net.Listener, options ...ServeOption) (http.Handler, error) {
	topMux := http.NewServeMux()
	mux := topMux
	for _, option := range options {
		var err error
		mux, err = option(n, l, mux)
		if err != nil {
			return nil, err
		}
	}
	return topMux, nil
}

// ListenAndServe runs an HTTP server listening at |listeningMultiAddr| with
// the given serve options. The address must be provided in multiaddr format.
//
// TODO intelligently parse address strings in other formats so long as they
// unambiguously map to a valid multiaddr. e.g. for convenience, ":8080" should
// map to "/ip4/0.0.0.0/tcp/8080".
func ListenAndServe(n *core.IpfsNode, listeningMultiAddr string, options ...ServeOption) error {
	addr, err := ma.NewMultiaddr(listeningMultiAddr)
	if err != nil {
		return err
	}

	list, err := manet.Listen(addr)
	if err != nil {
		return err
	}

	// we might have listened to /tcp/0 - lets see what we are listing on
	addr = list.Multiaddr()
	fmt.Printf("API server listening on %s\n", addr)

	return Serve(n, list.NetListener(), options...)
}

func Serve(node *core.IpfsNode, lis net.Listener, options ...ServeOption) error {
	handler, err := makeHandler(node, lis, options...)
	if err != nil {
		return err
	}

	addr, err := manet.FromNetAddr(lis.Addr())
	if err != nil {
		return err
	}

	// if the server exits beforehand
	var serverError error
	serverExited := make(chan struct{})

	select {
	case <-node.Process().Closing():
		return fmt.Errorf("failed to start server, process closing")
	default:
	}

	node.Process().Go(func(p goprocess.Process) {
		serverError = http.Serve(lis, handler)
		close(serverExited)
	})

	// wait for server to exit.
	select {
	case <-serverExited:

	// if node being closed before server exits, close server
	case <-node.Process().Closing():
		log.Infof("server at %s terminating...", addr)

		lis.Close()

	outer:
		for {
			// wait until server exits
			select {
			case <-serverExited:
				// if the server exited as we are closing, we really dont care about errors
				serverError = nil
				break outer
			case <-time.After(5 * time.Second):
				log.Infof("waiting for server at %s to terminate...", addr)
			}
		}
	}

	log.Infof("server at %s terminated", addr)
	return serverError
}
