// package mfs implements an in memory model of a mutable IPFS filesystem.
//
// It consists of four main structs:
// 1) The Filesystem
//        The filesystem serves as a container and entry point for various mfs filesystems
// 2) Root
//        Root represents an individual filesystem mounted within the mfs system as a whole
// 3) Directories
// 4) Files
package mfs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	dag "github.com/ipfs/go-ipfs/merkledag"
	ft "github.com/ipfs/go-ipfs/unixfs"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
)

var ErrNotExist = errors.New("no such rootfs")

var log = logging.Logger("mfs")

var ErrIsDirectory = errors.New("error: is a directory")

type childCloser interface {
	closeChild(string, node.Node, bool) error
}

type NodeType int

const (
	TFile NodeType = iota
	TDir
)

// FSNode represents any node (directory, root, or file) in the mfs filesystem
type FSNode interface {
	GetNode() (node.Node, error)
	Flush() error
	Type() NodeType
}

// Root represents the root of a filesystem tree
type Root struct {
	// node is the merkledag root
	node *dag.ProtoNode

	// val represents the node. It can either be a File or a Directory
	val FSNode

	repub *Republisher

	dserv dag.DAGService

	Type string
}

type PubFunc func(context.Context, *cid.Cid) error

// newRoot creates a new Root and starts up a republisher routine for it
func NewRoot(parent context.Context, ds dag.DAGService, node *dag.ProtoNode, pf PubFunc) (*Root, error) {

	var repub *Republisher
	if pf != nil {
		repub = NewRepublisher(parent, pf, time.Millisecond*300, time.Second*3)
		repub.setVal(node.Cid())
		go repub.Run()
	}

	root := &Root{
		node:  node,
		repub: repub,
		dserv: ds,
	}

	pbn, err := ft.FromBytes(node.Data())
	if err != nil {
		log.Error("IPNS pointer was not unixfs node")
		return nil, err
	}

	switch pbn.GetType() {
	case ft.TDirectory, ft.THAMTShard:
		rval, err := NewDirectory(parent, node.String(), node, root, ds)
		if err != nil {
			return nil, err
		}

		root.val = rval
	case ft.TFile, ft.TMetadata, ft.TRaw:
		fi, err := NewFile(node.String(), node, root, ds)
		if err != nil {
			return nil, err
		}
		root.val = fi
	default:
		return nil, fmt.Errorf("unrecognized unixfs type: %s", pbn.GetType())
	}
	return root, nil
}

func (kr *Root) GetValue() FSNode {
	return kr.val
}

func (kr *Root) Flush() error {
	nd, err := kr.GetValue().GetNode()
	if err != nil {
		return err
	}

	if kr.repub != nil {
		kr.repub.Update(nd.Cid())
	}
	return nil
}

// closeChild implements the childCloser interface, and signals to the publisher that
// there are changes ready to be published
func (kr *Root) closeChild(name string, nd node.Node, sync bool) error {
	c, err := kr.dserv.Add(nd)
	if err != nil {
		return err
	}

	if kr.repub != nil {
		kr.repub.Update(c)
	}
	return nil
}

func (kr *Root) Close() error {
	nd, err := kr.GetValue().GetNode()
	if err != nil {
		return err
	}

	if kr.repub != nil {
		kr.repub.Update(nd.Cid())
		return kr.repub.Close()
	}

	return nil
}

// Republisher manages when to publish a given entry
type Republisher struct {
	TimeoutLong  time.Duration
	TimeoutShort time.Duration
	Publish      chan struct{}
	pubfunc      PubFunc
	pubnowch     chan chan struct{}

	ctx    context.Context
	cancel func()

	lk      sync.Mutex
	val     *cid.Cid
	lastpub *cid.Cid
}

// NewRepublisher creates a new Republisher object to republish the given root
// using the given short and long time intervals
func NewRepublisher(ctx context.Context, pf PubFunc, tshort, tlong time.Duration) *Republisher {
	ctx, cancel := context.WithCancel(ctx)
	return &Republisher{
		TimeoutShort: tshort,
		TimeoutLong:  tlong,
		Publish:      make(chan struct{}, 1),
		pubfunc:      pf,
		pubnowch:     make(chan chan struct{}),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (p *Republisher) setVal(c *cid.Cid) {
	p.lk.Lock()
	defer p.lk.Unlock()
	p.val = c
}

func (p *Republisher) WaitPub() {
	p.lk.Lock()
	consistent := p.lastpub == p.val
	p.lk.Unlock()
	if consistent {
		return
	}

	wait := make(chan struct{})
	p.pubnowch <- wait
	<-wait
}

func (p *Republisher) Close() error {
	err := p.publish(p.ctx)
	p.cancel()
	return err
}

// Touch signals that an update has occurred since the last publish.
// Multiple consecutive touches may extend the time period before
// the next Publish occurs in order to more efficiently batch updates
func (np *Republisher) Update(c *cid.Cid) {
	np.setVal(c)
	select {
	case np.Publish <- struct{}{}:
	default:
	}
}

// Run is the main republisher loop
func (np *Republisher) Run() {
	for {
		select {
		case <-np.Publish:
			quick := time.After(np.TimeoutShort)
			longer := time.After(np.TimeoutLong)

		wait:
			var pubnowresp chan struct{}

			select {
			case <-np.ctx.Done():
				return
			case <-np.Publish:
				quick = time.After(np.TimeoutShort)
				goto wait
			case <-quick:
			case <-longer:
			case pubnowresp = <-np.pubnowch:
			}

			err := np.publish(np.ctx)
			if pubnowresp != nil {
				pubnowresp <- struct{}{}
			}
			if err != nil {
				log.Errorf("republishRoot error: %s", err)
			}

		case <-np.ctx.Done():
			return
		}
	}
}

func (np *Republisher) publish(ctx context.Context) error {
	np.lk.Lock()
	topub := np.val
	np.lk.Unlock()

	err := np.pubfunc(ctx, topub)
	if err != nil {
		return err
	}
	np.lk.Lock()
	np.lastpub = topub
	np.lk.Unlock()
	return nil
}
