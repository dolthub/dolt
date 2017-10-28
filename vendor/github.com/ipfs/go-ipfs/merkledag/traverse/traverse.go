// Package traverse provides merkledag traversal functions
package traverse

import (
	"context"
	"errors"

	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
)

// Order is an identifier for traversal algorithm orders
type Order int

const (
	DFSPre  Order = iota // depth-first pre-order
	DFSPost              // depth-first post-order
	BFS                  // breadth-first
)

// Options specifies a series of traversal options
type Options struct {
	DAG     node.NodeGetter // the dagservice to fetch nodes
	Order   Order           // what order to traverse in
	Func    Func            // the function to perform at each step
	ErrFunc ErrFunc         // see ErrFunc. Optional

	SkipDuplicates bool // whether to skip duplicate nodes
}

// State is a current traversal state
type State struct {
	Node  node.Node
	Depth int
}

type traversal struct {
	opts Options
	seen map[string]struct{}
}

func (t *traversal) shouldSkip(n node.Node) (bool, error) {
	if t.opts.SkipDuplicates {
		k := n.Cid()
		if _, found := t.seen[k.KeyString()]; found {
			return true, nil
		}
		t.seen[k.KeyString()] = struct{}{}
	}

	return false, nil
}

func (t *traversal) callFunc(next State) error {
	return t.opts.Func(next)
}

// getNode returns the node for link. If it return an error,
// stop processing. if it returns a nil node, just skip it.
//
// the error handling is a little complicated.
func (t *traversal) getNode(link *node.Link) (node.Node, error) {

	getNode := func(l *node.Link) (node.Node, error) {
		next, err := l.GetNode(context.TODO(), t.opts.DAG)
		if err != nil {
			return nil, err
		}

		skip, err := t.shouldSkip(next)
		if skip {
			next = nil
		}
		return next, err
	}

	next, err := getNode(link)
	if err != nil && t.opts.ErrFunc != nil { // attempt recovery.
		err = t.opts.ErrFunc(err)
		next = nil // skip regardless
	}
	return next, err
}

// Func is the type of the function called for each dag.Node visited by Traverse.
// The traversal argument contains the current traversal state.
// If an error is returned, processing stops.
type Func func(current State) error

// If there is a problem walking to the Node, and ErrFunc is provided, Traverse
// will call ErrFunc with the error encountered. ErrFunc can decide how to handle
// that error, and return an error back to Traversal with how to proceed:
//   * nil - skip the Node and its children, but continue processing
//   * all other errors halt processing immediately.
//
// If ErrFunc is nil, Traversal will stop, as if:
//
//   opts.ErrFunc = func(err error) { return err }
//
type ErrFunc func(err error) error

func Traverse(root node.Node, o Options) error {
	t := traversal{
		opts: o,
		seen: map[string]struct{}{},
	}

	state := State{
		Node:  root,
		Depth: 0,
	}

	switch o.Order {
	default:
		return dfsPreTraverse(state, &t)
	case DFSPre:
		return dfsPreTraverse(state, &t)
	case DFSPost:
		return dfsPostTraverse(state, &t)
	case BFS:
		return bfsTraverse(state, &t)
	}
}

type dfsFunc func(state State, t *traversal) error

func dfsPreTraverse(state State, t *traversal) error {
	if err := t.callFunc(state); err != nil {
		return err
	}
	if err := dfsDescend(dfsPreTraverse, state, t); err != nil {
		return err
	}
	return nil
}

func dfsPostTraverse(state State, t *traversal) error {
	if err := dfsDescend(dfsPostTraverse, state, t); err != nil {
		return err
	}
	if err := t.callFunc(state); err != nil {
		return err
	}
	return nil
}

func dfsDescend(df dfsFunc, curr State, t *traversal) error {
	for _, l := range curr.Node.Links() {
		node, err := t.getNode(l)
		if err != nil {
			return err
		}
		if node == nil { // skip
			continue
		}

		next := State{
			Node:  node,
			Depth: curr.Depth + 1,
		}
		if err := df(next, t); err != nil {
			return err
		}
	}
	return nil
}

func bfsTraverse(root State, t *traversal) error {

	if skip, err := t.shouldSkip(root.Node); skip || err != nil {
		return err
	}

	var q queue
	q.enq(root)
	for q.len() > 0 {
		curr := q.deq()
		if curr.Node == nil {
			return errors.New("failed to dequeue though queue not empty")
		}

		// call user's func
		if err := t.callFunc(curr); err != nil {
			return err
		}

		for _, l := range curr.Node.Links() {
			node, err := t.getNode(l)
			if err != nil {
				return err
			}
			if node == nil { // skip
				continue
			}

			q.enq(State{
				Node:  node,
				Depth: curr.Depth + 1,
			})
		}
	}
	return nil
}

type queue struct {
	s []State
}

func (q *queue) enq(n State) {
	q.s = append(q.s, n)
}

func (q *queue) deq() State {
	if len(q.s) < 1 {
		return State{}
	}
	n := q.s[0]
	q.s = q.s[1:]
	return n
}

func (q *queue) len() int {
	return len(q.s)
}
