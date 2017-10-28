package corerepo

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/ipfs/go-ipfs/core"
	mfs "github.com/ipfs/go-ipfs/mfs"
	gc "github.com/ipfs/go-ipfs/pin/gc"
	repo "github.com/ipfs/go-ipfs/repo"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	humanize "gx/ipfs/QmPSBJL4momYnE7DcUyk2DVhD6rH488ZmHBGLbxNdhU44K/go-humanize"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
)

var log = logging.Logger("corerepo")

var ErrMaxStorageExceeded = errors.New("Maximum storage limit exceeded. Maybe unpin some files?")

type GC struct {
	Node       *core.IpfsNode
	Repo       repo.Repo
	StorageMax uint64
	StorageGC  uint64
	SlackGB    uint64
	Storage    uint64
}

func NewGC(n *core.IpfsNode) (*GC, error) {
	r := n.Repo
	cfg, err := r.Config()
	if err != nil {
		return nil, err
	}

	// check if cfg has these fields initialized
	// TODO: there should be a general check for all of the cfg fields
	// maybe distinguish between user config file and default struct?
	if cfg.Datastore.StorageMax == "" {
		r.SetConfigKey("Datastore.StorageMax", "10GB")
		cfg.Datastore.StorageMax = "10GB"
	}
	if cfg.Datastore.StorageGCWatermark == 0 {
		r.SetConfigKey("Datastore.StorageGCWatermark", 90)
		cfg.Datastore.StorageGCWatermark = 90
	}

	storageMax, err := humanize.ParseBytes(cfg.Datastore.StorageMax)
	if err != nil {
		return nil, err
	}
	storageGC := storageMax * uint64(cfg.Datastore.StorageGCWatermark) / 100

	// calculate the slack space between StorageMax and StorageGCWatermark
	// used to limit GC duration
	slackGB := (storageMax - storageGC) / 10e9
	if slackGB < 1 {
		slackGB = 1
	}

	return &GC{
		Node:       n,
		Repo:       r,
		StorageMax: storageMax,
		StorageGC:  storageGC,
		SlackGB:    slackGB,
	}, nil
}

func BestEffortRoots(filesRoot *mfs.Root) ([]*cid.Cid, error) {
	rootDag, err := filesRoot.GetValue().GetNode()
	if err != nil {
		return nil, err
	}

	return []*cid.Cid{rootDag.Cid()}, nil
}

func GarbageCollect(n *core.IpfsNode, ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // in case error occurs during operation
	roots, err := BestEffortRoots(n.FilesRoot)
	if err != nil {
		return err
	}
	rmed := gc.GC(ctx, n.Blockstore, n.DAG, n.Pinning, roots)

	return CollectResult(ctx, rmed, nil)
}

// CollectResult collects the output of a garbage collection run and calls the
// given callback for each object removed.  It also collects all errors into a
// MultiError which is returned after the gc is completed.
func CollectResult(ctx context.Context, gcOut <-chan gc.Result, cb func(*cid.Cid)) error {
	var errors []error
loop:
	for {
		select {
		case res, ok := <-gcOut:
			if !ok {
				break loop
			}
			if res.Error != nil {
				errors = append(errors, res.Error)
			} else if res.KeyRemoved != nil && cb != nil {
				cb(res.KeyRemoved)
			}
		case <-ctx.Done():
			errors = append(errors, ctx.Err())
			break loop
		}
	}

	switch len(errors) {
	case 0:
		return nil
	case 1:
		return errors[0]
	default:
		return NewMultiError(errors...)
	}
}

// NewMultiError creates a new MultiError object from a given slice of errors.
func NewMultiError(errs ...error) *MultiError {
	return &MultiError{errs[:len(errs)-1], errs[len(errs)-1]}
}

// MultiError contains the results of multiple errors.
type MultiError struct {
	Errors  []error
	Summary error
}

func (e *MultiError) Error() string {
	var buf bytes.Buffer
	for _, err := range e.Errors {
		buf.WriteString(err.Error())
		buf.WriteString("; ")
	}
	buf.WriteString(e.Summary.Error())
	return buf.String()
}

func GarbageCollectAsync(n *core.IpfsNode, ctx context.Context) <-chan gc.Result {
	roots, err := BestEffortRoots(n.FilesRoot)
	if err != nil {
		out := make(chan gc.Result)
		out <- gc.Result{Error: err}
		close(out)
		return out
	}

	return gc.GC(ctx, n.Blockstore, n.DAG, n.Pinning, roots)
}

func PeriodicGC(ctx context.Context, node *core.IpfsNode) error {
	cfg, err := node.Repo.Config()
	if err != nil {
		return err
	}

	if cfg.Datastore.GCPeriod == "" {
		cfg.Datastore.GCPeriod = "1h"
	}

	period, err := time.ParseDuration(cfg.Datastore.GCPeriod)
	if err != nil {
		return err
	}
	if int64(period) == 0 {
		// if duration is 0, it means GC is disabled.
		return nil
	}

	gc, err := NewGC(node)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(period):
			// the private func maybeGC doesn't compute storageMax, storageGC, slackGC so that they are not re-computed for every cycle
			if err := gc.maybeGC(ctx, 0); err != nil {
				log.Error(err)
			}
		}
	}
}

func ConditionalGC(ctx context.Context, node *core.IpfsNode, offset uint64) error {
	gc, err := NewGC(node)
	if err != nil {
		return err
	}
	return gc.maybeGC(ctx, offset)
}

func (gc *GC) maybeGC(ctx context.Context, offset uint64) error {
	storage, err := gc.Repo.GetStorageUsage()
	if err != nil {
		return err
	}

	if storage+offset > gc.StorageGC {
		if storage+offset > gc.StorageMax {
			log.Warningf("pre-GC: %s", ErrMaxStorageExceeded)
		}

		// Do GC here
		log.Info("Watermark exceeded. Starting repo GC...")
		defer log.EventBegin(ctx, "repoGC").Done()

		if err := GarbageCollect(gc.Node, ctx); err != nil {
			return err
		}
		log.Infof("Repo GC done. See `ipfs repo stat` to see how much space got freed.\n")
	}
	return nil
}
