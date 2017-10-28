package merkledag

import (
	"runtime"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
	blocks "gx/ipfs/QmSn9Td7xgxm9EV7iEjTckpUWmWApggzPxu7eFGWkkpwin/go-block-format"
)

// ParallelBatchCommits is the number of batch commits that can be in-flight before blocking.
// TODO(#4299): Experiment with multiple datastores, storage devices, and CPUs to find
// the right value/formula.
var ParallelBatchCommits = runtime.NumCPU() * 2

// Batch is a buffer for batching adds to a dag.
type Batch struct {
	ds *dagService

	activeCommits int
	commitError   error
	commitResults chan error

	blocks []blocks.Block
	size   int

	MaxSize   int
	MaxBlocks int
}

func (t *Batch) processResults() {
	for t.activeCommits > 0 && t.commitError == nil {
		select {
		case err := <-t.commitResults:
			t.activeCommits--
			if err != nil {
				t.commitError = err
			}
		default:
			return
		}
	}
}

func (t *Batch) asyncCommit() {
	numBlocks := len(t.blocks)
	if numBlocks == 0 || t.commitError != nil {
		return
	}
	if t.activeCommits >= ParallelBatchCommits {
		err := <-t.commitResults
		t.activeCommits--

		if err != nil {
			t.commitError = err
			return
		}
	}
	go func(b []blocks.Block) {
		_, err := t.ds.Blocks.AddBlocks(b)
		t.commitResults <- err
	}(t.blocks)

	t.activeCommits++
	t.blocks = make([]blocks.Block, 0, numBlocks)
	t.size = 0

	return
}

// Add adds a node to the batch and commits the batch if necessary.
func (t *Batch) Add(nd node.Node) (*cid.Cid, error) {
	// Not strictly necessary but allows us to catch errors early.
	t.processResults()
	if t.commitError != nil {
		return nil, t.commitError
	}

	t.blocks = append(t.blocks, nd)
	t.size += len(nd.RawData())
	if t.size > t.MaxSize || len(t.blocks) > t.MaxBlocks {
		t.asyncCommit()
	}
	return nd.Cid(), t.commitError
}

// Commit commits batched nodes.
func (t *Batch) Commit() error {
	t.asyncCommit()
	for t.activeCommits > 0 && t.commitError == nil {
		err := <-t.commitResults
		t.activeCommits--
		if err != nil {
			t.commitError = err
		}
	}

	return t.commitError
}
