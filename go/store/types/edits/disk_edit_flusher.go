package edits

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

type flusherEntry struct {
	path string
	id   uint64
}

// FlushResult contains the results of flushing a types.EditAccumulator to disk and the ID associated with it
type FlushResult struct {
	Edits types.EditProvider
	ID    uint64
}

// FlushResults is a sortable slice of FlushResult instances
type FlushResults []*FlushResult

func (res FlushResults) Sort() {
	sort.Slice(res, func(i, j int) bool {
		return res[i].ID < res[j].ID
	})
}

// DiskEditFlusher is a class that handles asynchronously flushing types.EditAccumulators to disk then allows getting
// an associated types.EditProvider for each flushed accumulator at a later time.
type DiskEditFlusher struct {
	ctx       context.Context
	directory string
	nbf       *types.NomsBinFormat
	vrw       types.ValueReadWriter

	eg      *errgroup.Group
	mu      *sync.Mutex
	entries []flusherEntry
}

// NewDiskEditFlusher returns a new DiskEditFlusher instance
func NewDiskEditFlusher(ctx context.Context, directory string, nbf *types.NomsBinFormat, vrw types.ValueReadWriter) *DiskEditFlusher {
	eg, egCtx := errgroup.WithContext(ctx)
	return &DiskEditFlusher{
		ctx:       egCtx,
		directory: directory,
		nbf:       nbf,
		vrw:       vrw,
		eg:        eg,
		mu:        &sync.Mutex{},
		entries:   nil,
	}
}

// Flush kicks off a new go routine to write the edits from the types.EditAccumulator to disk.  An id is provided along
// with the accumulator to allow for differentiating which results came from which flush.
func (ef *DiskEditFlusher) Flush(accumulator types.EditAccumulator, id uint64) {
	ef.eg.Go(func() error {
		path, err := FlushEditsToDisk(ef.ctx, ef.directory, accumulator)
		if err != nil {
			return err
		}

		ef.mu.Lock()
		defer ef.mu.Unlock()

		ef.entries = append(ef.entries, flusherEntry{path, id})
		return nil
	})
}

func (ef *DiskEditFlusher) resultsFromEntries(ctx context.Context, entries []flusherEntry) (FlushResults, error) {
	eps := make(FlushResults, 0, len(entries))
	for _, entry := range entries {
		ep, err := EditProviderFromDisk(ef.nbf, ef.vrw, entry.path)
		if err != nil {
			for i := range eps {
				_ = eps[i].Edits.Close(ctx)
			}

			return nil, err
		}

		eps = append(eps, &FlushResult{Edits: ep, ID: entry.id})
	}

	eps.Sort()
	return eps, nil
}

// Wait waits for asynchronous flushing tasks to complete and then returns their results. The FlushResult.Edits needs to be
// closed by the caller for each result. FlushResults will be sorted by ID
func (ef *DiskEditFlusher) Wait(ctx context.Context) (FlushResults, error) {
	err := ef.eg.Wait()
	if err != nil {
		return nil, err
	}

	ef.mu.Lock()
	defer ef.mu.Unlock()

	return ef.resultsFromEntries(ctx, ef.entries)
}

// WaitForIDs waits for asynchronous flushing tasks to complete and then returns the results of flushing the specified ids.
// The FlushResult.Edits needs to be closed by the caller for each result.  FlushResults will be sorted by ID
func (ef *DiskEditFlusher) WaitForIDs(ctx context.Context, idFilter *set.Uint64Set) (FlushResults, error) {
	err := ef.eg.Wait()
	if err != nil {
		return nil, err
	}

	ef.mu.Lock()
	defer ef.mu.Unlock()

	var entries []flusherEntry
	var excluded []flusherEntry
	for _, entry := range ef.entries {
		if idFilter.Contains(entry.id) {
			entries = append(entries, entry)
		} else {
			excluded = append(excluded, entry)
		}
	}

	if len(excluded) > 0 {
		// best effort async delete excluded files
		go func() {
			for i := range excluded {
				_ = os.Remove(excluded[i].path)
			}
		}()
	}

	return ef.resultsFromEntries(ctx, entries)
}

// EditProviderFromDisk returns a types.EditProvider instance which reads data from the specified file
func EditProviderFromDisk(nbf *types.NomsBinFormat, vrw types.ValueReadWriter, path string) (types.EditProvider, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	ep := types.TupleReaderAsEditProvider(types.NewTupleReader(nbf, vrw, f))
	return &deleteOnCloseEP{EditProvider: ep, path: path}, nil
}

// FlushEditsToDisk writes the contents of a types.EditAccumulator to disk and returns the path where the
// associated file exists.
func FlushEditsToDisk(ctx context.Context, directory string, ea types.EditAccumulator) (string, error) {
	itr, err := ea.FinishedEditing()
	if err != nil {
		return "", err
	}

	path, wr, err := openTupleWriter(directory)
	if err != nil {
		return "", err
	}

	err = flushKVPs(wr, itr)
	if err != nil {
		return "", err
	}

	err = closeTupleWriter(ctx, path, wr, err)
	if err != nil {
		return "", err
	}

	return path, nil
}

func openTupleWriter(directory string) (string, types.TupleWriteCloser, error) {
	absPath := filepath.Join(directory, uuid.New().String())
	f, err := os.OpenFile(absPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)

	if err != nil {
		return "", nil, err
	}

	return absPath, types.NewTupleWriter(f), nil
}

func flushKVPs(wr types.TupleWriter, itr types.EditProvider) error {
	// iterate over all kvps writing the key followed by the value
	for {
		kvp, err := itr.Next()

		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		k := kvp.Key.(types.Tuple)
		v := kvp.Val.(types.Tuple)
		err = wr.WriteTuples(k, v)
		if err != nil {
			return err
		}
	}
}

func closeTupleWriter(ctx context.Context, absPath string, wr types.TupleWriteCloser, err error) error {
	closeErr := wr.Close(ctx)

	if err != nil || closeErr != nil {
		if err == nil {
			err = closeErr
		}

		// an error occurred writing. Best effort deletion
		_ = os.Remove(absPath)
		return err
	}

	return nil
}

var _ types.EditProvider = (*deleteOnCloseEP)(nil)

type deleteOnCloseEP struct {
	types.EditProvider
	path string
}

func (d *deleteOnCloseEP) Close(ctx context.Context) error {
	err := d.EditProvider.Close(ctx)
	_ = os.Remove(d.path)
	return err
}
