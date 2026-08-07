// Copyright 2020 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nbs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

type gcCopier struct {
	writer GenericTableWriter
	tfp    tableFilePersister
}

func newTableWriterFromArchiveLevel(archiveLevel chunks.GCArchiveLevel) (GenericTableWriter, error) {
	switch archiveLevel {
	case chunks.SimpleArchive:
		return NewArchiveStreamWriter("")
	case chunks.NoArchive:
		return NewCmpChunkTableWriter("")
	default:
		return nil, fmt.Errorf("invalid archive level: %d", archiveLevel)
	}
}

func newGarbageCollectionCopier(archiveLevel chunks.GCArchiveLevel, tfp tableFilePersister) (*gcCopier, error) {
	writer, err := newTableWriterFromArchiveLevel(archiveLevel)
	if err != nil {
		return nil, err
	}
	return &gcCopier{writer, tfp}, nil
}

func (gcc *gcCopier) addChunk(ctx context.Context, c ToChunker) error {
	_, err := gcc.writer.AddChunk(c)
	return err
}

// If the writer should be closed and deleted, instead of being used with
// copyTablesToDir, call this method.
func (gcc *gcCopier) cancel(_ context.Context) error {
	if gcc.writer == nil {
		return nil
	}
	err := gcc.writer.Cancel()
	if err != nil {
		return fmt.Errorf("gcCopier cancel err: %w", err)
	}
	return nil
}

// copyTablesToDir writes the GC copier's output to the persister's directory.
// It returns the table specs and a pending handle that must be kept open until
// the files are Open'd (added to openFiles). The caller must close the handle.
func (gcc *gcCopier) copyTablesToDir(ctx context.Context) (ts []tableSpec, pending io.Closer, err error) {
	// Registered before Finish so that a Finish error still drops the writer's
	// temp file. Nothing else holds a reference to |gcc.writer| once we return
	// --- for a rotating copier's child, |gcc| itself is a throwaway copy ---
	// so a missed Cancel here leaks the temp file for the life of the process.
	// The error is deliberately dropped: on the success path |ts| and |pending|
	// are valid and the caller owns the pending handle, so turning a failed
	// temp-file cleanup into a returned error would strand that handle.
	defer func() {
		_ = gcc.writer.Cancel()
		gcc.writer = nil
	}()

	var filename string
	_, filename, err = gcc.writer.Finish()
	if err != nil {
		return nil, nil, err
	}

	if gcc.writer.ChunkCount() == 0 {
		return []tableSpec{}, noopPendingHandle{}, nil
	}

	addr, ok := fileNameToAddr(filename)
	if !ok {
		return nil, nil, fmt.Errorf("invalid filename: %s", filename)
	}

	exists, closer, err := gcc.tfp.Exists(ctx, filename, uint32(gcc.writer.ChunkCount()), nil)
	if err != nil {
		return nil, nil, err
	}

	spec := tableSpec{
		name:       addr,
		chunkCount: uint32(gcc.writer.ChunkCount()),
	}

	if exists {
		return []tableSpec{spec}, closer, nil
	}

	// Attempt to rename the file to the destination if we are working with a fsTablePersister...
	if mover, ok := gcc.tfp.(movingTableFilePersister); ok {
		pending, err = mover.TryMoveCmpChunkTableWriter(ctx, filename, gcc.writer)
		if err == nil {
			return []tableSpec{spec}, pending, nil
		}
	}

	// Otherwise, write the file through CopyTableFile.
	r, err := gcc.writer.Reader()
	if err != nil {
		return nil, nil, fmt.Errorf("gc_copier, Reader() error: %w", err)
	}
	defer r.Close()
	sz := gcc.writer.FullLength()

	dataSplit, err := gcc.writer.ChunkDataLength()
	if err != nil {
		return nil, nil, fmt.Errorf("gc_copier, ChunkDataLength() error: %w", err)
	}

	pending, err = gcc.tfp.CopyTableFile(ctx, r, filename, sz, dataSplit)
	if err != nil {
		return nil, nil, fmt.Errorf("gc_copier, CopyTableFile error: %w", err)
	}

	return []tableSpec{spec}, pending, nil
}

func fileNameToAddr(fileName string) (hash.Hash, bool) {
	if len(fileName) == 32 {
		addr, ok := hash.MaybeParse(fileName)
		if ok {
			return addr, true
		}
	}
	if len(fileName) == 32+len(ArchiveFileSuffix) && strings.HasSuffix(fileName, ArchiveFileSuffix) {
		addr, ok := hash.MaybeParse(fileName[:32])
		if ok {
			return addr, true
		}
	}
	return hash.Hash{}, false
}

// newlyWrittenSources is a set of newly written chunk sources.
// It can be written to by multiple goroutines concurrently,
// so access is controlled by a mutex
type newlyWrittenSources struct {
	specs     []tableSpec
	sourceSet chunkSourceSet
	mu        sync.Mutex
}

func (sl *newlyWrittenSources) append(ctx context.Context, specs []tableSpec, nbs *NomsBlockStore) error {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.specs = append(sl.specs, specs...)
	for _, spec := range specs {
		err := nbs.tables.insertIntoChunkSourceSet(ctx, sl.sourceSet, spec, nil, nbs.stats)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sl *newlyWrittenSources) hasMany(ctx context.Context, toVisit hash.HashSet) (filtered hash.HashSet, err error) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	return sl.sourceSet.hasMany(ctx, toVisit)
}

// rotatingGCCopier is a variant of gcCopier that writes to multiple output files. Once an output file exceeds
// a threshold size, it finalizes the file and begins writing a new one.
type rotatingGCCopier struct {
	gcCopier
	maxFileSize  uint64
	bytesWritten uint64
	archiveLevel chunks.GCArchiveLevel
	dest         *NomsBlockStore
	eg           errgroup.Group

	specs newlyWrittenSources
	// seenChunks is the set of chunks already written to the in-progress table
	seenChunks hash.HashSet
	// incrementalUpdateManifest determines whether to update the manifest as each new file is created.
	// This is useful for resuming GC if it gets interrupted, but only if the manifest isn't going to be swapped at the end.
	// Thus, this should be true only when oldGen is being GCed, when not in full mode
	incrementalUpdateManifest bool
}

func newRotatingGCCopier(archiveLevel chunks.GCArchiveLevel, tfp tableFilePersister, dest *NomsBlockStore, fileSizeLimit uint64, incrementalUpdateManifest bool) (*rotatingGCCopier, error) {
	writer, err := newTableWriterFromArchiveLevel(archiveLevel)
	if err != nil {
		return nil, err
	}
	return &rotatingGCCopier{
		gcCopier:     gcCopier{writer, tfp},
		maxFileSize:  fileSizeLimit,
		bytesWritten: 0,
		archiveLevel: archiveLevel,
		dest:         dest,
		eg:           errgroup.Group{},
		specs: newlyWrittenSources{
			sourceSet: make(chunkSourceSet),
		},
		seenChunks:                hash.HashSet{},
		incrementalUpdateManifest: incrementalUpdateManifest,
	}, nil
}

func (gcc *rotatingGCCopier) addChunk(ctx context.Context, c ToChunker) error {
	_, err := gcc.writer.AddChunk(c)
	if err != nil {
		return err
	}
	gcc.seenChunks.Insert(c.Hash())
	gcc.bytesWritten += uint64(c.CompressedSize())
	if gcc.bytesWritten >= gcc.maxFileSize {
		return gcc.rotate(ctx)
	}
	return nil
}

// containsChunk checks whether the in-progress table file contains the provided chunk
func (gcc *rotatingGCCopier) containsChunk(h hash.Hash) bool {
	if gcc == nil {
		return false
	}
	return gcc.seenChunks.Has(h)
}

func (gcc *rotatingGCCopier) finalizeChildWriter(ctx context.Context, copier gcCopier) error {
	specs, pending, err := copier.copyTablesToDir(ctx)
	if err != nil {
		return err
	}
	defer pending.Close()

	if gcc.incrementalUpdateManifest {
		// Pass a nil |existing| set here. Reading gcc.specs.sourceSet is
		// unsynchronized and would race with the gcc.specs.append call below,
		// which holds gcc.specs.mu and can be running concurrently in a
		// sibling finalizeChildWriter goroutine. The set is only an
		// optimization to clone already-open sources instead of reopening
		// them, so nil is always correct: openForAdd just opens |specs| from
		// disk instead.
		err = addTableFilesToManifest(ctx, gcc.dest, specs, nil)
		if err != nil {
			return err
		}
	}
	if _, abort := os.LookupEnv("DOLT_TEST_ABORT_GC_AFTER_INCREMENTAL_FILE_WRITE"); abort {
		return fmt.Errorf("GC aborting after writing incremental table file")
	}

	return gcc.specs.append(ctx, specs, gcc.dest)
}

// rotate replaces the table file writer with a new one, and creates an async goroutine to
// finalize the original writer.
func (gcc *rotatingGCCopier) rotate(ctx context.Context) error {
	// Copy the state of gcc.gcCopier so that the child goroutine will use the current writer,
	// even after gcc.gcCopier.writer gets reassigned below.
	previousCopier := gcc.gcCopier
	// Hand ownership of the old writer to the child goroutine before spawning
	// it. If the constructor below fails we return with a nil writer rather
	// than one the child is concurrently finalizing, which a later cancel()
	// would Cancel a second time --- BufferedFileByteSink.finish is not
	// thread-safe.
	gcc.gcCopier.writer = nil
	gcc.eg.Go(func() error {
		return gcc.finalizeChildWriter(ctx, previousCopier)
	})

	writer, err := newTableWriterFromArchiveLevel(gcc.archiveLevel)
	if err != nil {
		return err
	}

	gcc.gcCopier.writer = writer
	gcc.seenChunks = hash.HashSet{}
	gcc.bytesWritten = 0
	return nil
}

func (gcc *rotatingGCCopier) finalize(ctx context.Context) (*newlyWrittenSources, error) {
	err := gcc.finalizeChildWriter(ctx, gcc.gcCopier)
	// finalizeChildWriter took ownership of the writer and cancelled it.
	gcc.writer = nil
	// Wait even when the final writer failed, so that in-flight rotate
	// goroutines finish cleaning up their own writers instead of outliving GC.
	waitErr := gcc.eg.Wait()
	if err != nil {
		return nil, errors.Join(err, waitErr)
	}
	return &gcc.specs, waitErr
}

func (gcc *rotatingGCCopier) waitForPendingChunkFiles() error {
	if gcc == nil {
		return nil
	}
	return gcc.eg.Wait()
}

func (gcc *rotatingGCCopier) cancel(ctx context.Context) error {
	gcc.specs.sourceSet.close()
	var err error
	if gcc.writer != nil {
		err = gcc.writer.Cancel()
		// Cleared unconditionally: Cancel always removes the temp file, and
		// leaving a non-nil writer behind on error just invites a double
		// Cancel from a later caller.
		gcc.writer = nil
	}
	// Detached rotate goroutines own writers we cannot reach from here, so wait
	// for them to finish cleaning up rather than letting them outlive the GC.
	return errors.Join(err, gcc.eg.Wait())
}
