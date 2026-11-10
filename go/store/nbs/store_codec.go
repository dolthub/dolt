// Copyright 2026 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// A storeCodec decides how a NomsBlockStore writes and opens its files, and checks the manifests and files it meets.
// Dolt's plainCodec stores records as they are and checks nothing. Builds that transform records, such as by encrypting
// them, replace newStoreCodec or install a codec with a StoreOption. The per-file codecs that such a build installs
// (tableCodec, archiveCodec, spanSealer and journalCodec) are nil for files stored as they are.
type storeCodec interface {
	// journalFormat settles the format of a journaling store in |dir| before its journal is read, and returns it.
	journalFormat(nbs *NomsBlockStore, dir, format string) (string, error)
	// checkManifest checks each manifest the store reads. |exists| is false while the store has none.
	checkManifest(nbs *NomsBlockStore, exists bool, contents manifestContents) error
	// checkWrite checks that the store may take new chunks.
	checkWrite(nbs *NomsBlockStore) error
	// openAdded opens files being added to the store, as nbs.openChunkSourcesForManifestUpdate does.
	openAdded(ctx context.Context, nbs *NomsBlockStore, files map[hash.Hash]uint32, existing chunkSourceSet) (openChunkSourcesResult, error)
	// checkAdded checks, under the store's lock, files being added to its manifest. The store defers |done| over the
	// rest of the update, so it runs however the update ends and may replace the error the update returns.
	checkAdded(nbs *NomsBlockStore, added chunkSourceSet) (done func(err *error, gcGenDifferent *bool), err error)
	// checkGC checks that garbage collection may copy |src|'s chunks into |dest|.
	checkGC(src, dest *NomsBlockStore) error
	// newGCWriter returns a writer of the files that garbage collection copies chunks into.
	newGCWriter(ctx context.Context, level chunks.GCArchiveLevel) (GenericTableWriter, error)
	// readsRecords reports whether the store can read its records, which checking the refs of added files needs.
	readsRecords() bool
	// plainTables reports whether the store's table files end with a plain index and footer.
	plainTables() bool
	// writeMemTable writes |mt| as a table file, as memTable.write does.
	writeMemTable(ctx context.Context, mt *memTable, haver chunkReader, keeper keeperF, stats *Stats) (name hash.Hash, data []byte, splitOffset uint64, count uint32, gcb gcBehavior, err error)
	// openWritten sets up the reading of the table file a persister just wrote.
	openWritten(cs chunkSource) (chunkSource, error)
	// newJournalCodec returns the codec of the store's journal, or nil to write and read it as it is.
	newJournalCodec() journalCodec
}

// newStoreCodec returns the codec of each new store.
var newStoreCodec = func() storeCodec {
	return plainCodec{}
}

// tableDataEnd returns the split offset of the table file or archive |cs|, where its records end, given |splitOffset|,
// the split offset of a plain file. A |splitOffset| of 0 for a table file leaves its reader to find it from the footer.
var tableDataEnd = func(cs chunkSource, splitOffset uint64) uint64 {
	return splitOffset
}

// refusesFile reports whether |err| is a codec's refusal to open a file, which a persister returns rather than trying
// the file in another form.
var refusesFile = func(err error) bool {
	return false
}

type plainCodec struct{}

var _ storeCodec = plainCodec{}

func (plainCodec) journalFormat(_ *NomsBlockStore, _, format string) (string, error) {
	return format, nil
}

func (plainCodec) checkManifest(*NomsBlockStore, bool, manifestContents) error {
	return nil
}

func (plainCodec) checkWrite(*NomsBlockStore) error {
	return nil
}

func (plainCodec) openAdded(ctx context.Context, nbs *NomsBlockStore, files map[hash.Hash]uint32, existing chunkSourceSet) (openChunkSourcesResult, error) {
	return nbs.openChunkSourcesForManifestUpdate(ctx, files, existing)
}

func (plainCodec) checkAdded(*NomsBlockStore, chunkSourceSet) (func(*error, *bool), error) {
	return func(*error, *bool) {}, nil
}

func (plainCodec) checkGC(_, _ *NomsBlockStore) error {
	return nil
}

func (plainCodec) newGCWriter(_ context.Context, level chunks.GCArchiveLevel) (GenericTableWriter, error) {
	return newTableWriterFromArchiveLevel(level)
}

func (plainCodec) readsRecords() bool {
	return true
}

func (plainCodec) plainTables() bool {
	return true
}

func (plainCodec) writeMemTable(_ context.Context, mt *memTable, haver chunkReader, keeper keeperF, stats *Stats) (hash.Hash, []byte, uint64, uint32, gcBehavior, error) {
	return mt.write(haver, keeper, stats)
}

func (plainCodec) openWritten(cs chunkSource) (chunkSource, error) {
	return cs, nil
}

func (plainCodec) newJournalCodec() journalCodec {
	return nil
}

// persisterCodec is the codec a persister writes and opens table files with, which its store sets when it is created.
// A persister outside a store writes and opens them as they are.
type persisterCodec struct {
	codec storeCodec
}

func (p *persisterCodec) setCodec(c storeCodec) {
	p.codec = c
}

// setPersisterCodec gives persister |p| its store's codec |c|, if |p| takes one.
func setPersisterCodec(p tablePersister, c storeCodec) {
	if pc, ok := p.(interface{ setCodec(storeCodec) }); ok {
		pc.setCodec(c)
	}
}

func (p persisterCodec) openOpts(opts openOpts) openOpts {
	opts.codec = p.codec
	return opts
}

func (p persisterCodec) writeMemTable(ctx context.Context, mt *memTable, haver chunkReader, keeper keeperF, stats *Stats) (hash.Hash, []byte, uint64, uint32, gcBehavior, error) {
	if p.codec == nil {
		return mt.write(haver, keeper, stats)
	}
	return p.codec.writeMemTable(ctx, mt, haver, keeper, stats)
}

func (p persisterCodec) openWritten(cs chunkSource, err error) (chunkSource, error) {
	if p.codec == nil || err != nil {
		return cs, err
	}
	return p.codec.openWritten(cs)
}

func (p persisterCodec) newJournalCodec() journalCodec {
	if p.codec == nil {
		return nil
	}
	return p.codec.newJournalCodec()
}

// codecOpts are the open options that codecs read: the codec of the store a table file or archive is opened for,
// which its persister sets, nil outside a store, and whether the file is being added to that store.
type codecOpts struct {
	codec  storeCodec
	adding bool
}

func addedOpts(opts openOpts) openOpts {
	opts.adding = true
	return opts
}
