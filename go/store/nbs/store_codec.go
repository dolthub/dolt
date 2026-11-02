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
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

var (
	ErrEncryptionUnsupported = errors.New("encrypted databases require Dolt Enterprise")
	ErrEncryptionMismatch    = errors.New("storage file encryption does not match the database")
)

// StoreOption configures a NomsBlockStore as it opens.
type StoreOption func(*storeOptions)

type storeOptions struct {
	newCodec codecFactory
}

// codecFactory returns a store's codec and its format: |format| if the store exists, else the one it is created in.
type codecFactory func(exists bool, format string) (string, storeCodec, error)

// newStoreCodec settles the codec of a store opened without a StoreOption. Dolt Enterprise replaces it.
var newStoreCodec codecFactory = func(_ bool, format string) (string, storeCodec, error) {
	if format == constants.EncryptedFormatString {
		return "", nil, ErrEncryptionUnsupported
	}
	return format, plainCodec{}, nil
}

// storeCodec seals and opens the records of a store's files. Dolt stores them as they are; see plainCodec.
type storeCodec interface {
	encrypted() bool
	// checkAccess fails if the store can neither seal nor open records.
	checkAccess() error
	// newTableSealer returns the sealer of a new table file, or nil to write it plain.
	newTableSealer() (recordSealer, error)
	// openTable returns the opener of an encrypted table file's records.
	openTable(f fileKeys) (recordOpener, error)
	// newSpanSealer returns the sealer of a new archive, or nil to write it plain. With |recompress|, it compresses
	// sealed snappy chunks with dictionaries, as garbage collection does, rather than copying them.
	newSpanSealer(recompress bool) (spanSealer, error)
	// openArchive returns the opener of an encrypted archive's spans.
	openArchive(f fileKeys) (archiveSpans, error)
	// newJournalCodec returns the codec of a chunk journal writer.
	newJournalCodec() journalCodec
	// mergeKeyBlocks returns the key block of a file holding the records of |parts| in order.
	mergeKeyBlocks(parts []keyBlockPart) ([]byte, error)
	// checkOpened vets files being added to the store, without its lock.
	checkOpened(ctx context.Context, nbs *NomsBlockStore, added chunkSourceSet, root hash.Hash) error
	// checkAdded vets files being added under the store's lock; |commit| runs once they are in its manifest.
	checkAdded(nbs *NomsBlockStore, added chunkSourceSet) (commit func() error, err error)
}

// fileKeys are an encrypted file's key block and the positions of its records.
type fileKeys struct {
	keyBlock []byte
	count    uint32
	position func(ordinal uint32) uint64
	// adding is set for a file being added to the store from elsewhere.
	adding bool
}

// keyBlockPart is a source's key block in a conjoined file, whose records start at |start|.
type keyBlockPart struct {
	keyBlock []byte
	start    uint32
}

// recordSealer seals the records of a new file.
type recordSealer interface {
	// seal appends to |dst| the record sealing |payload|, chunk |h|'s data as the file's |ordinal|th record.
	seal(dst []byte, ordinal uint32, h hash.Hash, payload []byte) ([]byte, error)
	// adopt takes the key of |sc|, the file's |ordinal|th record, if its record can be copied as it is.
	adopt(ordinal uint32, sc SealedChunk) bool
	keyBlock() ([]byte, error)
}

// recordOpener opens the records of an encrypted file by position.
type recordOpener interface {
	// open returns the payload of chunk |h|'s |sealed| record.
	open(ctx context.Context, pos uint64, h hash.Hash, sealed []byte) ([]byte, error)
	// sealedChunk returns chunk |h| with its record still sealed, for writers that copy it.
	sealedChunk(ctx context.Context, pos uint64, h hash.Hash, sealed []byte) (SealedChunk, error)
	// storedKey returns the key that sealed the record at |pos| as stored, for remote clients.
	storedKey(pos uint64) ([]byte, error)
}

// spanRef is what an archive span holds: a dictionary, chunk |h|'s data compressed with dictionary span |dict|, or
// when |dict| is 0, chunk |h|'s snappy data followed by its CRC.
type spanRef struct {
	h      hash.Hash
	dict   uint32
	isDict bool
}

// spanSealer seals the spans of a new archive.
type spanSealer interface {
	// seal returns the bytes to store for span |ordinal|, which holds |ref|.
	seal(ordinal uint32, ref spanRef, b []byte) ([]byte, error)
	// addChunk adds a chunk of a type the writer does not know, such as a SealedChunk.
	addChunk(asw *ArchiveStreamWriter, c ToChunker) (uint32, error)
	keyBlock() ([]byte, error)
}

// archiveSpans opens the spans of an encrypted archive. Clones of its reader share it.
type archiveSpans interface {
	openDict(ctx context.Context, id uint32, stored []byte) (*DecompBundle, error)
	// chunkData returns chunk |rc|'s data, checked against its address.
	chunkData(ctx context.Context, rc resolvedChunk, dict *DecompBundle, stored []byte) ([]byte, error)
	toChunker(ctx context.Context, rc resolvedChunk, dict *DecompBundle, stored []byte) (ToChunker, error)
	// storedKey returns the key that sealed span |pos| + 1 as stored, for remote clients.
	storedKey(pos uint64) ([]byte, error)
}

// SealedChunk is a chunk read from an encrypted file with its record still sealed. ToChunk opens it.
type SealedChunk interface {
	ToChunker
	sealedRecord() []byte
}

type plainCodec struct{}

func (plainCodec) encrypted() bool {
	return false
}

func (plainCodec) checkAccess() error {
	return nil
}

func (plainCodec) newTableSealer() (recordSealer, error) {
	return nil, nil
}

func (plainCodec) openTable(fileKeys) (recordOpener, error) {
	return nil, ErrEncryptionMismatch
}

func (plainCodec) newSpanSealer(bool) (spanSealer, error) {
	return nil, nil
}

func (plainCodec) openArchive(fileKeys) (archiveSpans, error) {
	return nil, ErrEncryptionMismatch
}

func (plainCodec) newJournalCodec() journalCodec {
	return plainJournal{}
}

func (plainCodec) mergeKeyBlocks([]keyBlockPart) ([]byte, error) {
	return nil, ErrEncryptionMismatch
}

func (plainCodec) checkOpened(context.Context, *NomsBlockStore, chunkSourceSet, hash.Hash) error {
	return nil
}

func (plainCodec) checkAdded(*NomsBlockStore, chunkSourceSet) (func() error, error) {
	return func() error { return nil }, nil
}

// persisterCodec is the codec of the store a persister writes for, which the store sets once it settles it.
type persisterCodec struct {
	codec storeCodec
}

func (p *persisterCodec) setCodec(c storeCodec) {
	p.codec = c
}

func (p persisterCodec) withCodec(opts openOpts) openOpts {
	opts.codec = p.codec
	return opts
}

func (p persisterCodec) storeCodec() storeCodec {
	if p.codec == nil {
		return plainCodec{}
	}
	return p.codec
}

type codecSetter interface {
	setCodec(storeCodec)
}

// fileCodec returns the codec to open a file under: its store's, or for a file read outside a store, the default
// codec for its encryption.
func (opts openOpts) fileCodec(encrypted bool) (storeCodec, error) {
	if opts.codec != nil {
		return opts.codec, nil
	}
	format := constants.FormatDoltString
	if encrypted {
		format = constants.EncryptedFormatString
	}
	_, codec, err := newStoreCodec(true, format)
	return codec, err
}

func (nbs *NomsBlockStore) newMemTable() *memTable {
	mt := newMemTable(nbs.memtableSz)
	mt.codec = nbs.codec
	return mt
}

// checkGCCodecs checks that garbage collection from |src| to |dest| can open and seal their records.
func checkGCCodecs(src, dest storeCodec) error {
	if err := src.checkAccess(); err != nil {
		return err
	}
	if err := dest.checkAccess(); err != nil {
		return err
	}
	if src.encrypted() != dest.encrypted() {
		return fmt.Errorf("%w: garbage collection between stores of different encryption", ErrEncryptionMismatch)
	}
	return nil
}

// settleCodec settles the store's codec from its format, and returns the format.
func (nbs *NomsBlockStore) settleCodec(exists bool, format string) (string, error) {
	newCodec := nbs.newCodec
	if newCodec == nil {
		newCodec = newStoreCodec
	}
	format, codec, err := newCodec(exists, format)
	if err != nil {
		return "", err
	}
	if cs, ok := nbs.persister.(codecSetter); ok {
		cs.setCodec(codec)
	} else if codec.encrypted() && nbs.persister != nil {
		return "", fmt.Errorf("%w: storage backend %T", ErrEncryptionUnsupported, nbs.persister)
	}
	nbs.codec = codec
	return format, nil
}

// settleJournalCodec settles a journaling store's codec before its journal is read, and returns its format.
func (nbs *NomsBlockStore) settleJournalCodec(dir, format string) (string, error) {
	vers, exists, err := peekStoreVersion(dir)
	if err != nil {
		return "", err
	}
	if !exists {
		vers = format
	}
	if vers, err = nbs.settleCodec(exists, vers); err != nil {
		return "", err
	}
	if nbs.codec.encrypted() {
		nbs.staticVersion = vers
	}
	return vers, nil
}

func (nbs *NomsBlockStore) applyOptions(opts []StoreOption) {
	var o storeOptions
	for _, opt := range opts {
		opt(&o)
	}
	nbs.newCodec = o.newCodec
}

// checkManifestCodec settles the store's codec from its first manifest, and refuses a later one that differs.
func (nbs *NomsBlockStore) checkManifestCodec(exists bool, contents manifestContents) error {
	if nbs.codec != nil {
		if exists && (contents.nbfVers == constants.EncryptedFormatString) != nbs.codec.encrypted() {
			return fmt.Errorf("%w: store opened as %q, manifest is now %q", ErrEncryptionMismatch, nbs.upstream.nbfVers, contents.nbfVers)
		}
		return nil
	}
	format := nbs.upstream.nbfVers
	if exists {
		format = contents.nbfVers
	}
	format, err := nbs.settleCodec(exists, format)
	if err == nil && !exists {
		nbs.upstream.nbfVers = format
	}
	return err
}
