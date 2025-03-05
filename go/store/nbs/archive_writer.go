// Copyright 2024 Dolthub, Inc.
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
	"bytes"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"sort"

	"github.com/dolthub/dolt/go/store/hash"
)

type stagedByteSpanSlice []byteSpan

type stagedChunkRef struct {
	hash             hash.Hash
	dictionary, data uint32
}
type stagedChunkRefSlice []stagedChunkRef

type stage int

const (
	stageByteSpan stage = iota
	stageIndex
	stageMetadata
	stageFooter
	stageFlush
	stageDone
)

type archiveWriter struct {
	output           *HashingByteSink
	bytesWritten     uint64
	stagedBytes      stagedByteSpanSlice
	stagedChunks     stagedChunkRefSlice
	seenChunks       hash.HashSet
	indexLen         uint32
	metadataLen      uint32
	dataCheckSum     sha512Sum
	indexCheckSum    sha512Sum
	metadataCheckSum sha512Sum
	footerCheckSum   sha512Sum
	workflowStage    stage
	finalPath        string
}

/*
There is a workflow to writing an archive:
 1. writeByteSpan: Write a group of bytes to the archive. This will immediately write the bytes to the output, and
    return an ID for the byte span. Caller must keep track of this ID.
 2. stageZStdChunk: Given a hash, dictionary (as byteSpan ID), and data (as byteSpan ID), stage a chunk for writing. This
    does not write anything to disk yet.
 3. Repeat steps 1 and 2 as necessary. You can interleave them, but all chunks must be staged before the next step.
 4. finalizeByteSpans: At this point, all byte spans have been written out, and the checksum for the data block
    is calculated. No more byte spans can be written after this step.
 5. writeIndex: Write the index to the archive. This will do all the work of writing the byte span map, prefix map,
    chunk references, and suffixes. Index checksum is calculated at the end of this step.
 6. writeMetadata: Write the metadataSpan to the archive. Calculate the metadataSpan checksum at the end of this step.
 7. writeFooter: Write the footer to the archive. This will write out the index length, byte span count, chunk count.
 8. flushToFile: Write the archive to disk and move into its new home.

When all of these steps have been completed without error, the ByteSink used to create the writer can be flushed and closed
to complete the archive writing process.
*/

// newArchiveWriter - Create an *archiveWriter with the given output ByteSync, which will be used to materialize an archive on disk.
func newArchiveWriter() (*archiveWriter, error) {
	bs, err := NewBufferedFileByteSink("", defaultTableSinkBlockSize, defaultChBufferSize)
	if err != nil {
		return nil, err
	}

	hbs := NewSHA512HashingByteSink(bs)
	return &archiveWriter{output: hbs, seenChunks: hash.HashSet{}}, nil
}

func newArchiveWriterWithSink(bs ByteSink) *archiveWriter {
	hbs := NewSHA512HashingByteSink(bs)
	return &archiveWriter{output: hbs, seenChunks: hash.HashSet{}}
}

// writeByteSpan writes a byte span to the archive, returning the ByteSpan ID if the write was successful. Note
// that writing an empty byte span is a no-op and will return 0. Also, the slice passed in is copied, so the caller
// can reuse the slice after this call.
func (aw *archiveWriter) writeByteSpan(b []byte) (uint32, error) {
	if aw.workflowStage != stageByteSpan {
		return 0, fmt.Errorf("Runtime error: writeByteSpan called out of order")
	}

	if len(b) == 0 {
		return 0, fmt.Errorf("Runtime error: empty compressed byte span")
	}

	offset := aw.bytesWritten

	written, err := aw.output.Write(b)
	if err != nil {
		return 0, err
	}
	if written != len(b) {
		return 0, io.ErrShortWrite
	}
	aw.bytesWritten += uint64(written)

	aw.stagedBytes = append(aw.stagedBytes, byteSpan{offset, uint64(written)})

	return uint32(len(aw.stagedBytes)), nil
}

func (aw *archiveWriter) chunkSeen(h hash.Hash) bool {
	return aw.seenChunks.Has(h)
}

func (aw *archiveWriter) stageZStdChunk(hash hash.Hash, dictionary, data uint32) error {
	if aw.workflowStage != stageByteSpan {
		return fmt.Errorf("Runtime error: stageZStdChunk called out of order")
	}

	if data == 0 || data > uint32(len(aw.stagedBytes)) {
		return ErrInvalidChunkRange
	}
	if aw.seenChunks.Has(hash) {
		return ErrDuplicateChunkWritten
	}
	if dictionary == 0 || dictionary > uint32(len(aw.stagedBytes)) {
		return ErrInvalidDictionaryRange
	}

	aw.seenChunks.Insert(hash)
	aw.stagedChunks = append(aw.stagedChunks, stagedChunkRef{hash, dictionary, data})
	return nil
}

func (aw *archiveWriter) stageSnappyChunk(hash hash.Hash, data uint32) error {
	if aw.workflowStage != stageByteSpan {
		return fmt.Errorf("Runtime error: stageSnappyChunk called out of order")
	}

	if data == 0 || data > uint32(len(aw.stagedBytes)) {
		return ErrInvalidChunkRange
	}
	if aw.seenChunks.Has(hash) {
		return ErrDuplicateChunkWritten
	}

	aw.seenChunks.Insert(hash)
	aw.stagedChunks = append(aw.stagedChunks, stagedChunkRef{hash, 0, data})
	return nil
}

func (scrs stagedChunkRefSlice) Len() int {
	return len(scrs)
}
func (scrs stagedChunkRefSlice) Less(i, j int) bool {
	return bytes.Compare(scrs[i].hash[:], scrs[j].hash[:]) == -1
}
func (scrs stagedChunkRefSlice) Swap(i, j int) {
	scrs[i], scrs[j] = scrs[j], scrs[i]
}

func (aw *archiveWriter) finalizeByteSpans() error {
	if aw.workflowStage != stageByteSpan {
		return fmt.Errorf("Runtime error: finalizeByteSpans called out of order")
	}

	// Get the checksum for the data written so far
	aw.dataCheckSum = sha512Sum(aw.output.GetSum())
	aw.output.ResetHasher()
	aw.workflowStage = stageIndex

	return nil
}

type streamCounter struct {
	wrapped io.Writer
	count   uint64
}

func (sc *streamCounter) Write(p []byte) (n int, err error) {
	n, err = sc.wrapped.Write(p)
	// n may be non-0, even if err is non-nil.
	sc.count += uint64(n)
	return
}

var _ io.Writer = &streamCounter{}

// writeIndex writes the index to the archive. Expects the hasher to be reset before being called, and will reset it. It
// sets the indexLen and indexCheckSum fields on the archiveWriter, and updates the bytesWritten field.
func (aw *archiveWriter) writeIndex() error {
	if aw.workflowStage != stageIndex {
		return fmt.Errorf("Runtime error: writeIndex called out of order")
	}

	indexStart := aw.bytesWritten

	// Write out the byte span end offsets
	endOffset := uint64(0)
	for _, bs := range aw.stagedBytes {
		endOffset += bs.length
		err := aw.writeUint64(endOffset)
		if err != nil {
			return err
		}
	}

	// sort stagedChunks by hash.Prefix(). Note this isn't a perfect sort for hashes, we are just grouping them by prefix
	sort.Sort(aw.stagedChunks)

	// We lay down the sorted chunk list in it's three forms.
	// Prefix Map
	for _, scr := range aw.stagedChunks {
		err := aw.writeUint64(scr.hash.Prefix())
		if err != nil {
			return err
		}
	}

	// ChunkReferences
	for _, scr := range aw.stagedChunks {
		err := aw.writeUint32(scr.dictionary)
		if err != nil {
			return err
		}

		err = aw.writeUint32(scr.data)
		if err != nil {
			return err
		}
	}

	indexSize := aw.bytesWritten - indexStart

	// Suffixes
	for _, scr := range aw.stagedChunks {
		_, err := aw.output.Write(scr.hash.Suffix())
		if err != nil {
			return err
		}
		indexSize += hash.SuffixLen
		aw.bytesWritten += hash.SuffixLen
	}

	aw.indexLen = uint32(indexSize)
	aw.indexCheckSum = sha512Sum(aw.output.GetSum())
	aw.output.ResetHasher()
	aw.workflowStage = stageMetadata

	return nil
}

// writeMetadata writes the metadataSpan to the archive. Expects the hasher to be reset before be called, and will reset it.
// It sets the metadataLen and metadataCheckSum fields on the archiveWriter, and updates the bytesWritten field.
//
// Empty input is allowed.
func (aw *archiveWriter) writeMetadata(data []byte) error {
	if aw.workflowStage != stageMetadata {
		return fmt.Errorf("Runtime error: writeMetadata called out of order")
	}

	if data == nil {
		data = []byte{}
	}

	written, err := aw.output.Write(data)
	if err != nil {
		return err
	}
	aw.bytesWritten += uint64(written)
	aw.metadataLen = uint32(written)
	aw.metadataCheckSum = sha512Sum(aw.output.GetSum())
	aw.output.ResetHasher()
	aw.workflowStage = stageFooter

	return nil
}

func (aw *archiveWriter) writeFooter() error {
	if aw.workflowStage != stageFooter {
		return fmt.Errorf("Runtime error: writeFooter called out of order")
	}

	// Write out the index length
	err := aw.writeUint32(aw.indexLen)
	if err != nil {
		return err
	}

	// Write out the byte span count
	err = aw.writeUint32(uint32(len(aw.stagedBytes)))
	if err != nil {
		return err
	}

	// Write out the chunk count
	err = aw.writeUint32(uint32(len(aw.stagedChunks)))
	if err != nil {
		return err
	}

	// Write out the metadataSpan length
	err = aw.writeUint32(aw.metadataLen)
	if err != nil {
		return err
	}

	err = aw.writeCheckSums()
	if err != nil {
		return err
	}

	// Write out the format version
	_, err = aw.output.Write([]byte{archiveFormatVersionMax})
	if err != nil {
		return err
	}
	aw.bytesWritten++

	// Write out the file signature
	_, err = aw.output.Write([]byte(archiveFileSignature))
	if err != nil {
		return err
	}
	aw.bytesWritten += archiveFileSigSize
	aw.workflowStage = stageFlush

	aw.footerCheckSum = sha512Sum(aw.output.GetSum())
	aw.output.ResetHasher()

	return nil
}

func (aw *archiveWriter) writeCheckSums() error {
	err := aw.writeSha512(aw.dataCheckSum)
	if err != nil {
		return err
	}

	err = aw.writeSha512(aw.indexCheckSum)
	if err != nil {
		return err
	}

	return aw.writeSha512(aw.metadataCheckSum)
}

func (aw *archiveWriter) writeSha512(sha sha512Sum) error {
	_, err := aw.output.Write(sha[:])
	if err != nil {
		return err
	}

	aw.bytesWritten += sha512.Size
	return nil
}

// Write a uint64 to the archive. Increments the bytesWritten field.
func (aw *archiveWriter) writeUint64(val uint64) error {
	err := binary.Write(aw.output, binary.BigEndian, val)
	if err != nil {
		return err
	}

	aw.bytesWritten += uint64Size
	return nil
}

// Write a uint32 to the archive. Increments the bytesWritten field.
func (aw *archiveWriter) writeUint32(val uint32) error {
	err := binary.Write(aw.output, binary.BigEndian, val)
	if err != nil {
		return err
	}

	aw.bytesWritten += uint32Size
	return nil
}

// Write a uint64 to the archive as a varint. This is used during the index writing process, so we expect the io.Writer
// to keep track of the written byte count.
func writeVarUint64(w io.Writer, val uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], val)
	_, err := w.Write(buf[:n])
	return err
}

// flushToFile writes the archive to disk. The input is the directory where the file should be written, the file name
// will be the footer hash + ".darc" as a suffix.
func (aw *archiveWriter) flushToFile(fullPath string) error {
	if aw.workflowStage != stageFlush {
		return fmt.Errorf("Runtime error: flushToFile called out of order")
	}

	if bs, ok := aw.output.backingSink.(*BufferedFileByteSink); ok {
		err := bs.finish()
		if err != nil {
			return err
		}
	}

	aw.finalPath = fullPath
	err := aw.output.FlushToFile(fullPath)
	if err != nil {
		return err
	}
	aw.workflowStage = stageDone
	return nil
}

func (aw *archiveWriter) getName() (hash.Hash, error) {
	if aw.workflowStage != stageFlush && aw.workflowStage != stageDone {
		return hash.Hash{}, fmt.Errorf("Runtime error: getName called out of order")
	}

	return hash.New(aw.footerCheckSum[:hash.ByteLen]), nil
}

func (aw *archiveWriter) genFileName(path string) (string, error) {
	if aw.workflowStage != stageFlush {
		return "", fmt.Errorf("Runtime error: genFileName called out of order")
	}

	h, err := aw.getName()
	if err != nil {
		return "", err
	}

	fileName := fmt.Sprintf("%s%s", h.String(), ArchiveFileSuffix)
	fullPath := filepath.Join(path, fileName)
	return fullPath, nil
}

type archiveStreamWriter struct {
	writer  *archiveWriter
	dictMap map[*DecompBundle]uint32
}

func (asw *archiveStreamWriter) writeArchiveToChunker(chunker ArchiveToChunker) error {
	dict := chunker.dict

	var err error
	dictId, ok := asw.dictMap[dict]
	if !ok {
		// Never seen this dictionary. write it out, and stick it's ID in the map.
		dictId, err = asw.writer.writeByteSpan(*dict.rawDictionary)
		if err != nil {
			return err
		}
		asw.dictMap[dict] = dictId
	}

	dataId, err := asw.writer.writeByteSpan(chunker.chunkData)
	if err != nil {
		return err
	}

	return asw.writer.stageZStdChunk(chunker.Hash(), dictId, dataId)
}

func (asw *archiveStreamWriter) writeCompressedChunkToChunker(chunker CompressedChunk) error {
	dataId, err := asw.writer.writeByteSpan(chunker.FullCompressedChunk)
	if err != nil {
		return err
	}
	return asw.writer.stageSnappyChunk(chunker.Hash(), dataId)
}
