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
	"context"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/dolthub/gozstd"

	"github.com/dolthub/dolt/go/store/chunks"
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
	// MD5 is calculated on the entire output, so this hash sink wraps actual ByteSink.
	md5Summer *HashingByteSink
	// SHA512 is calculated on chunks of the output stream, so will be reset at appropriate times. This
	// sinker is what archive code writes to, and it wraps the MD5 sink.
	output           *HashingByteSink
	bytesWritten     uint64
	stagedBytes      stagedByteSpanSlice
	stagedChunks     stagedChunkRefSlice
	seenChunks       hash.HashSet
	indexLen         uint64
	metadataLen      uint32
	dataCheckSum     sha512Sum
	indexCheckSum    sha512Sum
	metadataCheckSum sha512Sum
	footerCheckSum   sha512Sum
	fullMD5          md5Sum
	workflowStage    stage
	finalPath        string
	chunkDataLength  uint64
}

/*
There is a workflow to writing an archive:
 1. writeByteSpan: Write a group of bytes to the archive. This will immediately write the bytes to the output, and
    return an ID for the byte span. Caller must keep track of this ID.
 2. stageZStdChunk: Given a hash, dictionary (as byteSpan ID), and data (as byteSpan ID), stage a chunk for writing. This
    does not write anything to disk yet. stageSnappyChunk is a similar function for snappy compressed chunks (no dictionary).
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

The archiveWriter is not thread safe, and should only be used by a single routine for its entire build workflow.
*/

// newArchiveWriter creates a new archiveWriter. Output is written to a temp file, as the file name won't be known
// until we've finished writing the footer.
func newArchiveWriter(tmpDir string) (*archiveWriter, error) {
	bs, err := NewBufferedFileByteSink(tmpDir, defaultTableSinkBlockSize, defaultChBufferSize)
	if err != nil {
		return nil, err
	}
	hbMd5 := NewMD5HashingByteSink(bs)
	hbSha := NewSHA512HashingByteSink(hbMd5)
	return &archiveWriter{
		md5Summer:  hbMd5,
		seenChunks: hash.HashSet{},
		output:     hbSha,
	}, nil
}

// newArchiveWriter - Create an *archiveWriter with the given output ByteSync. This is used for testing.
func newArchiveWriterWithSink(bs ByteSink) *archiveWriter {
	hbMd5 := NewMD5HashingByteSink(bs)
	hbSha := NewSHA512HashingByteSink(hbMd5)
	return &archiveWriter{
		md5Summer:  hbMd5,
		seenChunks: hash.HashSet{},
		output:     hbSha,
	}
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

// stageZStdChunk stages a zStd compressed chunk for writing. The |dictionary| and |data| arguments must refer to IDs
// returned by |writeByteSpan|.
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

// stageSnappyChunk stages a snappy compressed chunk for writing. This is similar to stageZStdChunk, but does not require
// the dictionary. the |dataId| must refer to an ID returned by |writeByteSpan|.
func (aw *archiveWriter) stageSnappyChunk(hash hash.Hash, dataId uint32) error {
	if aw.workflowStage != stageByteSpan {
		return fmt.Errorf("Runtime error: stageSnappyChunk called out of order")
	}

	if dataId == 0 || dataId > uint32(len(aw.stagedBytes)) {
		return ErrInvalidChunkRange
	}
	if aw.seenChunks.Has(hash) {
		return ErrDuplicateChunkWritten
	}

	aw.seenChunks.Insert(hash)
	aw.stagedChunks = append(aw.stagedChunks, stagedChunkRef{hash, 0, dataId})
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

// finalizeByteSpans should be called after all byte spans have been written. It calculates the checksum for the data
// to be written later in the footer.
func (aw *archiveWriter) finalizeByteSpans() error {
	if aw.workflowStage != stageByteSpan {
		return fmt.Errorf("Runtime error: finalizeByteSpans called out of order")
	}

	// Get the checksum for the data written so far
	aw.dataCheckSum = sha512Sum(aw.output.GetSum())
	aw.output.ResetHasher()
	aw.chunkDataLength = aw.md5Summer.Size()
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

	aw.indexLen = indexSize
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

// writeFooter writes the footer to the archive. This method is intended to be called after writeMetadata,
// and will complete the writing of bytes into the temp file.
func (aw *archiveWriter) writeFooter() error {
	if aw.workflowStage != stageFooter {
		return fmt.Errorf("Runtime error: writeFooter called out of order")
	}

	// Write out the index length
	err := aw.writeUint64(aw.indexLen)
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

	aw.fullMD5 = md5Sum(aw.md5Summer.GetSum())

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

// getName returns the hash of the footer, which is used as the fileID for the archive. This differs from the name
// on disk which has the .darc suffix.
func (aw *archiveWriter) getName() (hash.Hash, error) {
	if aw.workflowStage != stageFlush && aw.workflowStage != stageDone {
		return hash.Hash{}, fmt.Errorf("Runtime error: getName called out of order")
	}

	return hash.New(aw.footerCheckSum[:hash.ByteLen]), nil
}

// genFileName generates the file name for the archive. The path argument is the directory where the file should be written.
func (aw *archiveWriter) genFileName(path string) (string, error) {
	if aw.workflowStage != stageFlush {
		return "", fmt.Errorf("Runtime error: genFileName called out of order")
	}

	h, err := aw.getName()
	if err != nil {
		return "", err
	}

	fileName := h.String() + ArchiveFileSuffix
	fullPath := filepath.Join(path, fileName)
	return fullPath, nil
}

func (aw *archiveWriter) getChunkDataLength() (uint64, error) {
	if aw.workflowStage == stageByteSpan {
		return 0, errors.New("runtime error: chunkData not valid until finalized")
	}
	return aw.chunkDataLength, nil
}

type ArchiveStreamWriter struct {
	writer     *archiveWriter
	dictMap    map[*DecompBundle]uint32
	chunkCount int32

	// snappyQueue is a queue of CompressedChunk that have been written, but not flushed to the archive.
	// These are kept in memory until we have enough to create a compression dictionary for them (and subsequent
	// snappy chunks). When this value is nil, the snappyDict must be set (they are exclusive)
	snappyQueue *[]CompressedChunk
	snappyDict  *DecompBundle
}

func NewArchiveStreamWriter(tmpDir string) (*ArchiveStreamWriter, error) {
	writer, err := newArchiveWriter(tmpDir)
	if err != nil {
		return nil, err
	}

	sq := make([]CompressedChunk, 0, 1000)

	return &ArchiveStreamWriter{
		writer:      writer,
		dictMap:     map[*DecompBundle]uint32{},
		chunkCount:  0,
		snappyQueue: &sq,
		snappyDict:  nil,
	}, nil
}

var _ GenericTableWriter = (*ArchiveStreamWriter)(nil)

func (asw *ArchiveStreamWriter) Reader() (io.ReadCloser, error) {
	return asw.writer.output.Reader()
}

func (asw *ArchiveStreamWriter) Finish() (uint32, string, error) {
	bytesWritten := uint32(0)

	if asw.snappyQueue != nil {
		// There may be snappy chunks queued up because we didn't get enough to build a dictionary.
		for _, cc := range *asw.snappyQueue {
			dataId, err := asw.writer.writeByteSpan(cc.FullCompressedChunk)
			if err != nil {
				return bytesWritten, "", err
			}

			bytesWritten += uint32(len(cc.FullCompressedChunk))
			asw.chunkCount += 1
			err = asw.writer.stageSnappyChunk(cc.Hash(), dataId)
			if err != nil {
				return bytesWritten, "", err
			}
		}
	}

	// This will perform all the steps to construct an archive file - starting with the finalization of byte spans.
	// All writeByteSpan calls and stage* calls must be completed before this.
	err := indexFinalize(asw.writer, hash.Hash{})
	if err != nil {
		return 0, "", err
	}

	h, err := asw.writer.getName()
	if err != nil {
		return 0, "", err
	}
	return 0, h.String() + ArchiveFileSuffix, nil
}

func (asw *ArchiveStreamWriter) ChunkCount() int {
	return int(asw.chunkCount)
}

func (asw *ArchiveStreamWriter) ChunkDataLength() (uint64, error) {
	return asw.writer.getChunkDataLength()
}

func (asw *ArchiveStreamWriter) AddChunk(chunker ToChunker) (uint32, error) {
	if cc, ok := chunker.(CompressedChunk); ok {
		return asw.writeCompressedChunk(cc)
	}
	if ac, ok := chunker.(ArchiveToChunker); ok {
		return asw.writeArchiveToChunker(ac)
	}
	return 0, fmt.Errorf("Unknown chunk type: %T", chunker)
}

func (asw *ArchiveStreamWriter) FullLength() uint64 {
	return asw.writer.md5Summer.Size()
}

func (asw *ArchiveStreamWriter) GetMD5() []byte {
	return asw.writer.fullMD5[:]
}

func (asw *ArchiveStreamWriter) Cancel() error {
	rdr, err := asw.writer.output.Reader()
	if err != nil {
		return err
	}
	err = rdr.Close()
	if err != nil {
		return err
	}
	return asw.Remove()
}

func (asw *ArchiveStreamWriter) Remove() error {
	if asw.writer.finalPath == "" {
		return nil
	}
	return os.Remove(asw.writer.finalPath)
}

func (asw *ArchiveStreamWriter) writeArchiveToChunker(chunker ArchiveToChunker) (uint32, error) {
	dict := chunker.dict

	bytesWritten := uint32(0)

	var err error
	dictId, ok := asw.dictMap[dict]
	if !ok {
		// compress the raw bytes of the dictionary before persisting it.
		compressedDict := gozstd.Compress(nil, *dict.rawDictionary)

		// New dictionary. Write it out, and add id to the map.
		dictId, err = asw.writer.writeByteSpan(compressedDict)
		if err != nil {
			return 0, err
		}
		bytesWritten += uint32(len(compressedDict))
		asw.dictMap[dict] = dictId
	}

	dataId, err := asw.writer.writeByteSpan(chunker.chunkData)
	if err != nil {
		return bytesWritten, err
	}
	bytesWritten += uint32(len(chunker.chunkData))
	asw.chunkCount += 1
	return bytesWritten, asw.writer.stageZStdChunk(chunker.Hash(), dictId, dataId)
}

func (asw *ArchiveStreamWriter) writeCompressedChunk(chunker CompressedChunk) (bytesWritten uint32, err error) {
	if asw.snappyQueue != nil {
		// We have a queue of compressed chunks that we are waiting to flush.
		// Add this chunk to the queue.
		*asw.snappyQueue = append(*asw.snappyQueue, chunker)
		if len(*asw.snappyQueue) < maxSamples {
			return 0, nil
		}

		// We have enough to build a dictionary. Build it, and flush the queue.
		samples := make([]*chunks.Chunk, len(*asw.snappyQueue))
		for i, cc := range *asw.snappyQueue {
			chk, err := cc.ToChunk()
			if err != nil {
				return 0, err
			}
			samples[i] = &chk
		}
		rawDictionary := buildDictionary(samples)
		compressedDict := gozstd.Compress(nil, rawDictionary)
		asw.snappyDict, err = NewDecompBundle(compressedDict)
		if err != nil {
			return 0, err
		}

		// New dictionary. Write it out, and add id to the map.
		dictId, err := asw.writer.writeByteSpan(compressedDict)
		if err != nil {
			return 0, err
		}
		bytesWritten += uint32(len(compressedDict))
		asw.dictMap[asw.snappyDict] = dictId

		// Now stage all the
		for _, cc := range *asw.snappyQueue {
			bw := uint32(0)
			bw, err = asw.convertSnappyAndStage(cc)
			if err != nil {
				return bytesWritten, err
			}
			bytesWritten += bw
			asw.chunkCount += 1
		}
		asw.snappyQueue = nil
		return bytesWritten, err
	} else {
		// Convert this chunk from snappy to zstd, and write it out.
		bw, err := asw.convertSnappyAndStage(chunker)
		if err != nil {
			return 0, err
		}
		asw.chunkCount += 1
		return bw, nil
	}
}

// convertSnappyAndStage converts a snappy compressed chunk to zstd compression and stages it for writing.
// It returns the number of bytes written and an error if any occurred during the process. This method
// assumes that the snappyDict is already created and available in the ArchiveStreamWriter.
func (asw *ArchiveStreamWriter) convertSnappyAndStage(cc CompressedChunk) (uint32, error) {
	dictId, ok := asw.dictMap[asw.snappyDict]
	if !ok {
		return 0, errors.New("runtime error: snappyDict not found in dictMap")
	}

	h := cc.Hash()
	chk, err := cc.ToChunk()
	if err != nil {
		return 0, err
	}

	compressedData := gozstd.CompressDict(nil, chk.Data(), asw.snappyDict.cDict)

	dataId, err := asw.writer.writeByteSpan(compressedData)
	if err != nil {
		return 0, err
	}
	bytesWritten := uint32(len(compressedData))

	return bytesWritten, asw.writer.stageZStdChunk(h, dictId, dataId)
}

// conjoinAll combines two or more archiveReader instances into a single archive.
// This method takes a slice of archiveReader instances and merges their contents
// into the current archiveWriter.
//
// This method finalizes the index and footer. Effectively completes the in memory archive writing
// process, but does not write it to disk.
func (aw *archiveWriter) conjoinAll(ctx context.Context, readers []archiveReader) error {
	if len(readers) < 2 {
		return fmt.Errorf("conjoinAll requires at least 2 archive readers, got %d", len(readers))
	}

	// Sort readers by data span length (largest first)
	sort.Slice(readers, func(i, j int) bool {
		dataSpanI := readers[i].footer.dataSpan()
		dataSpanJ := readers[j].footer.dataSpan()
		return dataSpanI.length > dataSpanJ.length
	})

	stats := &Stats{}

	// Process first reader - write data blocks and collect byte span info
	firstReader := readers[0]

	// Write the entire data section from the first reader using io.Copy
	dataSpan := firstReader.footer.dataSpan()
	sectionReader := newSectionReader(ctx, firstReader.reader, int64(dataSpan.offset), int64(dataSpan.length), stats)

	written, err := io.Copy(aw.output, sectionReader)
	if err != nil {
		return fmt.Errorf("failed to copy data from first archive: %w", err)
	}
	aw.bytesWritten += uint64(written)

	// Build byte span index from first reader
	for i := uint32(1); i <= firstReader.footer.byteSpanCount; i++ {
		span := firstReader.getByteSpanByID(i)
		aw.stagedBytes = append(aw.stagedBytes, span)
	}

	// Process chunks from first reader and build hash set
	for i := 0; i < int(firstReader.footer.chunkCount); i++ {
		// Get chunk reference
		dictId, dataId := firstReader.getChunkRef(i)

		// Reconstruct the hash from prefix and suffix
		prefix := firstReader.prefixes[i]
		suffix := firstReader.getSuffixByID(uint64(i))

		// Create hash from prefix and suffix
		hashBytes := make([]byte, hash.ByteLen)
		binary.BigEndian.PutUint64(hashBytes[:hash.PrefixLen], prefix)
		copy(hashBytes[hash.PrefixLen:], suffix[:])
		chunkHash := hash.New(hashBytes)

		// Add to seen chunks and staged chunks
		aw.seenChunks.Insert(chunkHash)
		aw.stagedChunks = append(aw.stagedChunks, stagedChunkRef{
			hash:       chunkHash,
			dictionary: dictId,
			data:       dataId,
		})
	}

	// Process remaining readers
	for _, reader := range readers[1:] {
		// Track the current byte offset to adjust byte span IDs
		currentByteOffset := aw.bytesWritten

		// Write the entire data section - this is not the final solution because of duplicate chunks.
		dataSpan := reader.footer.dataSpan()
		sectionReader := newSectionReader(ctx, reader.reader, int64(dataSpan.offset), int64(dataSpan.length), stats)

		written, err := io.Copy(aw.output, sectionReader)
		if err != nil {
			return fmt.Errorf("failed to copy data from archive: %w", err)
		}
		aw.bytesWritten += uint64(written)

		// Map byte span IDs from this reader to the combined archive
		spanIdOffset := uint32(len(aw.stagedBytes))

		// Add byte spans from this reader, adjusting offsets
		for i := uint32(1); i <= reader.footer.byteSpanCount; i++ {
			span := reader.getByteSpanByID(i)
			adjustedSpan := byteSpan{
				offset: span.offset + currentByteOffset,
				length: span.length,
			}
			aw.stagedBytes = append(aw.stagedBytes, adjustedSpan)
		}

		// Process chunks from this reader
		for i := 0; i < int(reader.footer.chunkCount); i++ {
			// Get chunk reference
			dictId, dataId := reader.getChunkRef(i)

			// Reconstruct the hash from prefix and suffix
			prefix := reader.prefixes[i]
			suffix := reader.getSuffixByID(uint64(i))

			// Create hash from prefix and suffix
			hashBytes := make([]byte, hash.ByteLen)
			binary.BigEndian.PutUint64(hashBytes[:hash.PrefixLen], prefix)
			copy(hashBytes[hash.PrefixLen:], suffix[:])
			chunkHash := hash.New(hashBytes)

			// Error on duplicates for the time being.
			if aw.seenChunks.Has(chunkHash) {
				return fmt.Errorf("Duplicate chunk found during conjoinAll: %s", chunkHash.String())
			}
			aw.seenChunks.Insert(chunkHash)

			// Adjust byte span IDs for the combined archive
			adjustedDictId := dictId
			adjustedDataId := dataId
			if dictId != 0 {
				adjustedDictId = dictId + spanIdOffset
			}
			adjustedDataId = dataId + spanIdOffset

			aw.stagedChunks = append(aw.stagedChunks, stagedChunkRef{
				hash:       chunkHash,
				dictionary: adjustedDictId,
				data:       adjustedDataId,
			})
		}
	}
	
	err = indexFinalize(aw, hash.Hash{})
	if err != nil {
		return fmt.Errorf("failed to finalize archive: %w", err)
	}

	return nil
}
