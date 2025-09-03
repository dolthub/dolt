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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/dolthub/gozstd"

	"github.com/dolthub/dolt/go/cmd/dolt/doltversion"
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
	// SHA512 is calculated on chunks of the output stream, so will be reset at appropriate times. This
	// sinker is what archive code writes to, and it wraps the MD5 sink.
	output *HashingByteSink
	// seenChunks is used when building archives chunk-by-chunk, to ensure that we do not write the same chunk multiple
	// times. It is not used for any other purpose, and there are cases where we bypass checking it (e.g. conjoining archives).
	seenChunks hash.HashSet
	// MD5 is calculated on the entire output, so this hash sink wraps actual ByteSink.
	md5Summer       *HashingByteSink
	finalPath       string
	stagedBytes     stagedByteSpanSlice
	stagedChunks    stagedChunkRefSlice
	workflowStage   stage
	bytesWritten    uint64
	indexLen        uint64
	chunkDataLength uint64
	metadataLen     uint32
	suffixCheckSum  sha512Sum
	fullMD5         md5Sum
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

// newArchiveWriter - Create an *archiveWriter with the given output ByteSync.
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

func (aw *archiveWriter) indexFinalize(originTableFile hash.Hash) error {
	err := aw.writeIndex()
	if err != nil {
		return err
	}

	meta := map[string]string{
		amdkDoltVersion:    doltversion.Version,
		amdkConversionTime: time.Now().UTC().Format(time.RFC3339),
	}
	if !originTableFile.IsEmpty() {
		meta[amdkOriginTableFile] = originTableFile.String()
	}

	jsonData, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	err = aw.writeMetadata(jsonData)
	if err != nil {
		return err
	}

	return aw.writeFooter()
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

	// sort stagedChunks by hash. This is foundational to the archive format.
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

	// Suffixes output. This data is used to create the name for this archive.
	aw.output.ResetHasher()
	for _, scr := range aw.stagedChunks {
		_, err := aw.output.Write(scr.hash.Suffix())
		if err != nil {
			return err
		}
	}
	dataWritten := uint64(len(aw.stagedChunks)) * hash.SuffixLen
	aw.bytesWritten += dataWritten
	aw.indexLen = aw.bytesWritten - indexStart
	aw.suffixCheckSum = sha512Sum(aw.output.GetSum())

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

	err = aw.writeEmptyCheckSums()
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

	aw.fullMD5 = md5Sum(aw.md5Summer.GetSum())

	return nil
}

// writeEmptyCheckSums writes 3 empty sha512 checksum of all zeros to the archive output. This is a hold over from previous
// versions of the archive format that had checksums for data, index, and metadata. It's easier to keep the data empty
// data in the index than implement a new format version. We've never used these checksums for anything.
func (aw *archiveWriter) writeEmptyCheckSums() error {
	var zeros [(3 * sha512.Size)]byte
	written, err := aw.output.Write(zeros[:])
	if err != nil {
		return err
	}

	aw.bytesWritten += uint64(written)
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

	return hash.New(aw.suffixCheckSum[:hash.ByteLen]), nil
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
	writer  *archiveWriter
	dictMap map[*DecompBundle]uint32
	// snappyQueue is a queue of CompressedChunk that have been written, but not flushed to the archive.
	// These are kept in memory until we have enough to create a compression dictionary for them (and subsequent
	// snappy chunks). When this value is nil, the snappyDict must be set (they are exclusive)
	snappyQueue *[]CompressedChunk
	snappyDict  *DecompBundle
	chunkCount  int32
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

	// This will perform all the steps to construct an archive file.
	// All writeByteSpan calls and stage* calls must be completed before this.
	err := asw.writer.finalizeByteSpans()
	if err != nil {
		return 0, "", err
	}
	err = asw.writer.indexFinalize(hash.Hash{})
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
func (aw *archiveWriter) conjoinAll(ctx context.Context, sources []chunkSource, stats *Stats) error {
	if len(sources) < 2 {
		return fmt.Errorf("conjoinAll requires at least 2 archive readers, got %d", len(sources))
	}

	srcSz := make([]sourceWithSize, 0, len(sources))
	for _, src := range sources {
		aSrc, ok := src.(archiveChunkSource)
		if !ok {
			return fmt.Errorf("runtime error: source %T is not an archiveChunkSource", src)
		}
		dataSpan := aSrc.aRdr.footer.dataSpan()

		srcSz = append(srcSz, sourceWithSize{src, dataSpan.length})
	}

	// similar to cloud conjoin, we build the index first. It could come after in this case.
	thePlan, err := planArchiveConjoin(srcSz, stats)
	if err != nil {
		return fmt.Errorf("failed to plan archive conjoin: %w", err)
	}

	// Now that we have the plan, we slam all datablocks into the output stream then write the index last.
	for _, src := range thePlan.sources.sws {
		aSrc, ok := src.source.(archiveChunkSource)
		if !ok {
			return fmt.Errorf("runtime error: source %T is not an archiveChunkSource", src)
		}

		// Write the entire data section for the current reader.
		dataSpan := aSrc.aRdr.footer.dataSpan()
		sectionReader := newSectionReader(ctx, aSrc.aRdr.reader, int64(dataSpan.offset), int64(dataSpan.length), stats)

		written, err := io.Copy(aw.output, sectionReader)
		if err != nil {
			return fmt.Errorf("failed to copy data from archive: %w", err)
		}
		aw.bytesWritten += uint64(written)
	}

	// Now that we have all the data written, we can write out the index.
	written, err := aw.output.Write(thePlan.mergedIndex)
	if err != nil {
		return fmt.Errorf("failed to write index to archive: %w", err)
	}

	aw.bytesWritten += uint64(written)
	aw.workflowStage = stageDone

	return nil
}

type tableChunkRecord struct {
	offset uint64
	length uint32
	hash   hash.Hash
}

func planArchiveConjoin(sources []sourceWithSize, stats *Stats) (compactionPlan, error) {
	if len(sources) < 2 {
		return compactionPlan{}, fmt.Errorf("conjoinIndexes requires at least 2 archive readers, got %d", len(sources))
	}

	// place largest chunk sources at the beginning of the conjoin
	orderedSrcs := chunkSourcesByDescendingDataSize{sws: sources}
	sort.Sort(orderedSrcs)
	sources = nil

	writer := NewBlockBufferByteSink(fourMb)
	aw := newArchiveWriterWithSink(writer)

	currentDataOffset := uint64(0)
	chunkCounter := uint32(0)

	for _, src := range orderedSrcs.sws {
		reader := src.source
		arcSrc, ok := reader.(archiveChunkSource)
		if !ok {
			// When it's not an archive, we want to use the table index to extract chunk records one at a time.
			index, err := reader.index()
			if err != nil {
				return compactionPlan{}, err
			}
			chks := index.chunkCount()
			chunkCounter += chks

			chunkRecs := make([]tableChunkRecord, 0, chks)
			for i := uint32(0); i < chks; i++ {
				var h hash.Hash
				ie, err := index.indexEntry(i, &h)
				if err != nil {
					return compactionPlan{}, fmt.Errorf("failure to retrieve indexEntry(%d): %w", i, err)
				}
				chunkRecs = append(chunkRecs, tableChunkRecord{
					offset: ie.Offset(),
					length: ie.Length(),
					hash:   h,
				})
			}
			sort.Slice(chunkRecs, func(i, j int) bool {
				return chunkRecs[i].offset < chunkRecs[j].offset
			})

			for _, rec := range chunkRecs {
				adjustedSpan := byteSpan{
					offset: rec.offset + currentDataOffset,
					length: uint64(rec.length),
				}
				aw.stagedBytes = append(aw.stagedBytes, adjustedSpan)

				aw.stagedChunks = append(aw.stagedChunks, stagedChunkRef{
					hash:       rec.hash,
					dictionary: 0,
					data:       uint32(len(aw.stagedBytes)),
				})
			}
		} else {
			footer := arcSrc.aRdr.footer
			chunkCounter += footer.chunkCount

			// Map byte span IDs from this reader to the combined archive
			spanIdOffset := uint32(len(aw.stagedBytes))

			for i := uint32(1); i <= footer.byteSpanCount; i++ {
				span := arcSrc.aRdr.getByteSpanByID(i)
				adjustedSpan := byteSpan{
					offset: span.offset + currentDataOffset,
					length: span.length,
				}
				aw.stagedBytes = append(aw.stagedBytes, adjustedSpan)
			}

			for i := 0; i < int(footer.chunkCount); i++ {
				dictId, dataId := arcSrc.aRdr.getChunkRef(i)

				prefix := arcSrc.aRdr.indexReader.getPrefix(uint32(i))
				suffix := arcSrc.aRdr.indexReader.getSuffix(uint32(i))
				chunkHash := reconstructHashFromPrefixAndSuffix(prefix, suffix)

				// Add to seen chunks and staged chunks. Note that we allow duplicates here, whereas we quietly skip
				// duplicates when doing a chunk-by-chunk build of an archive.
				aw.seenChunks.Insert(chunkHash)

				// Adjust byte span IDs for the combined archive
				adjustedDictId := dictId
				if dictId != 0 {
					adjustedDictId = dictId + spanIdOffset
				}
				adjustedDataId := dataId + spanIdOffset

				aw.stagedChunks = append(aw.stagedChunks, stagedChunkRef{
					hash:       chunkHash,
					dictionary: adjustedDictId,
					data:       adjustedDataId,
				})
			}
		}
		currentDataOffset += src.dataLen
	}

	aw.bytesWritten = currentDataOffset
	// Preserve this for stat reporting as aw.bytesWritten will be updated as we write the index.
	dataBlocksLen := currentDataOffset

	// The conjoin process is a little different from the normal archive writing process. We manually stick everything
	// into the writer, and then finalize the index and footer at the end. The datablocks will be written in separately
	// after we have created the index and footer.
	//
	// So we set the workflow stage to stageIndex because we skipped the byte span insertion stage.
	aw.workflowStage = stageIndex

	err := aw.indexFinalize(hash.Hash{})
	if err != nil {
		return compactionPlan{}, fmt.Errorf("failed to finalize archive: %w", err)
	}

	name, err := aw.getName()
	if err != nil {
		return compactionPlan{}, fmt.Errorf("failed to get name of conjoined archive: %w", err)
	}

	buf := bytes.NewBuffer(make([]byte, 0, writer.pos))
	if err := writer.Flush(buf); err != nil {
		return compactionPlan{}, fmt.Errorf("failed to build index buffer while conjoining archives: %w", err)
	}

	stats.BytesPerConjoin.Sample(dataBlocksLen + uint64(len(buf.Bytes())))

	return compactionPlan{
		sources:             orderedSrcs,
		name:                name,
		suffix:              ArchiveFileSuffix,
		mergedIndex:         buf.Bytes(),
		chunkCount:          chunkCounter,
		totalCompressedData: dataBlocksLen,
	}, nil
}
