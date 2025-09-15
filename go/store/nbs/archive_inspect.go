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
	"context"

	"github.com/dolthub/dolt/go/store/hash"
)

// ArchiveInspector provides a way to inspect archive files from outside the nbs package
type ArchiveInspector struct {
	reader archiveReader
}

// NewArchiveInspectorFromFile creates an ArchiveInspector from a file path with mmap enabled by default
func NewArchiveInspectorFromFile(ctx context.Context, archivePath string) (*ArchiveInspector, error) {
	return NewArchiveInspectorFromFileWithMmap(ctx, archivePath, true)
}

// NewArchiveInspectorFromFileWithMmap creates an ArchiveInspector from a file path with configurable mmap
func NewArchiveInspectorFromFileWithMmap(ctx context.Context, archivePath string, enableMmap bool) (*ArchiveInspector, error) {
	fra, err := newFileReaderAt(archivePath, enableMmap)
	if err != nil {
		return nil, err
	}

	// Use a dummy hash since we're just inspecting
	dummyHash := hash.Hash{}
	stats := &Stats{}
	
	archiveReader, err := newArchiveReader(ctx, fra, dummyHash, uint64(fra.sz), stats)
	if err != nil {
		fra.Close()
		return nil, err
	}

	return &ArchiveInspector{reader: archiveReader}, nil
}

// Close releases resources associated with the archive inspector
func (ai *ArchiveInspector) Close() error {
	return ai.reader.close()
}

// ChunkCount returns the number of chunks in the archive
func (ai *ArchiveInspector) ChunkCount() uint32 {
	return ai.reader.count()
}

// FormatVersion returns the format version of the archive
func (ai *ArchiveInspector) FormatVersion() uint8 {
	return ai.reader.footer.formatVersion
}

// FileSignature returns the file signature of the archive
func (ai *ArchiveInspector) FileSignature() string {
	return ai.reader.footer.fileSignature
}

// IndexSize returns the size of the index section in bytes
func (ai *ArchiveInspector) IndexSize() uint64 {
	return ai.reader.footer.indexSize
}

// MetadataSize returns the size of the metadata section in bytes
func (ai *ArchiveInspector) MetadataSize() uint32 {
	return ai.reader.footer.metadataSize
}

// FileSize returns the total size of the archive file
func (ai *ArchiveInspector) FileSize() uint64 {
	return ai.reader.footer.fileSize
}

// ByteSpanCount returns the number of byte spans in the archive
func (ai *ArchiveInspector) ByteSpanCount() uint32 {
	return ai.reader.footer.byteSpanCount
}

// GetMetadata retrieves the metadata from the archive as raw bytes
func (ai *ArchiveInspector) GetMetadata(ctx context.Context) ([]byte, error) {
	stats := &Stats{}
	return ai.reader.getMetadata(ctx, stats)
}