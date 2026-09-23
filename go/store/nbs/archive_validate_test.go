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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dolthub/gozstd"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

// testArchive is an archive built in memory, with the numbers needed to locate
// the sections of its index in |data|.
type testArchive struct {
	data          []byte
	name          hash.Hash
	chunkCount    uint32
	byteSpanCount uint32
	indexStart    uint64
}

// buildValidateTestArchive writes an archive holding |n| zstd chunks and one
// dictionary, so it has n+1 byte spans and every chunk names a dictionary.
func buildValidateTestArchive(t *testing.T, n int) testArchive {
	t.Helper()

	sink := NewFixedBufferByteSink(make([]byte, 64*1024))
	aw := newArchiveWriterWithSink(sink)

	dId, err := aw.writeByteSpan(defaultDict)
	require.NoError(t, err)

	// The Prefix Map has to end up sorted, and hashWithPrefix hands out
	// ascending prefixes.
	for i := 0; i < n; i++ {
		h := hashWithPrefix(t, uint64(i+1))
		bsId, err := aw.writeByteSpan(gozstd.CompressDict(nil, []byte(fmt.Sprintf("chunk number %d, with some data to compress", i)), defaultCDict))
		require.NoError(t, err)
		require.NoError(t, aw.stageZStdChunk(h, dId, bsId))
	}

	require.NoError(t, aw.finalizeByteSpans())
	require.NoError(t, aw.writeIndex())
	require.NoError(t, aw.writeMetadata([]byte("")))
	require.NoError(t, aw.writeFooter())

	name, err := aw.getName()
	require.NoError(t, err)

	data := append([]byte(nil), sink.buff[:sink.pos]...)
	ta := testArchive{
		data:          data,
		name:          name,
		chunkCount:    uint32(n),
		byteSpanCount: uint32(n + 1),
	}
	ta.indexStart = uint64(len(data)) - archiveFooterSize - aw.indexLen
	require.Equal(t, archiveIndexSize(ta.byteSpanCount, ta.chunkCount), aw.indexLen)
	return ta
}

// open parses |ta| the way a chunk source would.
func (ta testArchive) open(t *testing.T, opts openOpts) (archiveReader, error) {
	t.Helper()
	tra := tableReaderAtAdapter{bytes.NewReader(ta.data)}
	return newArchiveReader(context.Background(), tra, ta.name, uint64(len(ta.data)), NewUnlimitedMemQuotaProvider(), opts, &Stats{})
}

// mutateIndex returns a copy of |ta| with |mut| applied to the four sections
// of its index.
func (ta testArchive) mutateIndex(mut func(spanIndex, prefixes, chunkRefs, suffixes []byte)) testArchive {
	cp := ta
	cp.data = append([]byte(nil), ta.data...)

	idx := cp.data[ta.indexStart:]
	spanIndex := idx[:uint64(ta.byteSpanCount)*uint64Size]
	idx = idx[len(spanIndex):]
	prefixes := idx[:uint64(ta.chunkCount)*uint64Size]
	idx = idx[len(prefixes):]
	chunkRefs := idx[:uint64(ta.chunkCount)*2*uint32Size]
	idx = idx[len(chunkRefs):]
	suffixes := idx[:uint64(ta.chunkCount)*hash.SuffixLen]

	mut(spanIndex, prefixes, chunkRefs, suffixes)
	return cp
}

// mutateFooter returns a copy of |ta| with |mut| applied to its footer.
func (ta testArchive) mutateFooter(mut func(footer []byte)) testArchive {
	cp := ta
	cp.data = append([]byte(nil), ta.data...)
	mut(cp.data[uint64(len(cp.data))-archiveFooterSize:])
	return cp
}

// TestValidateArchiveFooter covers the checks which run on every archive open,
// which are all footer arithmetic.
func TestValidateArchiveFooter(t *testing.T) {
	const count = 8
	ta := buildValidateTestArchive(t, count)

	ar, err := ta.open(t, openOpts{})
	require.NoError(t, err)
	require.NoError(t, ar.close())

	tests := []struct {
		name string
		mut  func(footer []byte)
	}{
		{
			name: "chunk count does not match the index size",
			mut: func(footer []byte) {
				binary.BigEndian.PutUint32(footer[afrChunkCountOffset:], count+1)
			},
		},
		{
			name: "byte span count does not match the index size",
			mut: func(footer []byte) {
				binary.BigEndian.PutUint32(footer[afrByteSpanOffset:], count+2)
			},
		},
		{
			name: "index size does not match the counts",
			mut: func(footer []byte) {
				binary.BigEndian.PutUint64(footer[afrIndexLenOffset:], archiveIndexSize(count+1, count)+1)
			},
		},
		{
			name: "index overruns the file",
			mut: func(footer []byte) {
				// The right size for the counts, but bigger than
				// the file.
				binary.BigEndian.PutUint32(footer[afrChunkCountOffset:], 1<<20)
				binary.BigEndian.PutUint32(footer[afrByteSpanOffset:], 1<<20)
				binary.BigEndian.PutUint64(footer[afrIndexLenOffset:], archiveIndexSize(1<<20, 1<<20))
			},
		},
		{
			name: "metadata overruns the file",
			mut: func(footer []byte) {
				binary.BigEndian.PutUint32(footer[afrMetaLenOffset:], 1<<30)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ta.mutateFooter(test.mut).open(t, openOpts{})
			require.ErrorIs(t, err, ErrCorruptArchiveIndex)
		})
	}
}

// TestDeepValidateArchive covers the checks which only run when an Open asks
// for them. Every mutation here leaves a footer which still adds up, so only
// the pass over the index sees it.
func TestDeepValidateArchive(t *testing.T) {
	const count = 8
	ta := buildValidateTestArchive(t, count)

	ar, err := ta.open(t, openOpts{deepValidate: true})
	require.NoError(t, err)
	require.NoError(t, ar.close())

	tests := []struct {
		name string
		mut  func(spanIndex, prefixes, chunkRefs, suffixes []byte)
	}{
		{
			name: "byte span does not advance",
			mut: func(spanIndex, _, _, _ []byte) {
				// Span 2 ends where span 1 does: a zero length span.
				copy(spanIndex[uint64Size:], spanIndex[:uint64Size])
			},
		},
		{
			name: "byte span goes backwards",
			mut: func(spanIndex, _, _, _ []byte) {
				binary.BigEndian.PutUint64(spanIndex[uint64Size:], 0)
			},
		},
		{
			name: "byte spans stop short of the data section",
			mut: func(spanIndex, _, _, _ []byte) {
				last := spanIndex[uint64(count)*uint64Size:]
				binary.BigEndian.PutUint64(last, binary.BigEndian.Uint64(last)-1)
			},
		},
		{
			name: "prefixes out of order",
			mut: func(_, prefixes, _, _ []byte) {
				a := binary.BigEndian.Uint64(prefixes)
				b := binary.BigEndian.Uint64(prefixes[uint64Size:])
				binary.BigEndian.PutUint64(prefixes, b)
				binary.BigEndian.PutUint64(prefixes[uint64Size:], a)
			},
		},
		{
			name: "chunk names the null byte span for its data",
			mut: func(_, _, chunkRefs, _ []byte) {
				binary.BigEndian.PutUint32(chunkRefs[uint32Size:], 0)
			},
		},
		{
			name: "chunk names a data byte span past the end",
			mut: func(_, _, chunkRefs, _ []byte) {
				binary.BigEndian.PutUint32(chunkRefs[uint32Size:], count+2)
			},
		},
		{
			name: "chunk names a dictionary byte span past the end",
			mut: func(_, _, chunkRefs, _ []byte) {
				binary.BigEndian.PutUint32(chunkRefs, count+2)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bad := ta.mutateIndex(test.mut)

			ar, err := bad.open(t, openOpts{})
			require.NoError(t, err, "the footer checks do not cover this")
			require.NoError(t, ar.close())

			_, err = bad.open(t, openOpts{deepValidate: true})
			require.ErrorIs(t, err, ErrCorruptArchiveIndex)
		})
	}
}

// TestArchiveChunkSourceChecksChunkCount covers the cross-check against the
// chunk count the manifest recorded for the file.
func TestArchiveChunkSourceChecksChunkCount(t *testing.T) {
	ctx := context.Background()
	const count = 8
	ta := buildValidateTestArchive(t, count)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, ta.name.String()+ArchiveFileSuffix), ta.data, 0666))

	acs, err := newArchiveChunkSource(ctx, dir, ta.name, count, NewUnlimitedMemQuotaProvider(), false, noopRefCounter{}, openOpts{deepValidate: true}, &Stats{})
	require.NoError(t, err)
	require.NoError(t, acs.close())

	_, err = newArchiveChunkSource(ctx, dir, ta.name, count+1, NewUnlimitedMemQuotaProvider(), false, noopRefCounter{}, openOpts{}, &Stats{})
	require.ErrorIs(t, err, ErrCorruptArchiveIndex)
	require.Contains(t, err.Error(), ta.name.String()+ArchiveFileSuffix)
}

// TestArchiveChunkSourceRejectsCorruptIndex opens a file whose footer adds up
// but whose index is not what was written, over both index readers.
func TestArchiveChunkSourceRejectsCorruptIndex(t *testing.T) {
	ctx := context.Background()
	const count = 8
	ta := buildValidateTestArchive(t, count).mutateIndex(func(spanIndex, _, _, _ []byte) {
		copy(spanIndex[uint64Size:], spanIndex[:uint64Size])
	})

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, ta.name.String()+ArchiveFileSuffix), ta.data, 0666))

	for _, mmap := range []bool{false, true} {
		t.Run(fmt.Sprintf("mmap=%t", mmap), func(t *testing.T) {
			acs, err := newArchiveChunkSource(ctx, dir, ta.name, count, NewUnlimitedMemQuotaProvider(), mmap, noopRefCounter{}, openOpts{}, &Stats{})
			require.NoError(t, err, "a read path open does not pay for the index scan")
			require.NoError(t, acs.close())

			_, err = newArchiveChunkSource(ctx, dir, ta.name, count, NewUnlimitedMemQuotaProvider(), mmap, noopRefCounter{}, openOpts{deepValidate: true}, &Stats{})
			require.ErrorIs(t, err, ErrCorruptArchiveIndex)
		})
	}
}

// archiveFixtureDir holds the bats fixtures' archives, which are the only real
// archives in the tree written by an older Dolt.
const archiveFixtureDir = "../../../integration-tests/bats/archive-test-repos"

// TestValidateArchiveFixtures runs both tiers of validation over the version 1
// and version 2 archives in the bats fixtures. Nothing in the format requires
// the invariants they check, so this is what says older archives satisfy them.
func TestValidateArchiveFixtures(t *testing.T) {
	ctx := context.Background()

	var files []string
	for _, pat := range []string{"*/noms/*.darc", "*/noms/oldgen/*.darc"} {
		found, err := filepath.Glob(filepath.Join(archiveFixtureDir, pat))
		require.NoError(t, err)
		files = append(files, found...)
	}
	if len(files) == 0 {
		t.Skipf("no archive fixtures under %s", archiveFixtureDir)
	}

	seen := map[byte]bool{}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			name, ok := hash.MaybeParse(strings.TrimSuffix(filepath.Base(f), ArchiveFileSuffix))
			require.True(t, ok)

			fra, err := newFileReaderAt(f, false)
			require.NoError(t, err)

			ar, err := newArchiveReader(ctx, fra, name, uint64(fra.sz), NewUnlimitedMemQuotaProvider(), openOpts{deepValidate: true}, &Stats{})
			require.NoError(t, err)
			seen[ar.footer.formatVersion] = true
			require.NoError(t, ar.close())
		})
	}

	require.True(t, seen[archiveVersionInitial], "expected a version 1 archive among the fixtures")
	require.True(t, seen[archiveVersionSnappySupport], "expected a version 2 archive among the fixtures")
}
