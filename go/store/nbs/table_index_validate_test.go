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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/hash"
)

// validateTestChunks is enough chunks that the index has both an offsets1 and
// an offsets2 half, and enough that a prefix ordering violation is reachable.
func validateTestChunks(t *testing.T, n int) [][]byte {
	t.Helper()
	cs := make([][]byte, n)
	for i := range cs {
		cs[i] = []byte(fmt.Sprintf("chunk number %d, with some data to compress", i))
	}
	return cs
}

// mutateIndex returns a copy of the table file |tf| with |mut| applied to the
// three blocks of its index.
func mutateIndex(t *testing.T, tf []byte, count uint32, mut func(tuples, lengths, suffixes []byte)) []byte {
	t.Helper()
	cp := append([]byte(nil), tf...)
	idxStart := uint64(len(cp)) - (indexSize(count) + footerSize)
	tuples := cp[idxStart : idxStart+lengthsOffset(count)]
	lengths := cp[idxStart+lengthsOffset(count) : idxStart+suffixesOffset(count)]
	suffixes := cp[idxStart+suffixesOffset(count) : uint64(len(cp))-footerSize]
	mut(tuples, lengths, suffixes)
	return cp
}

func TestValidateTableIndex(t *testing.T) {
	ctx := context.Background()
	const count = 32
	tf, _, err := buildTable(validateTestChunks(t, count))
	require.NoError(t, err)

	// The unmodified file parses, so every failure below is the mutation.
	idx, err := parseTableIndexByCopy(ctx, tf, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	require.NoError(t, idx.checkTableFileSize(uint64(len(tf))))
	require.NoError(t, idx.Close())

	tests := []struct {
		name string
		mut  func(tuples, lengths, suffixes []byte)
	}{
		{
			name: "unsorted prefixes",
			mut: func(tuples, _, _ []byte) {
				// Swap the prefixes of the first two tuples, leaving the
				// ordinals a valid permutation.
				a := binary.BigEndian.Uint64(tuples[0:])
				b := binary.BigEndian.Uint64(tuples[prefixTupleSize:])
				binary.BigEndian.PutUint64(tuples[0:], b)
				binary.BigEndian.PutUint64(tuples[prefixTupleSize:], a)
			},
		},
		{
			name: "out of range ordinal",
			mut: func(tuples, _, _ []byte) {
				binary.BigEndian.PutUint32(tuples[hash.PrefixLen:], count)
			},
		},
		{
			name: "zero length chunk record",
			mut: func(_, lengths, _ []byte) {
				binary.BigEndian.PutUint32(lengths[0:], 0)
			},
		},
		{
			name: "chunk record too short for a crc",
			mut: func(_, lengths, _ []byte) {
				binary.BigEndian.PutUint32(lengths[lengthSize:], checksumSize)
			},
		},
		{
			name: "lengths shifted by one record",
			mut: func(_, lengths, _ []byte) {
				copy(lengths, lengths[lengthSize:])
				binary.BigEndian.PutUint32(lengths[uint64(count-1)*lengthSize:], 0)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bad := mutateIndex(t, tf, count, test.mut)
			_, err := parseTableIndexByCopy(ctx, bad, &UnlimitedQuotaProvider{})
			require.ErrorIs(t, err, ErrCorruptTableIndex)
		})
	}
}

// TestValidateTableIndexSizeCrossCheck covers the corruption that survives the
// structural pass: lengths which are individually plausible and sum to the
// wrong total.
func TestValidateTableIndexSizeCrossCheck(t *testing.T) {
	ctx := context.Background()
	const count = 32
	tf, _, err := buildTable(validateTestChunks(t, count))
	require.NoError(t, err)

	bad := mutateIndex(t, tf, count, func(_, lengths, _ []byte) {
		l := binary.BigEndian.Uint32(lengths[0:])
		binary.BigEndian.PutUint32(lengths[0:], l+16)
	})

	idx, err := parseTableIndexByCopy(ctx, bad, &UnlimitedQuotaProvider{})
	require.NoError(t, err, "individually plausible lengths pass the structural checks")
	defer idx.Close()
	require.ErrorIs(t, idx.checkTableFileSize(uint64(len(bad))), ErrCorruptTableIndex)
}

// repeatOrdinalMut points the second prefix tuple at the first one's chunk
// record. Every ordinal is still in range, so only the distinctness check in
// deepValidate can see it.
func repeatOrdinalMut(tuples, _, _ []byte) {
	o := binary.BigEndian.Uint32(tuples[hash.PrefixLen:])
	binary.BigEndian.PutUint32(tuples[prefixTupleSize+hash.PrefixLen:], o)
}

func TestDeepValidateTableIndex(t *testing.T) {
	ctx := context.Background()
	const count = 32
	tf, name, err := buildTable(validateTestChunks(t, count))
	require.NoError(t, err)

	idx, err := parseTableIndexByCopy(ctx, tf, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	require.NoError(t, idx.deepValidate(name))
	require.NoError(t, idx.Close())

	tests := []struct {
		name string
		mut  func(tuples, lengths, suffixes []byte)
	}{
		{
			name: "repeated ordinal",
			mut:  repeatOrdinalMut,
		},
		{
			// A suffix corruption is invisible to every other check: the
			// address it produces is as well formed as the one it replaced.
			name: "corrupt suffix",
			mut: func(_, _, suffixes []byte) {
				suffixes[0] ^= 0xff
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bad := mutateIndex(t, tf, count, test.mut)

			idx, err := parseTableIndexByCopy(ctx, bad, &UnlimitedQuotaProvider{})
			require.NoError(t, err, "the structural checks do not cover this")
			defer idx.Close()
			require.NoError(t, idx.checkTableFileSize(uint64(len(bad))))

			require.ErrorIs(t, idx.deepValidate(name), ErrCorruptTableIndex)
		})
	}
}

func TestVerifyTableIndexName(t *testing.T) {
	ctx := context.Background()
	const count = 32
	tf, name, err := buildTable(validateTestChunks(t, count))
	require.NoError(t, err)

	idx, err := parseTableIndexByCopy(ctx, tf, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	require.NoError(t, idx.verifyName(name))
	require.NoError(t, idx.Close())

	// A suffix corruption is invisible to every other check: the address it
	// produces is as well formed as the one it replaced.
	bad := mutateIndex(t, tf, count, func(_, _, suffixes []byte) {
		suffixes[0] ^= 0xff
	})
	idx, err = parseTableIndexByCopy(ctx, bad, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	defer idx.Close()
	require.NoError(t, idx.checkTableFileSize(uint64(len(bad))))
	require.ErrorIs(t, idx.verifyName(name), ErrCorruptTableIndex)
}

// TestOpenRejectsCorruptTableFile is the case that motivated these checks: a
// file which was written with a bad index but is the right length for its
// chunk count, so it slices up without complaint.
func TestOpenRejectsCorruptTableFile(t *testing.T) {
	ctx := context.Background()
	const count = 32
	tf, name, err := buildTable(validateTestChunks(t, count))
	require.NoError(t, err)

	bad := mutateIndex(t, tf, count, func(tuples, _, _ []byte) {
		// Rotate the prefix tuples. Every ordinal is still in range and still
		// used exactly once; only the prefix ordering gives it away.
		first := append([]byte(nil), tuples[:prefixTupleSize]...)
		copy(tuples, tuples[prefixTupleSize:])
		copy(tuples[uint64(count-1)*prefixTupleSize:], first)
	})

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name.String()), bad, 0666))

	q := NewUnlimitedMemQuotaProvider()
	_, err = newFileTableReader(ctx, dir, name, count, q, false, noopRefCounter{}, openOpts{}, &Stats{})
	require.ErrorIs(t, err, ErrCorruptTableIndex)
	assert.Equal(t, uint64(0), q.Usage(), "a rejected index releases its quota")
}

func TestOpenVerifiesSuffixHash(t *testing.T) {
	ctx := context.Background()
	const count = 32
	tf, name, err := buildTable(validateTestChunks(t, count))
	require.NoError(t, err)

	bad := mutateIndex(t, tf, count, func(_, _, suffixes []byte) {
		suffixes[0] ^= 0xff
	})

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name.String()), bad, 0666))

	q := NewUnlimitedMemQuotaProvider()
	cs, err := newFileTableReader(ctx, dir, name, count, q, false, noopRefCounter{}, openOpts{}, &Stats{})
	require.NoError(t, err, "the suffix hash is not checked unless the open asks for it")
	require.NoError(t, cs.close())

	_, err = newFileTableReader(ctx, dir, name, count, q, false, noopRefCounter{}, openOpts{deepValidate: true}, &Stats{})
	require.ErrorIs(t, err, ErrCorruptTableIndex)
	assert.Equal(t, uint64(0), q.Usage(), "a rejected index releases its quota")
}

// TestBSOpenChecksTableFileSize covers the size cross-check on the blobstore
// path, where the size comes back from the ranged Get rather than a stat.
func TestBSOpenChecksTableFileSize(t *testing.T) {
	ctx := context.Background()
	const count = 32
	tf, name, err := buildTable(validateTestChunks(t, count))
	require.NoError(t, err)

	bad := mutateIndex(t, tf, count, func(_, lengths, _ []byte) {
		l := binary.BigEndian.Uint32(lengths[0:])
		binary.BigEndian.PutUint32(lengths[0:], l+16)
	})

	bs := blobstore.NewInMemoryBlobstore("")
	_, err = blobstore.PutBytes(ctx, bs, name.String(), bad)
	require.NoError(t, err)

	q := NewUnlimitedMemQuotaProvider()
	_, err = newBSTableChunkSource(ctx, bs, name, count, q, openOpts{}, &Stats{})
	require.ErrorIs(t, err, ErrCorruptTableIndex)
	assert.Equal(t, uint64(0), q.Usage(), "a rejected index releases its quota")
}

// TestBSOpenVerifiesSuffixHash is TestOpenVerifiesSuffixHash for the blobstore
// path, checking that the option reaches it.
func TestBSOpenVerifiesSuffixHash(t *testing.T) {
	ctx := context.Background()
	const count = 32
	tf, name, err := buildTable(validateTestChunks(t, count))
	require.NoError(t, err)

	bad := mutateIndex(t, tf, count, func(_, _, suffixes []byte) {
		suffixes[0] ^= 0xff
	})

	bs := blobstore.NewInMemoryBlobstore("")
	_, err = blobstore.PutBytes(ctx, bs, name.String(), bad)
	require.NoError(t, err)

	q := NewUnlimitedMemQuotaProvider()
	cs, err := newBSTableChunkSource(ctx, bs, name, count, q, openOpts{}, &Stats{})
	require.NoError(t, err, "the suffix hash is not checked unless the open asks for it")
	require.NoError(t, cs.close())

	_, err = newBSTableChunkSource(ctx, bs, name, count, q, openOpts{deepValidate: true}, &Stats{})
	require.ErrorIs(t, err, ErrCorruptTableIndex)
	assert.Equal(t, uint64(0), q.Usage(), "a rejected index releases its quota")
}

// TestOpenRejectsWrongSizedTableFile covers the checkTableFileSize path
// through Open, including the quota it has to give back.
func TestOpenRejectsWrongSizedTableFile(t *testing.T) {
	ctx := context.Background()
	const count = 32
	tf, name, err := buildTable(validateTestChunks(t, count))
	require.NoError(t, err)

	bad := mutateIndex(t, tf, count, func(_, lengths, _ []byte) {
		l := binary.BigEndian.Uint32(lengths[0:])
		binary.BigEndian.PutUint32(lengths[0:], l+16)
	})

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name.String()), bad, 0666))

	q := NewUnlimitedMemQuotaProvider()
	_, err = newFileTableReader(ctx, dir, name, count, q, false, noopRefCounter{}, openOpts{}, &Stats{})
	require.ErrorIs(t, err, ErrCorruptTableIndex)
	assert.Equal(t, uint64(0), q.Usage(), "a rejected index releases its quota")
}

// benchIndexBuf builds a well formed index and footer for |n| chunks: prefixes
// ascending, ordinals the identity permutation, every chunk record 4KB.
func benchIndexBuf(n uint32) []byte {
	idxSz := indexSize(n)
	buf := make([]byte, idxSz+footerSize)
	off := uint64(0)
	for i := uint32(0); i < n; i++ {
		binary.BigEndian.PutUint64(buf[off:], uint64(i)*2)
		binary.BigEndian.PutUint32(buf[off+hash.PrefixLen:], i)
		off += prefixTupleSize
	}
	for i := uint32(0); i < n; i++ {
		binary.BigEndian.PutUint32(buf[off:], 4096)
		off += lengthSize
	}
	off += uint64(n) * hash.SuffixLen
	binary.BigEndian.PutUint32(buf[idxSz:], n)
	binary.BigEndian.PutUint64(buf[idxSz+uint32Size:], uint64(n)*4096)
	copy(buf[idxSz+uint32Size+uint64Size:], magicNumber)
	return buf
}

// BenchmarkParseTableIndex measures what validation costs against the parse it
// is attached to. All of these are O(chunkCount) passes over the same buffers.
//
// As measured on an M2 Max at 1M chunks: the parse alone is ~0.9ms, the
// structural validate adds ~1.7ms on top of it, and deepValidate is ~9.8ms ---
// ~7.8ms of suffix hashing and ~2.0ms of the ordinal permutation check.
//
// Against the 28 bytes per chunk an Open reads out of storage first --- 28MB
// here --- validate is minor over a network but comparable to a local NVMe
// read. deepValidate is the one that costs real time, which is why only the
// paths admitting a file into the store ask for it. Worth keeping honest
// numbers here: they are what decides which tier a new check belongs in.
func BenchmarkParseTableIndex(b *testing.B) {
	was := TableIndexGCFinalizerWithStackTrace
	TableIndexGCFinalizerWithStackTrace = false
	defer func() { TableIndexGCFinalizerWithStackTrace = was }()

	for _, n := range []uint32{10_000, 1_000_000} {
		tmpl := benchIndexBuf(n)
		offsetsSz := int(uint64(n-n/2) * offsetSize)

		b.Run(fmt.Sprintf("chunks=%d/parse+validate", n), func(b *testing.B) {
			q := NewUnlimitedMemQuotaProvider()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				idxBuf, err := q.AcquireQuotaByteSlice(b.Context(), len(tmpl))
				require.NoError(b, err)
				copy(idxBuf, tmpl)
				offsets, err := q.AcquireQuotaByteSlice(b.Context(), offsetsSz)
				require.NoError(b, err)
				b.StartTimer()

				idx, err := parseTableIndexWithOffsetBuff(idxBuf, offsets, q)
				require.NoError(b, err)

				b.StopTimer()
				require.NoError(b, idx.Close())
				b.StartTimer()
			}
		})

		b.Run(fmt.Sprintf("chunks=%d/validate", n), func(b *testing.B) {
			q := NewUnlimitedMemQuotaProvider()
			idxBuf, err := q.AcquireQuotaByteSlice(b.Context(), len(tmpl))
			require.NoError(b, err)
			copy(idxBuf, tmpl)
			offsets, err := q.AcquireQuotaByteSlice(b.Context(), offsetsSz)
			require.NoError(b, err)
			idx, err := parseTableIndexWithOffsetBuff(idxBuf, offsets, q)
			require.NoError(b, err)
			defer idx.Close()

			for i := 0; i < b.N; i++ {
				require.NoError(b, idx.validate())
			}
		})

		b.Run(fmt.Sprintf("chunks=%d/deepValidate", n), func(b *testing.B) {
			q := NewUnlimitedMemQuotaProvider()
			idxBuf, err := q.AcquireQuotaByteSlice(b.Context(), len(tmpl))
			require.NoError(b, err)
			copy(idxBuf, tmpl)
			offsets, err := q.AcquireQuotaByteSlice(b.Context(), offsetsSz)
			require.NoError(b, err)
			idx, err := parseTableIndexWithOffsetBuff(idxBuf, offsets, q)
			require.NoError(b, err)
			defer idx.Close()

			name := nameFromSuffixes(idx.suffixes)
			for i := 0; i < b.N; i++ {
				require.NoError(b, idx.deepValidate(name))
			}
		})

		b.Run(fmt.Sprintf("chunks=%d/verifyName", n), func(b *testing.B) {
			q := NewUnlimitedMemQuotaProvider()
			idxBuf, err := q.AcquireQuotaByteSlice(b.Context(), len(tmpl))
			require.NoError(b, err)
			copy(idxBuf, tmpl)
			offsets, err := q.AcquireQuotaByteSlice(b.Context(), offsetsSz)
			require.NoError(b, err)
			idx, err := parseTableIndexWithOffsetBuff(idxBuf, offsets, q)
			require.NoError(b, err)
			defer idx.Close()

			name := nameFromSuffixes(idx.suffixes)
			for i := 0; i < b.N; i++ {
				require.NoError(b, idx.verifyName(name))
			}
		})
	}
}
