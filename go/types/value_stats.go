// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"fmt"
	"io"

	"github.com/attic-labs/noms/go/hash"
	humanize "github.com/dustin/go-humanize"
	"github.com/golang/snappy"
)

type ValueStats interface {
	String() string
}

func WriteValueStats(w io.Writer, v Value, vr ValueReader) {
	switch v.Kind() {
	case BoolKind, NumberKind, StringKind, RefKind, StructKind, TypeKind:
		writeUnchunkedValueStats(w, v, vr)
	case BlobKind, ListKind, MapKind, SetKind:
		writePtreeStats(w, v, vr)
	}
}

func writeUnchunkedValueStats(w io.Writer, v Value, vr ValueReader) {
	fmt.Fprintf(w, "Kind: %s\nCompressedSize: %s\n", v.Kind().String(), humanize.Bytes(compressedSize(v)))
}

const treeRowFormat = "%5s%20s%20s%20s\n"

var treeLevelHeader = fmt.Sprintf(treeRowFormat, "Level", "Nodes", "Values/Node", "Size/Node")

func writePtreeStats(w io.Writer, v Value, vr ValueReader) {
	totalCompressedSize := uint64(0)
	totalChunks := uint64(0)

	fmt.Fprintf(w, "Kind: %s\n", v.Kind().String())
	fmt.Fprintf(w, treeLevelHeader)

	level := int64(v.(Collection).asSequence().treeLevel())
	nodes := ValueSlice{v}

	// TODO: For level 0, use NBS to fetch leaf sizes without actually reading leaf data.
	for level >= 0 {
		children := RefSlice{}
		visited := hash.HashSet{}
		chunkCount, valueCount, byteSize := uint64(0), uint64(0), uint64(0)

		for _, n := range nodes {
			chunkCount++
			if level > 0 {
				n.WalkRefs(func(r Ref) {
					children = append(children, r)
				})
			}

			s := n.(Collection).asSequence()
			valueCount += uint64(s.seqLen())

			h := n.Hash()
			if !visited.Has(h) {
				// Indexed Ptrees can share nodes within the same tree level. Only count each unique value once
				byteSize += compressedSize(n)
				visited.Insert(h)
			}
		}

		printTreeLevel(w, uint64(level), valueCount, chunkCount, byteSize)

		nodes = loadNextLevel(children, vr)
		level--
		totalCompressedSize += byteSize
		totalChunks += chunkCount
	}
}

func printTreeLevel(w io.Writer, level, values, chunks, byteSize uint64) {
	avgItems := float64(values) / float64(chunks)
	avgSize := byteSize / chunks

	fmt.Fprintf(w, treeRowFormat,
		fmt.Sprintf("%d", level),
		humanize.Comma(int64(chunks)),
		fmt.Sprintf("%.1f", avgItems),
		humanize.Bytes(avgSize))
}

func compressedSize(v Value) uint64 {
	chunk := EncodeValue(v)
	compressed := snappy.Encode(nil, chunk.Data())
	return uint64(len(compressed))
}

func loadNextLevel(refs RefSlice, vr ValueReader) ValueSlice {
	hs := make(hash.HashSlice, len(refs))
	for i, r := range refs {
		hs[i] = r.TargetHash()
	}

	// Fetch committed child sequences in a single batch
	return vr.ReadManyValues(hs)
}
