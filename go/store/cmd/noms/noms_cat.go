// Copyright 2019 Dolthub, Inc.
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

package main

import (
	"context"
	"encoding/base32"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/golang/snappy"
	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/cmd/noms/util"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	u64Size        = 8
	u32Size        = 4
	crcSize        = u32Size
	prefixSize     = u64Size
	ordinalSize    = u32Size
	chunkSizeSize  = u32Size
	suffixSize     = 12
	chunkCntSize   = u32Size
	totalUncmpSize = u64Size
	magicSize      = u64Size

	magicNumber uint64 = 0xffb5d8c22463ee50
)

var (
	catRaw        = false
	catDecomp     = false
	catNoShow     = false
	catNoRefs     = false
	catHashesOnly = false
)

var nomsCat = &util.Command{
	Run:       runCat,
	UsageLine: "cat <file>",
	Short:     "Print the contents of a chunk file",
	Long:      "Print the contents of a chunk file",
	Flags:     setupCatFlags,
	Nargs:     1,
}

func setupCatFlags() *flag.FlagSet {
	catFlagSet := flag.NewFlagSet("cat", flag.ExitOnError)
	catFlagSet.BoolVar(&catRaw, "raw", false, "If true, includes the raw binary version of each chunk in the nbs file")
	catFlagSet.BoolVar(&catNoShow, "no-show", false, "If true, skips printing of the value")
	catFlagSet.BoolVar(&catNoRefs, "no-refs", false, "If true, skips printing of the refs")
	catFlagSet.BoolVar(&catHashesOnly, "hashes-only", false, "If true, only prints the b32 hashes")
	catFlagSet.BoolVar(&catDecomp, "decompressed", false, "If true, includes the decompressed binary version of each chunk in the nbs file")
	return catFlagSet
}

type footer struct {
	chunkCnt   uint32
	uncompSize uint64
	magicMatch bool
}

type prefixIndex struct {
	hashPrefix []byte
	chunkIndex uint32
}

type chunkData struct {
	compressed    []byte
	uncompressed  []byte
	dataOffset    uint64
	crc           uint32
	decompSuccess bool
}

func runCat(ctx context.Context, args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Not enough arguments")
		return 0
	}

	chunkFile := args[0]
	_, err := os.Stat(chunkFile)

	if err != nil {
		fmt.Fprintln(os.Stderr, chunkFile+" does not exist")
		return 1
	}

	fileBytes, err := os.ReadFile(chunkFile)

	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to read "+chunkFile, err)
		return 1
	}

	//read the file backwards
	pos := len(fileBytes)
	pos, footer := parseFooter(fileBytes, pos)
	pos, suffixes := parseChunkSuffixes(fileBytes, pos, int(footer.chunkCnt))
	pos, sizes := parseChunkSizes(fileBytes, pos, int(footer.chunkCnt))
	pos, pi := parsePrefixIndices(fileBytes, pos, int(footer.chunkCnt))
	pos, cd := parseChunks(fileBytes, pos, sizes)

	fmt.Println("Info for file", chunkFile+":")
	fmt.Printf("    chunk count:                     %d\n", footer.chunkCnt)
	fmt.Printf("    total uncompressed chunk size:   %d\n", footer.uncompSize)
	fmt.Printf("    magic number matches:            %t\n", footer.magicMatch)
	fmt.Println()

	fmt.Println("Prefix Indices:")
	for i, currPI := range pi {
		var hashData [20]byte

		cidx := currPI.chunkIndex
		copy(hashData[:], currPI.hashPrefix)
		copy(hashData[prefixSize:], suffixes[cidx])
		b32Hash := b32Str(hashData[:])

		currCD := cd[cidx]

		if catHashesOnly {
			fmt.Println("hash:", b32Hash, "offset:", currCD.dataOffset, "size:", len(currCD.compressed))
			continue
		}

		fmt.Printf("    prefixIndex[%d].hash:        (HEX) %s    (B32) %s\n", i, hexStr(hashData[:]), b32Hash)
		fmt.Printf("    prefixIndex[%d].hash.prefix: (HEX) %s\n", i, hexStr(currPI.hashPrefix))
		fmt.Printf("    prefixIndex[%d].hash.suffix: (HEX) %s\n", i, hexStr(suffixes[cidx]))
		fmt.Println()

		fmt.Printf("    prefixIndex[%d] references chunk[%d]:\n", i, cidx)

		chunk := chunks.NewChunkWithHash(hashData, currCD.uncompressed)

		//Want a clean db every loop
		sp, _ := spec.ForDatabase("mem")
		vrw := sp.GetVRW(ctx)
		waf := types.WalkAddrsForNBF(vrw.Format(), nil)

		fmt.Printf("        chunk[%d].raw.len:     %d\n", cidx, len(currCD.compressed))

		if catRaw {
			fmt.Printf("        chunk[%d].raw.crc:     %08x\n", cidx, currCD.crc)
			fmt.Printf("        chunk[%d].raw.data:\n", cidx)
			fmt.Println(hexView(currCD.compressed, "                               "))
		}

		fmt.Printf("        chunk[%d].decomp.len:  %d\n", cidx, len(currCD.uncompressed))

		if catDecomp {
			fmt.Printf("        chunk[%d].decomp.data:\n", cidx)
			fmt.Println(hexView(currCD.uncompressed, "                               "))
		}

		if !catNoShow {
			value, err := types.DecodeValue(chunk, vrw)

			if err != nil {
				fmt.Println("        error reading value (Could be a format issue).")
				continue
			}

			fmt.Printf("        chunk[%d].value.kind:  %s\n", cidx, value.Kind())
			fmt.Printf("        chunk[%d].value:\n\n", cidx)
			printValue(ctx, os.Stdout, value, filepath.Dir(chunkFile)+"::#"+b32Hash)
			fmt.Println()
		}

		if !catNoRefs {
			refIdx := 0
			err = waf(chunk, func(addr hash.Hash, _ bool) error {
				if refIdx == 0 {
					fmt.Printf("    chunk[%d] references chunks:\n", cidx)
				}

				fmt.Printf("        Ref Hash: %s\n", addr.String())
				refIdx++

				return nil
			})
		}

		d.PanicIfError(err)
		fmt.Println()
	}

	if pos != 0 {
		panic("Didn't read the whole file")
	}

	return 0
}

func parseFooter(bytes []byte, pos int) (int, footer) {
	magicBytes := bytes[pos-magicSize : pos]
	pos -= magicSize

	totalSizeBytes := bytes[pos-totalUncmpSize : pos]
	pos -= totalUncmpSize

	chunkCntBytes := bytes[pos-chunkCntSize : pos]
	pos -= chunkCntSize

	return pos, footer{
		chunkCnt:   binary.BigEndian.Uint32(chunkCntBytes),
		uncompSize: binary.BigEndian.Uint64(totalSizeBytes),
		magicMatch: binary.BigEndian.Uint64(magicBytes) == magicNumber,
	}
}

func parsePrefixIndices(bytes []byte, pos, numChunks int) (int, []prefixIndex) {
	var hashPrefixes [][]byte
	var ordinals []uint32
	for i := 0; i < numChunks; i++ {
		ordinalBytes := bytes[pos-ordinalSize : pos]
		pos -= ordinalSize

		hashPrefixBytes := bytes[pos-prefixSize : pos]
		pos -= prefixSize

		hashPrefixes = append(hashPrefixes, hashPrefixBytes)
		ordinals = append(ordinals, binary.BigEndian.Uint32(ordinalBytes))
	}

	var indices []prefixIndex
	for i := numChunks - 1; i >= 0; i-- {
		indices = append(indices, prefixIndex{
			hashPrefix: hashPrefixes[i],
			chunkIndex: ordinals[i],
		})
	}

	return pos, indices
}

func parseChunkSuffixes(bytes []byte, pos, numChunks int) (int, [][]byte) {
	pos -= suffixSize * numChunks

	var suffixes [][]byte
	for i := 0; i < numChunks; i++ {
		start := pos + (i * suffixSize)
		suffixes = append(suffixes, bytes[start:start+suffixSize])
	}

	return pos, suffixes
}

func parseChunkSizes(bytes []byte, pos, numChunks int) (int, []int) {
	pos -= chunkSizeSize * numChunks

	var sizes []int
	for i := 0; i < numChunks; i++ {
		start := pos + (i * chunkSizeSize)
		sizeBytes := bytes[start : start+chunkSizeSize]

		sizes = append(sizes, int(binary.BigEndian.Uint32(sizeBytes)))
	}

	return pos, sizes
}

func parseChunks(bytes []byte, pos int, sizes []int) (int, []chunkData) {
	var crcs []uint32
	var offsets []uint64
	var chunkBytes [][]byte
	for i := 0; i < len(sizes); i++ {
		size := sizes[len(sizes)-i-1]
		crcBytes := bytes[pos-crcSize : pos]
		offset := uint64(pos - size)
		dataBytes := bytes[offset : pos-crcSize]
		pos -= size

		crcValInFile := binary.BigEndian.Uint32(crcBytes)
		crcOfData := crc(dataBytes)

		if crcValInFile != crcOfData {
			panic("CRC MISMATCH!!!")
		}

		chunkBytes = append(chunkBytes, dataBytes)
		crcs = append(crcs, crcValInFile)
		offsets = append(offsets, offset)
	}

	var cd []chunkData
	for i := len(sizes) - 1; i >= 0; i-- {
		uncompressed, err := snappy.Decode(nil, chunkBytes[i])
		d.PanicIfError(err)

		cd = append(cd, chunkData{
			compressed:    chunkBytes[i],
			uncompressed:  uncompressed,
			crc:           crcs[i],
			dataOffset:    offsets[i],
			decompSuccess: err == nil,
		})
	}

	return pos, cd
}

func printValue(ctx context.Context, w io.Writer, v types.Value, valSpec string) {
	defer func() {
		if r := recover(); r != nil {
			msg := "   Failed to write the value " + valSpec + "\n"
			io.WriteString(w, msg)
		}
	}()

	types.WriteEncodedValue(ctx, w, v)
}

func hexStr(bytes []byte) string {
	return hex.EncodeToString(bytes)
}

const bytesPerRow = 16

func hexView(bytes []byte, indent string) string {
	str := ""
	for i := 0; i < len(bytes); i += bytesPerRow {
		rowLen := min(16, len(bytes)-i)
		rowBytes := bytes[i : i+rowLen]
		str += indent + hexViewRow(i, rowBytes) + "\n"
	}

	return str
}

func hexViewRow(firstByteIndex int, rowBytes []byte) string {
	addr := fmt.Sprintf("%04x", firstByteIndex)

	hexWords := ""
	for i, b := range rowBytes {
		hexWords += fmt.Sprintf("%02x", b)

		if i%2 == 1 {
			hexWords += " "
		}

		if i%8 == 7 {
			hexWords += " "
		}
	}
	hexWidth := (bytesPerRow * 2) + (bytesPerRow)/2 + (bytesPerRow)/8

	var charRep []byte
	for _, b := range rowBytes {
		if b < 32 || b > 126 {
			charRep = append(charRep, byte('.'))
		} else {
			charRep = append(charRep, b)
		}
	}

	formatStr := `%s:  %-` + strconv.Itoa(hexWidth) + `s %s`
	return fmt.Sprintf(formatStr, addr, hexWords, charRep)
}

var b32encoder = base32.NewEncoding("0123456789abcdefghijklmnopqrstuv")

func b32Str(bytes []byte) string {
	return b32encoder.EncodeToString(bytes)
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func crc(b []byte) uint32 {
	return crc32.Update(0, crcTable, b)
}
