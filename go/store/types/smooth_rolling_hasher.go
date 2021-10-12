// Copyright 2021 Dolthub, Inc.
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

package types

import (
	"os"
	"strings"

	"github.com/kch42/buzhash"

	"github.com/dolthub/dolt/go/store/sloppy"
)

const (
	smoothMinChunkSize = 1 << 9
	smoothMaxChunkSize = 1 << 14

	smoothChunkingFeatureFlag = "DOLT_ENABLE_SMOOTH_CHUNKING"
)

func init() {
	val, ok := os.LookupEnv(smoothChunkingFeatureFlag)
	if ok && strings.ToLower(val) == "true" {
		SmoothChunking = true
	}
}

var SmoothChunking = false

// smoothRollingHasher is a sequenceSplitter designed to constrain the output
// chunk size distribution. smoothRollingHasher matches against different patterns
// depending on the size of the current chunk being hashed. The larger the current
// chunk, the easier the pattern gets. The result is a chunk size distribution
// that is closer to a binomial distribution, rather than geometric.
type smoothRollingHasher struct {
	bw binaryNomsWriter
	sl *sloppy.Sloppy

	bz     *buzhash.BuzHash
	window uint32
	salt   byte

	patterns        []patternRange
	crossedBoundary bool

	nbf *NomsBinFormat
}

var _ sequenceSplitter = &smoothRollingHasher{}

type patternRange [2]uint32 // { rangeEnd, pattern }

const (
	rangeEnd = 0
	pattern  = 1
)

var smoothPatterns = []patternRange{
	// min = 512
	{1024, 1<<16 - 1},
	{2048, 1<<14 - 1},
	{4096, 1<<12 - 1},
	{8192, 1<<10 - 1},
	{16384, 1<<8 - 1},
	// max = 16384
}

func newSmoothRollingHasher(nbf *NomsBinFormat, salt byte) *smoothRollingHasher {

	rv := &smoothRollingHasher{
		bw:       newBinaryNomsWriter(),
		bz:       buzhash.NewBuzHash(chunkWindow),
		window:   chunkWindow,
		salt:     salt,
		patterns: smoothPatterns,
		nbf:      nbf,
	}

	rv.sl = sloppy.New(rv.HashByte)

	return rv
}

func (sh *smoothRollingHasher) Append(cb func(w *binaryNomsWriter) error) (err error) {
	err = cb(&sh.bw)
	if err == nil {
		sh.sl.Update(sh.bw.data())
	}
	return
}

func (sh *smoothRollingHasher) HashByte(b byte) bool {
	return sh.hashByte(b, sh.bw.offset)
}

func (sh *smoothRollingHasher) hashByte(b byte, offset uint32) bool {
	if !sh.crossedBoundary {
		sh.bz.HashByte(b ^ sh.salt)

		if sh.bw.offset < smoothMinChunkSize {
			return sh.crossedBoundary
		}
		if offset > smoothMaxChunkSize {
			sh.crossedBoundary = true
			return sh.crossedBoundary
		}

		hash := sh.bz.Sum32()
		for _, rp := range sh.patterns {
			if offset < rp[rangeEnd] {
				pat := rp[pattern]
				sh.crossedBoundary = hash&pat == pat
				return sh.crossedBoundary
			}
		}
	}

	return sh.crossedBoundary
}

func (sh *smoothRollingHasher) Nbf() *NomsBinFormat {
	return sh.nbf
}

func (sh *smoothRollingHasher) CrossedBoundary() bool {
	return sh.crossedBoundary
}

func (sh *smoothRollingHasher) Reset() {
	sh.crossedBoundary = false
	sh.bz = buzhash.NewBuzHash(sh.window)
	sh.bw.reset()
	sh.sl.Reset()
}
