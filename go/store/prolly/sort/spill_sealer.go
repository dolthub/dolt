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

package sort

import (
	"io"

	"github.com/dolthub/dolt/go/store/val"
)

// SpillSealer seals the tuples that a sorter spills to disk.
type SpillSealer interface {
	// NewFile returns the sealer of a new spill file, whose tuples are at most |maxTuple| bytes long.
	NewFile(maxTuple int) SpillFileSealer
}

// SpillFileSealer seals the tuples of one spill file, in order.
type SpillFileSealer interface {
	// Seal returns the record to write for |k|. The record may be reused by the next call.
	Seal(k val.Tuple) val.Tuple
	// Reader returns a reader of the records sealed so far.
	Reader() SpillFileReader
}

// SpillFileReader reads the sealed records of a spill file, in order.
type SpillFileReader interface {
	// Next returns the tuple of the next record that |r| reads, or io.EOF after the last one.
	Next(r io.Reader) (val.Tuple, error)
}

// SetSpillSealer makes the sorter seal the tuples it spills to disk with |s|; nil leaves them as they are.
func (a *tupleSorter) SetSpillSealer(s SpillSealer) {
	a.sealer, a.inProg.sealer = s, s
}

func sealedKeyFile(kf *keyFile, s SpillSealer) *keyFile {
	if s != nil {
		kf.seal = s.NewFile(kf.batchSize)
	}
	return kf
}
