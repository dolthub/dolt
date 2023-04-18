// Copyright 2022 Dolthub, Inc.
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

package tree

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/dolthub/dolt/go/store/hash"
)

var result hash.Hash

func BenchmarkBlobBuilder(b *testing.B) {
	var r hash.Hash
	var err error
	dataSizes := []int{1e3, 1e4, 1e5, 1e6}
	for _, d := range dataSizes {
		b.Run(fmt.Sprintf("datasize: %.0f", math.Log10(float64(d))), func(b *testing.B) {
			ns := NewTestNodeStore()
			bb := mustNewBlobBuilder(DefaultFixedChunkLength)
			bb.SetNodeStore(ns)
			buf := make([]byte, d)
			for i := 0; i < d; i++ {
				buf[i] = uint8(i)
			}

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				// always record the result to prevent
				// the compiler eliminating the function call.
				bb.Init(d)
				_, r, err = bb.Chunk(context.Background(), bytes.NewReader(buf))
				if err != nil {
					b.Fatal(err)
				}
				bb.Reset()
			}
			// always store the result to a package level variable
			// so the compiler cannot eliminate the Benchmark itself.
			result = r
		})
	}
}
