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
			bb := mustNewBlobBuilder(ns, DefaultFixedChunkLength)
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
