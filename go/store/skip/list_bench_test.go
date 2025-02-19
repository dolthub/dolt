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

package skip

import (
	"bytes"
	"context"
	"sort"
	"testing"
)

func BenchmarkGet(b *testing.B) {
	b.Run("unsorted keys", func(b *testing.B) {
		b.Run("n=64", func(b *testing.B) {
			benchmarkGet(b, randomInts(64))
		})
		b.Run("n=2048", func(b *testing.B) {
			benchmarkGet(b, randomInts(2048))
		})
		b.Run("n=65536", func(b *testing.B) {
			benchmarkGet(b, randomInts(65536))
		})
	})
	b.Run("ascending keys", func(b *testing.B) {
		b.Run("n=64", func(b *testing.B) {
			benchmarkGet(b, ascendingInts(64))
		})
		b.Run("n=2048", func(b *testing.B) {
			benchmarkGet(b, ascendingInts(2048))
		})
		b.Run("n=65536", func(b *testing.B) {
			benchmarkGet(b, ascendingInts(65536))
		})
	})
	b.Run("descending keys", func(b *testing.B) {
		b.Run("n=64", func(b *testing.B) {
			benchmarkGet(b, descendingInts(64))
		})
		b.Run("n=2048", func(b *testing.B) {
			benchmarkGet(b, descendingInts(2048))
		})
		b.Run("n=65536", func(b *testing.B) {
			benchmarkGet(b, descendingInts(65536))
		})
	})
}

func BenchmarkPut(b *testing.B) {
	b.Run("unsorted keys", func(b *testing.B) {
		b.Run("n=64", func(b *testing.B) {
			benchmarkPut(b, randomInts(64))
		})
		b.Run("n=2048", func(b *testing.B) {
			benchmarkPut(b, randomInts(2048))
		})
		b.Run("n=65536", func(b *testing.B) {
			benchmarkPut(b, randomInts(65536))
		})
	})
	b.Run("ascending keys", func(b *testing.B) {
		b.Run("n=64", func(b *testing.B) {
			benchmarkPut(b, ascendingInts(64))
		})
		b.Run("n=2048", func(b *testing.B) {
			benchmarkPut(b, ascendingInts(2048))
		})
		b.Run("n=65536", func(b *testing.B) {
			benchmarkPut(b, ascendingInts(65536))
		})
	})
	b.Run("descending keys", func(b *testing.B) {
		b.Run("n=64", func(b *testing.B) {
			benchmarkPut(b, descendingInts(64))
		})
		b.Run("n=2048", func(b *testing.B) {
			benchmarkPut(b, descendingInts(2048))
		})
		b.Run("n=65536", func(b *testing.B) {
			benchmarkPut(b, descendingInts(65536))
		})
	})
}

func BenchmarkIterAll(b *testing.B) {
	b.Run("unsorted keys", func(b *testing.B) {
		b.Run("n=64", func(b *testing.B) {
			benchmarkIterAll(b, randomInts(64))
		})
		b.Run("n=2048", func(b *testing.B) {
			benchmarkIterAll(b, randomInts(2048))
		})
		b.Run("n=65536", func(b *testing.B) {
			benchmarkIterAll(b, randomInts(65536))
		})
	})
	b.Run("ascending keys", func(b *testing.B) {
		b.Run("n=64", func(b *testing.B) {
			benchmarkIterAll(b, ascendingInts(64))
		})
		b.Run("n=2048", func(b *testing.B) {
			benchmarkIterAll(b, ascendingInts(2048))
		})
		b.Run("n=65536", func(b *testing.B) {
			benchmarkIterAll(b, ascendingInts(65536))
		})
	})
	b.Run("descending keys", func(b *testing.B) {
		b.Run("n=64", func(b *testing.B) {
			benchmarkIterAll(b, descendingInts(64))
		})
		b.Run("n=2048", func(b *testing.B) {
			benchmarkIterAll(b, descendingInts(2048))
		})
		b.Run("n=65536", func(b *testing.B) {
			benchmarkIterAll(b, descendingInts(65536))
		})
	})
}

func benchmarkGet(b *testing.B, vals [][]byte) {
	ctx := context.Background()
	l := NewSkipList(compareBytes)
	for i := range vals {
		l.Put(ctx, vals[i], vals[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok := l.Get(ctx, vals[i%len(vals)])
		if !ok {
			b.Fail()
		}
	}
	b.ReportAllocs()
}

func benchmarkPut(b *testing.B, vals [][]byte) {
	ctx := context.Background()
	l := NewSkipList(compareBytes)
	for i := 0; i < b.N; i++ {
		j := i % len(vals)
		if j == 0 {
			l.Truncate()
		}
		l.Put(ctx, vals[j], vals[j])
	}
	b.ReportAllocs()
}

func benchmarkIterAll(b *testing.B, vals [][]byte) {
	ctx := context.Background()
	l := NewSkipList(compareBytes)
	for i := range vals {
		l.Put(ctx, vals[i], vals[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := l.IterAtStart()
		k, _ := iter.Current()
		for k != nil {
			iter.Advance()
			k, _ = iter.Current()
		}
	}
	b.ReportAllocs()
}

func ascendingInts(sz int64) (vals [][]byte) {
	vals = randomInts(sz)
	sort.Slice(vals, func(i, j int) bool {
		return bytes.Compare(vals[i], vals[j]) < 1
	})
	return
}

func descendingInts(sz int64) (vals [][]byte) {
	vals = randomInts(sz)
	sort.Slice(vals, func(i, j int) bool {
		return bytes.Compare(vals[i], vals[j]) >= 1
	})
	return
}
