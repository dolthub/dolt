/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gx/ipfs/QmTBxwy8cerzXbZQFUwTBCSxx55jUgVzudFcpmnAHUGuPy/badger/y"
	"gx/ipfs/QmVmDhyTTUcQXFD1rRQ64fGLMSAoaQvNH3hwuaCFAPq2hy/errors"
)

func TestValueBasic(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	y.Check(err)
	defer os.RemoveAll(dir)

	kv, _ := NewKV(getTestOptions(dir))
	defer kv.Close()
	log := &kv.vlog

	// Use value big enough that the value log writes them even if SyncWrites is false.
	const val1 = "sampleval012345678901234567890123"
	const val2 = "samplevalb012345678901234567890123"
	require.True(t, len(val1) >= kv.opt.ValueThreshold)

	entry := &Entry{
		Key:             []byte("samplekey"),
		Value:           []byte(val1),
		Meta:            BitValuePointer,
		CASCounterCheck: 22222,
		casCounter:      33333,
	}
	entry2 := &Entry{
		Key:             []byte("samplekeyb"),
		Value:           []byte(val2),
		Meta:            BitValuePointer,
		CASCounterCheck: 22225,
		casCounter:      33335,
	}

	b := new(request)
	b.Entries = []*Entry{entry, entry2}

	log.write([]*request{b})
	require.Len(t, b.Ptrs, 2)
	fmt.Printf("Pointer written: %+v %+v\n", b.Ptrs[0], b.Ptrs[1])

	var buf1, buf2 []byte
	var err1, err2 error
	err1 = log.readValueBytes(b.Ptrs[0], func(val []byte) error {
		buf1 = y.Safecopy(nil, val)
		return nil
	})
	err2 = log.readValueBytes(b.Ptrs[1], func(val []byte) error {
		buf2 = y.Safecopy(nil, val)
		return nil
	})

	require.NoError(t, err1)
	require.NoError(t, err2)
	readEntries := []Entry{valueBytesToEntry(buf1), valueBytesToEntry(buf2)}
	require.EqualValues(t, []Entry{
		{
			Key:             []byte("samplekey"),
			Value:           []byte(val1),
			Meta:            BitValuePointer,
			CASCounterCheck: 22222,
			casCounter:      33333,
		},
		{
			Key:             []byte("samplekeyb"),
			Value:           []byte(val2),
			Meta:            BitValuePointer,
			CASCounterCheck: 22225,
			casCounter:      33335,
		},
	}, readEntries)
}

func TestValueGC(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opt := getTestOptions(dir)
	opt.ValueLogFileSize = 1 << 20

	kv, _ := NewKV(opt)
	defer kv.Close()

	sz := 32 << 10
	var entries []*Entry
	for i := 0; i < 100; i++ {
		v := make([]byte, sz)
		rand.Read(v[:rand.Intn(sz)])
		entries = append(entries, &Entry{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: v,
		})
	}
	kv.BatchSet(entries)
	for _, e := range entries {
		require.NoError(t, e.Error, "entry with error: %+v", e)
	}

	for i := 0; i < 45; i++ {
		kv.Delete([]byte(fmt.Sprintf("key%d", i)))
	}

	kv.vlog.filesLock.RLock()
	lf := kv.vlog.filesMap[kv.vlog.sortedFids()[0]]
	kv.vlog.filesLock.RUnlock()

	//	lf.iterate(0, func(e Entry) bool {
	//		e.print("lf")
	//		return true
	//	})

	kv.vlog.rewrite(lf)
	var item KVItem
	for i := 45; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if err := kv.Get(key, &item); err != nil {
			t.Error(err)
		}
		val := getItemValue(t, &item)
		require.NotNil(t, val)
		require.True(t, len(val) == sz, "Size found: %d", len(val))
	}
}

func TestValueGC2(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opt := getTestOptions(dir)
	opt.ValueLogFileSize = 1 << 20

	kv, _ := NewKV(opt)
	defer kv.Close()

	sz := 32 << 10
	var entries []*Entry
	for i := 0; i < 100; i++ {
		v := make([]byte, sz)
		rand.Read(v[:rand.Intn(sz)])
		entry := &Entry{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: v,
		}
		entries = append(entries, entry)
		// We don't overwrite these values later in the test
		if i == 10 || i == 11 {
			entry.Meta = BitSetIfAbsent
		}
	}
	kv.BatchSet(entries)
	for _, e := range entries {
		require.NoError(t, e.Error, "entry with error: %+v", e)
	}

	for i := 0; i < 5; i++ {
		kv.Delete([]byte(fmt.Sprintf("key%d", i)))
	}

	entries = entries[:0]
	for i := 5; i < 10; i++ {
		v := []byte(fmt.Sprintf("value%d", i))
		entries = append(entries, &Entry{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: v,
		})
	}
	kv.BatchSet(entries)
	for _, e := range entries {
		require.NoError(t, e.Error, "entry with error: %+v", e)
	}

	kv.vlog.filesLock.RLock()
	lf := kv.vlog.filesMap[kv.vlog.sortedFids()[0]]
	kv.vlog.filesLock.RUnlock()

	//	lf.iterate(0, func(e Entry) bool {
	//		e.print("lf")
	//		return true
	//	})

	kv.vlog.rewrite(lf)
	var item KVItem
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if err := kv.Get(key, &item); err != nil {
			t.Error(err)
		}
		val := getItemValue(t, &item)
		require.True(t, len(val) == 0, "Size found: %d", len(val))
	}
	for i := 5; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if err := kv.Get(key, &item); err != nil {
			t.Error(err)
		}
		val := getItemValue(t, &item)
		require.NotNil(t, val)
		require.Equal(t, string(val), fmt.Sprintf("value%d", i))
	}
	for i := 10; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if err := kv.Get(key, &item); err != nil {
			t.Error(err)
		}
		val := getItemValue(t, &item)
		require.NotNil(t, val)
		require.True(t, len(val) == sz, "Size found: %d", len(val))
	}
}

func TestValueGC3(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opt := getTestOptions(dir)
	opt.ValueLogFileSize = 1 << 20

	kv, err := NewKV(opt)
	require.NoError(t, err)
	defer kv.Close()

	// We want to test whether an iterator can continue through a value log GC.

	valueSize := 32 << 10

	var value3 []byte
	var entries []*Entry
	for i := 0; i < 100; i++ {
		v := make([]byte, valueSize) // 32K * 100 will take >=3'276'800 B.
		if i == 3 {
			value3 = v
		}
		rand.Read(v[:])
		// Keys key000, key001, key002, such that sorted order matches insertion order
		entry := &Entry{
			Key:   []byte(fmt.Sprintf("key%03d", i)),
			Value: v,
		}
		entries = append(entries, entry)
	}
	err = kv.BatchSet(entries)
	require.NoError(t, err)
	for _, e := range entries {
		require.NoError(t, e.Error)
	}

	// Start an iterator to keys in the first value log file
	itOpt := IteratorOptions{
		PrefetchValues: false,
		PrefetchSize:   0,
		Reverse:        false,
	}

	it := kv.NewIterator(itOpt)
	defer it.Close()
	// Walk a few keys
	it.Rewind()
	require.True(t, it.Valid())
	item := it.Item()
	require.Equal(t, []byte("key000"), item.Key())
	it.Next()
	require.True(t, it.Valid())
	item = it.Item()
	require.Equal(t, []byte("key001"), item.Key())
	it.Next()
	require.True(t, it.Valid())
	item = it.Item()
	require.Equal(t, []byte("key002"), item.Key())

	// Like other tests, we pull out a logFile to rewrite it directly

	kv.vlog.filesLock.RLock()
	logFile := kv.vlog.filesMap[kv.vlog.sortedFids()[0]]
	kv.vlog.filesLock.RUnlock()

	kv.vlog.rewrite(logFile)
	it.Next()
	require.True(t, it.Valid())
	item = it.Item()
	require.Equal(t, []byte("key003"), item.Key())
	var v3 []byte
	var wg sync.WaitGroup
	wg.Add(1)
	item.Value(func(x []byte) error { v3 = x; wg.Done(); return nil })
	wg.Wait()
	require.Equal(t, value3, v3)
}

func TestChecksums(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Set up SST with K1=V1
	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 100 * 1024 * 1024 // 100Mb
	kv, err := NewKV(opts)
	require.NoError(t, err)

	var (
		k1 = []byte("k1")
		k2 = []byte("k2")
		k3 = []byte("k3")
		v1 = []byte("value1-012345678901234567890123")
		v2 = []byte("value2-012345678901234567890123")
		v3 = []byte("value3-012345678901234567890123")
	)
	// Make sure the value log would actually store the item
	require.True(t, len(v3) >= kv.opt.ValueThreshold)

	require.NoError(t, kv.Set(k1, v1, 0))
	require.NoError(t, kv.Close())

	// Use a vlog with K1=V1 and a (corrupted) K2=V2
	buf := createVlog(t, []*Entry{
		{Key: k1, Value: v1},
		{Key: k2, Value: v2},
	})
	buf[len(buf)-1]++ // Corrupt last byte
	require.NoError(t, ioutil.WriteFile(vlogFilePath(dir, 0), buf, 0777))

	// K1 should exist, but K2 shouldn't.
	kv, err = NewKV(opts)
	require.NoError(t, err)
	var item KVItem
	require.NoError(t, kv.Get(k1, &item))
	require.Equal(t, getItemValue(t, &item), v1)
	ok, err := kv.Exists(k2)
	require.NoError(t, err)
	require.False(t, ok)
	// Write K3 at the end of the vlog.
	require.NoError(t, kv.Set(k3, v3, 0))
	require.NoError(t, kv.Close())

	// The vlog should contain K1 and K3 (K2 was lost when Badger started up
	// last due to checksum failure).
	kv, err = NewKV(opts)
	require.NoError(t, err)
	iter := kv.NewIterator(DefaultIteratorOptions)
	iter.Seek(k1)
	require.True(t, iter.Valid())
	it := iter.Item()
	require.Equal(t, it.Key(), k1)
	require.Equal(t, getItemValue(t, it), v1)
	iter.Next()
	require.True(t, iter.Valid())
	it = iter.Item()
	require.Equal(t, it.Key(), k3)
	require.Equal(t, getItemValue(t, it), v3)
	iter.Close()
	require.NoError(t, kv.Close())
}

func TestPartialAppendToValueLog(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Create skeleton files.
	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 100 * 1024 * 1024 // 100Mb
	kv, err := NewKV(opts)
	require.NoError(t, err)
	require.NoError(t, kv.Close())

	var (
		k1 = []byte("k1")
		k2 = []byte("k2")
		k3 = []byte("k3")
		v1 = []byte("value1-012345678901234567890123")
		v2 = []byte("value2-012345678901234567890123")
		v3 = []byte("value3-012345678901234567890123")
	)
	// Values need to be long enough to actually get written to value log.
	require.True(t, len(v3) >= kv.opt.ValueThreshold)

	// Create truncated vlog to simulate a partial append.
	buf := createVlog(t, []*Entry{
		{Key: k1, Value: v1},
		{Key: k2, Value: v2},
	})
	buf = buf[:len(buf)-6]
	require.NoError(t, ioutil.WriteFile(vlogFilePath(dir, 0), buf, 0777))

	// Badger should now start up, but with only K1.
	kv, err = NewKV(opts)
	require.NoError(t, err)
	var item KVItem
	require.NoError(t, kv.Get(k1, &item))
	ok, err := kv.Exists(k2)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, item.Key(), k1)
	require.Equal(t, getItemValue(t, &item), v1)

	// When K3 is set, it should be persisted after a restart.
	require.NoError(t, kv.Set(k3, v3, 0))
	require.NoError(t, kv.Close())
	kv, err = NewKV(getTestOptions(dir))
	require.NoError(t, err)
	checkKeys(t, kv, [][]byte{k1, k3})
	require.NoError(t, kv.Close())
}

func TestValueLogTrigger(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := getTestOptions(dir)
	opt.ValueLogFileSize = 1 << 20
	kv, err := NewKV(opt)
	require.NoError(t, err)

	// Write a lot of data, so it creates some work for valug log GC.
	sz := 32 << 10
	var entries []*Entry
	for i := 0; i < 100; i++ {
		v := make([]byte, sz)
		rand.Read(v[:rand.Intn(sz)])
		entries = append(entries, &Entry{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: v,
		})
	}
	kv.BatchSet(entries)
	for _, e := range entries {
		require.NoError(t, e.Error, "entry with error: %+v", e)
	}

	for i := 0; i < 45; i++ {
		kv.Delete([]byte(fmt.Sprintf("key%d", i)))
	}

	// Now attempt to run 5 value log GCs simultaneously.
	errCh := make(chan error, 5)
	for i := 0; i < 5; i++ {
		go func() { errCh <- kv.RunValueLogGC(0.5) }()
	}
	var numRejected int
	for i := 0; i < 5; i++ {
		err := <-errCh
		if err == ErrRejected {
			numRejected++
		}
	}
	require.True(t, numRejected > 0, "Should have found at least one value log GC request rejected.")
	require.NoError(t, kv.Close())

	err = kv.RunValueLogGC(0.5)
	require.Equal(t, ErrRejected, err, "Error should be returned after closing KV.")
}

func createVlog(t *testing.T, entries []*Entry) []byte {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 100 * 1024 * 1024 // 100Mb
	kv, err := NewKV(opts)
	require.NoError(t, err)
	require.NoError(t, kv.BatchSet(entries))
	require.NoError(t, kv.Close())

	filename := vlogFilePath(dir, 0)
	buf, err := ioutil.ReadFile(filename)
	require.NoError(t, err)
	return buf
}

func checkKeys(t *testing.T, kv *KV, keys [][]byte) {
	i := 0
	iter := kv.NewIterator(IteratorOptions{})
	for iter.Seek(keys[0]); iter.Valid(); iter.Next() {
		require.Equal(t, iter.Item().Key(), keys[i])
		i++
	}
	require.Equal(t, i, len(keys))
}

func BenchmarkReadWrite(b *testing.B) {
	rwRatio := []float32{
		0.1, 0.2, 0.5, 1.0,
	}
	valueSize := []int{
		64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384,
	}

	for _, vsz := range valueSize {
		for _, rw := range rwRatio {
			b.Run(fmt.Sprintf("%3.1f,%04d", rw, vsz), func(b *testing.B) {
				var vl valueLog
				dir, err := ioutil.TempDir("", "vlog")
				y.Check(err)
				defer os.RemoveAll(dir)
				err = vl.Open(nil, getTestOptions(dir))
				y.Check(err)
				defer vl.Close()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					e := new(Entry)
					e.Key = make([]byte, 16)
					e.Value = make([]byte, vsz)
					bl := new(request)
					bl.Entries = []*Entry{e}

					var ptrs []valuePointer

					vl.write([]*request{bl})
					ptrs = append(ptrs, bl.Ptrs...)

					f := rand.Float32()
					if f < rw {
						vl.write([]*request{bl})
						ptrs = append(ptrs, bl.Ptrs...)

					} else {
						ln := len(ptrs)
						if ln == 0 {
							b.Fatalf("Zero length of ptrs")
						}
						idx := rand.Intn(ln)
						err := vl.readValueBytes(ptrs[idx], func(buf []byte) error {
							e := valueBytesToEntry(buf)
							if len(e.Key) != 16 {
								return errors.New("Key is invalid")
							}
							if len(e.Value) != vsz {
								return errors.New("Value is invalid")
							}
							return nil
						})
						if err != nil {
							b.Fatalf("Benchmark Read: %v", err)
						}
					}
				}
			})
		}
	}
}
