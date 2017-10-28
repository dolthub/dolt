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

package badger_test

import (
	"fmt"
	"io/ioutil"
	"sync"

	"gx/ipfs/QmTBxwy8cerzXbZQFUwTBCSxx55jUgVzudFcpmnAHUGuPy/badger"
)

var d string = "doc"

func Example() {
	opt := badger.DefaultOptions
	dir, _ := ioutil.TempDir("", "badger")
	opt.Dir = dir
	opt.ValueDir = dir
	kv, _ := badger.NewKV(&opt)

	key := []byte("hello")

	kv.Set(key, []byte("world"), 0x00)
	fmt.Printf("SET %s world\n", key)

	var item badger.KVItem
	if err := kv.Get(key, &item); err != nil {
		fmt.Printf("Error while getting key: %q", key)
		return
	}
	var val []byte
	err := item.Value(func(v []byte) error {
		val = make([]byte, len(v))
		copy(val, v)
		return nil
	})
	if err != nil {
		fmt.Printf("Error while getting value for key: %q", key)
		return
	}

	fmt.Printf("GET %s %s\n", key, val)

	if err := kv.CompareAndSet(key, []byte("venus"), 100); err != nil {
		fmt.Println("CAS counter mismatch")
	} else {
		if err = kv.Get(key, &item); err != nil {
			fmt.Printf("Error while getting key: %q", key)
		}

		err := item.Value(func(v []byte) error {
			val = make([]byte, len(v))
			copy(val, v)
			return nil
		})

		if err != nil {
			fmt.Printf("Error while getting value for key: %q", key)
			return
		}

		fmt.Printf("Set to %s\n", val)
	}
	if err := kv.CompareAndSet(key, []byte("mars"), item.Counter()); err == nil {
		fmt.Println("Set to mars")
	} else {
		fmt.Printf("Unsuccessful write. Got error: %v\n", err)
	}

	// Output:
	// SET hello world
	// GET hello world
	// CAS counter mismatch
	// Set to mars
}

// func ExampleNewIterator() {
// 	opt := DefaultOptions
// 	opt.Dir = "/tmp/badger"
// 	kv := NewKV(&opt)

// 	itrOpt := IteratorOptions{
// 		PrefetchSize: 1000,
// 		PrefetchValues:  true,
// 		Reverse:      false,
// 	}
// 	itr := kv.NewIterator(itrOpt)
// 	for itr.Rewind(); itr.Valid(); itr.Next() {
// 		item := itr.Item()
// 		item.Key()
// 		var val []byte
// 		err = item.Value(func(v []byte) {
// 			val = make([]byte, len(v))
// 			copy(val, v)
// 		})
// 	}
// }

func ExampleKV_BatchSetAsync() {
	opt := badger.DefaultOptions
	dir, _ := ioutil.TempDir("", "badger")
	opt.Dir = dir
	opt.SyncWrites = true
	opt.ValueDir = dir
	kv, _ := badger.NewKV(&opt)
	wg := new(sync.WaitGroup)
	wb := make([]*badger.Entry, 0, 100)

	wg.Add(1)
	// Async writes would be useful if you want to write some key-value pairs without waiting
	// for them to be complete and perform some cleanup when they are written.

	// In Dgraph we keep on flushing posting lists periodically to badger. We do it an async
	// manner and provide a callback to it which can do the cleanup when the writes are done.
	f := func(err error) {
		defer wg.Done()
		if err != nil {
			// At this point you can retry writing keys or send error over a channel to handle
			// in some other goroutine.
			fmt.Printf("Got error: %+v\n", err)
		}

		// Check for error in entries which could be non-nil if the user supplies a CasCounter.
		for _, e := range wb {
			if e.Error != nil {
				fmt.Printf("Got error: %+v\n", e.Error)
			}
		}

		// You can do cleanup now. Like deleting keys from cache.
		fmt.Println("All async sets complete.")
	}

	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("%09d", i))
		wb = append(wb, &badger.Entry{
			Key:   k,
			Value: k,
		})
	}
	kv.BatchSetAsync(wb, f)
	fmt.Println("Finished writing keys to badger.")
	wg.Wait()

	// Output: Finished writing keys to badger.
	// All async sets complete.
}
