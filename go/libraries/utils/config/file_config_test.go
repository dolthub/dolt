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

package config

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

func newTestFileConfig(t *testing.T) *FileConfig {
	t.Helper()
	fs := filesys.NewInMemFS([]string{}, map[string][]byte{}, "/")
	cfg, err := NewFileConfig(cfgPath, fs, map[string]string{})
	require.NoError(t, err)
	return cfg
}

const (
	cfgPath = "/home/bheni/.ld/config.json"
)

func TestGetAndSet(t *testing.T) {
	fs := filesys.NewInMemFS([]string{}, map[string][]byte{}, "/")
	cfg, err := NewFileConfig(cfgPath, fs, map[string]string{})

	if err != nil {
		t.Fatal("Failed to create empty config")
	}

	params := map[string]string{
		"string": "this is a string",
		"int":    "-15",
		"uint":   "1234567",
		"float":  "3.1415",
	}

	err = cfg.SetStrings(params)

	if err != nil {
		t.Fatal("Failed to set values")
	}

	if exists, isDir := fs.Exists(cfgPath); !exists || isDir {
		t.Fatal("File not written after SetStrings was called")
	}

	cfg, err = FromFile(cfgPath, fs)

	if err != nil {
		t.Fatal("Error reading config")
	}

	if str, err := cfg.GetString("string"); err != nil || str != "this is a string" {
		t.Error("Failed to read back string after setting it")
	}

	testIteration(t, params, cfg)

	err = cfg.Unset([]string{"int", "float"})

	if err != nil {
		t.Error("Failed to unset properties")
	}

	testIteration(t, map[string]string{"string": "this is a string", "uint": "1234567"}, cfg)
}

func TestConcurrentSetStrings(t *testing.T) {
	cfg := newTestFileConfig(t)
	const goroutines = 50
	const keysPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < keysPerGoroutine; j++ {
				k := fmt.Sprintf("g%d_k%d", id, j)
				err := cfg.SetStrings(map[string]string{k: fmt.Sprintf("v%d", j)})
				assert.NoError(t, err)
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, goroutines*keysPerGoroutine, cfg.Size())
}

func TestConcurrentGetString(t *testing.T) {
	cfg := newTestFileConfig(t)
	require.NoError(t, cfg.SetStrings(map[string]string{"key": "value"}))

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			v, err := cfg.GetString("key")
			assert.NoError(t, err)
			assert.Equal(t, "value", v)
		}()
	}
	wg.Wait()
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	cfg := newTestFileConfig(t)
	require.NoError(t, cfg.SetStrings(map[string]string{"shared": "init"}))

	const goroutines = 50
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 4) // 4 operation types

	// Concurrent SetStrings
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				err := cfg.SetStrings(map[string]string{
					"shared":               fmt.Sprintf("writer%d_iter%d", id, j),
					fmt.Sprintf("w%d", id): fmt.Sprintf("%d", j),
				})
				assert.NoError(t, err)
			}
		}(i)
	}

	// Concurrent GetString
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = cfg.GetString("shared")
			}
		}()
	}

	// Concurrent GetStringOrDefault
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				v := cfg.GetStringOrDefault("shared", "default")
				assert.NotEmpty(t, v)
			}
		}()
	}

	// Concurrent Iter
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				cfg.Iter(func(k, v string) bool {
					return false
				})
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentSetAndUnset(t *testing.T) {
	cfg := newTestFileConfig(t)

	// Pre-populate keys that will be unset.
	keys := make([]string, 100)
	init := make(map[string]string)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
		init[keys[i]] = "val"
	}
	require.NoError(t, cfg.SetStrings(init))

	var wg sync.WaitGroup

	// Half the goroutines set new keys, the other half unset existing ones.
	const setters = 25
	const unsetters = 25
	wg.Add(setters + unsetters)

	for i := 0; i < setters; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				k := fmt.Sprintf("new_%d_%d", id, j)
				err := cfg.SetStrings(map[string]string{k: "v"})
				assert.NoError(t, err)
			}
		}(i)
	}

	for i := 0; i < unsetters; i++ {
		go func(id int) {
			defer wg.Done()
			// Each unsetter removes distinct keys so no double-delete conflicts.
			for j := 0; j < 4; j++ {
				idx := id*4 + j
				err := cfg.Unset([]string{keys[idx]})
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// 100 original - 100 unset + 500 new = 500
	assert.Equal(t, 500, cfg.Size())
}

func TestConcurrentSize(t *testing.T) {
	cfg := newTestFileConfig(t)

	var wg sync.WaitGroup
	const goroutines = 50
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			err := cfg.SetStrings(map[string]string{fmt.Sprintf("k%d", id): "v"})
			assert.NoError(t, err)
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			s := cfg.Size()
			assert.GreaterOrEqual(t, s, 0)
			assert.LessOrEqual(t, s, goroutines)
		}()
	}

	wg.Wait()
	assert.Equal(t, goroutines, cfg.Size())
}
