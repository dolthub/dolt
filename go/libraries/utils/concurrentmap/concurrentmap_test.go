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

package concurrentmap

import (
	"sync"
	"testing"
)

func TestConcurrentMapConstructor(t *testing.T) {
	m := New[int, string]()
	if m == nil {
		t.Fatal("New concurrent map is nil")
	}
	if m.m == nil {
		t.Error("New concurrent map's underlying map is nil")
	}
	if len(m.m) != 0 {
		t.Error("New concurrent map's underlying map is not empty")
	}
}

func TestConcurrentMapSetAndGet(t *testing.T) {
	m := New[int, string]()
	m.Set(1, "a")

	// Test that the value is set
	if val, found := m.Get(1); !found || val != "a" {
		t.Errorf("Got %s, want %s", val, "a")
	}
	// Test that the value is not set for a different key
	if val, found := m.Get(2); found || val != "" {
		t.Errorf("Got %s, want an empty value and a not found", val)
	}
}

func TestConcurrentMapDelete(t *testing.T) {
	m := New[int, string]()
	m.Set(1, "a")
	m.Delete(1)

	// Test that the value is deleted
	if _, found := m.Get(1); found {
		t.Errorf("Expected key 1 to be deleted")
	}
}

func TestConcurrentMapLen(t *testing.T) {
	m := New[int, string]()
	m.Set(1, "a")
	m.Set(2, "b")
	m.Set(3, "b")

	// Test that the length is correct
	if m.Len() != 3 {
		t.Errorf("Expected length 3, got %d", m.Len())
	}
}

func TestConcurrentMapDeepCopy(t *testing.T) {
	m := New[int, string]()
	m.Set(1, "a")
	copy := m.DeepCopy()
	m.Set(1, "b")

	// Test that the copy is not affected by the original
	if val, _ := copy.Get(1); val != "a" {
		t.Errorf("DeepCopy failed, expected 'a', got '%s'", val)
	}
}

func TestConcurrentMapIter(t *testing.T) {
	m := New[int, string]()
	m.Set(1, "a")
	m.Set(2, "b")
	m.Set(3, "c")

	counter := 0
	elements := make(map[int]string)
	m.Iter(func(key int, value string) bool {
		counter++
		elements[key] = value
		return true
	})

	// Test that the iterator iterates over all elements
	if counter != 3 {
		t.Errorf("Iter failed, expected to iterate 3 times, iterated %d times", counter)
	}

	// Test that iteration yields all elements
	if len(elements) != 3 {
		t.Errorf("Iter failed, there should be 3 elements in the map, got %d", len(elements))
	}
	if elements[1] != "a" || elements[2] != "b" || elements[3] != "c" {
		t.Errorf("Iter failed, expected to have 3 elements in the map, with correct values: %v", elements)
	}
}

func TestConcurrentMapSetAndGetWithConcurrency(t *testing.T) {
	m := New[int, int]()
	var wg sync.WaitGroup

	// Set 100 elements concurrently
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Set(i, i)
		}(i)
	}

	// Wait for al goroutines to finish
	wg.Wait()

	// Test that all elements are set
	for i := 0; i < 100; i++ {
		if val, found := m.Get(i); !found || val != i {
			t.Errorf("Got %d, want %d", val, i)
		}
	}
}
