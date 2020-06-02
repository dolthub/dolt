// Copyright 2020 Liquidata, Inc.
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

package async

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWaitGroupAddWait(_ *testing.T) {
	wg := &WaitGroup{}
	wg.Add(100)
	go func() {
		for i := 0; i < 100; i++ {
			wg.Done()
		}
	}()
	wg.Wait()
}

func TestWaitGroupAddWhileWait(t *testing.T) {
	defer func() {
		r := recover()
		assert.Nil(t, r)
	}()
	wg := &WaitGroup{}
	for i := 0; i < 5000000; i++ {
		wg.Add(1)
		go wg.Done()
	}
	go func() {
		for i := 0; i < 5000000; i++ {
			wg.Add(1)
			wg.Done()
		}
	}()
	wg.Wait()
}

func TestWaitGroupPanicOnNegative(t *testing.T) {
	defer func() {
		r := recover()
		assert.NotNil(t, r)
	}()
	wg := &WaitGroup{}
	wg.Add(1)
	wg.Done()
	wg.Done()
}
