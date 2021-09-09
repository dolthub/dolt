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

package buffer

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDynamicBuffer(t *testing.T) {
	const blockSize = 53

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 100; i++ {
		n := 1000 + rand.Int63()%10000
		t.Run(strconv.FormatInt(n, 10), func(t *testing.T) {
			data := make([]byte, n)
			read, err := rand.Read(data)
			require.NoError(t, err)
			require.Equal(t, int(n), read)

			buf := New(blockSize)
			buf.Append(data)
			itr := buf.Close()

			reassembled := bytes.NewBuffer(nil)
			err = itr.FlushTo(reassembled)
			require.NoError(t, err)
			require.Equal(t, data, reassembled.Bytes())
		})
	}
}
