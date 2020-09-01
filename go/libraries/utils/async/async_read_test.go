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
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	testReadNItems(t, 0, 32)
	testReadNItems(t, 1, 32)
	testReadNItems(t, 65536, 32)

	const maxItems = 4 * 1024
	const minItems = 4
	const maxTestBufferSize = 128

	for i := 0; i < 32; i++ {
		testReadNItems(t, rand.Int63n(maxItems-minItems)+minItems, rand.Int63n(maxTestBufferSize-1)+1)
	}
}

func testReadNItems(t *testing.T, n int64, bufferSize int64) {
	t.Run(fmt.Sprintf("%d_%d", n, bufferSize), func(t *testing.T) {
		arr := make([]int64, n)

		for i := int64(0); i < n; i++ {
			arr[i] = i
		}

		readFunc := readFuncForArr(arr)
		rd := NewAsyncReader(readFunc, 32)
		err := rd.Start(context.Background())
		require.NoError(t, err)

		res := make([]int64, 0)
		for {
			val, err := rd.Read()

			if err == io.EOF {
				break
			}

			assert.NoError(t, err)
			res = append(res, val.(int64))
		}

		err = rd.Close()
		require.NoError(t, err)

		assert.Equal(t, arr, res)
	})
}

func readFuncForArr(arr []int64) ReadFunc {
	pos := 0
	return func(ctx context.Context) (interface{}, error) {
		if pos >= len(arr) {
			return nil, io.EOF
		}

		val := arr[pos]
		pos++

		return val, nil
	}
}
