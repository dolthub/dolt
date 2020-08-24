package async

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"math/rand"
	"testing"
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
