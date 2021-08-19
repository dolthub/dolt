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
