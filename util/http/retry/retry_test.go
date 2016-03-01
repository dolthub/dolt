package retry

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testReadCloser struct{}

func (trc testReadCloser) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (trc testReadCloser) Close() error {
	return nil
}

func TestSuccessOnFirstRequest(t *testing.T) {
	assert := assert.New(t)

	test := func(code int) {
		resp := Request("test.com", func() (*http.Response, error) {
			return &http.Response{StatusCode: code, Body: testReadCloser{}}, nil
		})
		assert.Equal(code, resp.StatusCode)
	}

	test(242)
	test(343)
}

func TestSuccessOnRetry(t *testing.T) {
	assert := assert.New(t)

	test := func(nRetries int) {
		sleepCount := 0
		lastDur := time.Duration(0)
		sleepFn = func(dur time.Duration) {
			sleepCount++
			assert.True(dur > lastDur)
			lastDur = dur
		}

		retryCount := 0
		resp := Request("test.com", func() (*http.Response, error) {
			assert.Equal(sleepCount, retryCount)
			if retryCount == nRetries {
				return &http.Response{StatusCode: 242, Body: testReadCloser{}}, nil
			}
			retryCount++
			return &http.Response{StatusCode: 500 + retryCount, Body: testReadCloser{}}, nil
		})
		assert.Equal(242, resp.StatusCode)
	}

	test(1)
	test(2)
	test(3)
}

func TestFailOnRetry(t *testing.T) {
	assert := assert.New(t)

	test := func(code int) {
		sleepFn = func(d time.Duration) {}
		assert.Panics(func() {
			Request("test.com", func() (*http.Response, error) {
				return &http.Response{StatusCode: code, Body: testReadCloser{}}, nil
			})
		}, "should panic")
	}

	test(444)
	test(555)
}
