package diskcache

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type S struct{}

var _ = Suite(&S{})

func (s *S) Test(c *C) {
	tempDir, err := ioutil.TempDir("", "httpcache")
	if err != nil {
		c.Fatalf("TempDir,: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cache := New(tempDir)

	key := "testKey"
	_, ok := cache.Get(key)

	c.Assert(ok, Equals, false)

	val := []byte("some bytes")
	cache.Set(key, val)

	retVal, ok := cache.Get(key)
	c.Assert(ok, Equals, true)
	c.Assert(bytes.Equal(retVal, val), Equals, true)

	cache.Delete(key)

	_, ok = cache.Get(key)
	c.Assert(ok, Equals, false)
}
