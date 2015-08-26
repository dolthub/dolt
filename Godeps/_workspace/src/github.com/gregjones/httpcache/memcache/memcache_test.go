// +build !appengine

package memcache

import (
	"bytes"
	"fmt"
	"net"
	"testing"

	. "gopkg.in/check.v1"
)

const testServer = "localhost:11211"

func Test(t *testing.T) { TestingT(t) }

type S struct{}

var _ = Suite(&S{})

func (s *S) SetUpSuite(c *C) {
	conn, err := net.Dial("tcp", testServer)
	if err != nil {
		// TODO: rather than skip the test, fall back to a faked memcached server
		c.Skip(fmt.Sprintf("skipping test; no server running at %s", testServer))
	}
	conn.Write([]byte("flush_all\r\n")) // flush memcache
	conn.Close()
}

func (s *S) Test(c *C) {
	cache := New(testServer)

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
