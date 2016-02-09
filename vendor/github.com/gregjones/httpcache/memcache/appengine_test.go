// +build appengine

package memcache

import (
	"bytes"
	"testing"

	"appengine/aetest"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type S struct{}

var _ = Suite(&S{})

func (s *S) Test(c *C) {
	ctx, err := aetest.NewContext(nil)
	if err != nil {
		c.Fatal(err)
	}
	defer ctx.Close()

	cache := New(ctx)

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
