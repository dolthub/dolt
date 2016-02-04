package base

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/attic-labs/noms/d"
)

// ReadSeekCloser unifies io.Reader, io.Seeker and io.Closer
type ReadSeekCloser interface {
	io.ReadSeeker
	io.Closer
}

// MakeSeekable adds the io.Seeker interface to r. The caller is responsible for calling Close() on the returned object when done reading data.
func MakeSeekable(r io.Reader, length int64) ReadSeekCloser {
	// It might be a nice optimization to buffer small objects in memory, but bytes.Buffer doesn't implement io.Seeker, and bytes.Reader doesn't implement io.Writer.
	cache, err := ioutil.TempFile("", "seekable-reader-")
	d.Chk.NoError(err)
	return &seekableReader{r: r, cache: cache, length: length}
}

type seekableReader struct {
	r     io.Reader
	cache *os.File

	length, cached, pos int64
}

func (s *seekableReader) Read(b []byte) (n int, err error) {
	if s.pos < s.cached {
		// Caller sought backwards, so current position is somewhere in the cached data. Satisfy the Read() from the cache as much as possible. If that doesn't fill b, the caller will see that n < len(b) and try again.
		n, err = io.ReadAtLeast(s.cache, b, int(s.cached-s.pos))
		if err == io.EOF {
			err = nil
		}
		s.pos += int64(n)
		return
	}
	d.Chk.Equal(s.cached, s.pos, "Position is somehow _after_ the cached data!")
	if n, err = io.ReadFull(s.r, b); err != nil {
		return
	}
	if _, werr := s.cache.Write(b); werr != nil {
		return 0, werr
	}
	s.pos += int64(n)
	s.cached = s.pos
	return
}

func (s *seekableReader) Seek(offset int64, whence int) (ret int64, err error) {
	if offset < 0 {
		return -1, fmt.Errorf("Cannot seek to negative offset %d", offset)
	}

	switch whence {
	default:
		return -1, fmt.Errorf("whence must be one of 0, 1, or 2; not %d", whence)
	case 0:
		ret = offset
	case 1:
		ret = s.pos + offset
	case 2:
		ret = s.length - offset
	}
	if ret < s.cached {
		if _, err = s.cache.Seek(ret, 0); err != nil {
			return
		}
	} else if ret > s.cached {
		var n int64
		if n, err = io.CopyN(s.cache, s.r, ret-s.cached); err != nil {
			return
		}
		s.cached += n
		d.Chk.Equal(ret, s.cached)
	}
	s.pos = ret
	return
}

func (s *seekableReader) Close() error {
	defer func() { d.Chk.NoError(s.cache.Close()) }()
	return os.Remove(s.cache.Name())
}
