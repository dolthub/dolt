package iohelp

import (
	"bufio"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// ErrPreservingReader is a utility class that provides methods to read from a reader where errors can be ignored and
// handled later.  Once an error occurs subsequent calls to read won't pull data from the io.Reader, will be a noop, and
// the initial error can be retrieved from Err at any time.  ErrPreservingReader implements the io.Reader interface
// itself so it can be used as any other Reader would be.
type ErrPreservingReader struct {
	// R is the reader supplying the actual data.
	R io.Reader

	// Err is the first error that occurred, or nil
	Err error
}

// NewErrPreservingReader creates a new instance of an ErrPreservingReader
func NewErrPreservingReader(r io.Reader) *ErrPreservingReader {
	return &ErrPreservingReader{r, nil}
}

// Read reads data from the underlying io.Reader if no previous errors have occurred.  If an error has already occurred
// then read will simply no-op and return 0 for the number of bytes read and the original error.
func (r *ErrPreservingReader) Read(p []byte) (int, error) {
	n := 0

	if r.Err == nil {
		n, r.Err = r.R.Read(p)
	}

	return n, r.Err
}

// ReadNBytes will read n bytes from the given reader and return a new slice containing the data. ReadNBytes will always
// return n bytes, or it will return no data and an error (So if you request 100 bytes and there are only 99 left before
// the reader returns io.EOF you won't receive any of the data as this is considered an error as it can't read 100 bytes).
func ReadNBytes(r io.Reader, n int) ([]byte, error) {
	bytes := make([]byte, n)

	var err error
	for totalRead := 0; totalRead < n; {
		if err != nil {
			return nil, err
		}

		read := 0
		read, err = r.Read(bytes[totalRead:])

		totalRead += read
	}

	return bytes, nil
}

// ReadLineNoBuf will read a line from an unbuffered io.Reader where it considers lines to be separated by newlines (\n).
// The data returned will be a string with \r\n characters removed from the end, a bool which says whether the end of
// the stream has been reached, and any errors that have been encountered (other than eof which is treated as the end of
// the final line). This isn't efficient, so you shouldn't do this if you can use a buffered reader and the
// iohelp.ReadLine method.
func ReadLineNoBuf(r io.Reader) (string, bool, error) {
	var err error
	var dest []byte
	var oneByte [1]byte

	for {
		var n int
		n, err = r.Read(oneByte[:])

		if err != nil && err != io.EOF {
			return "", true, err
		}

		if n == 1 {
			c := oneByte[0]

			if c == '\n' {
				break
			}

			dest = append(dest, c)
		}

		if err == io.EOF {
			break
		}
	}

	crlfCount := 0
	lineLen := len(dest)
	for i := lineLen - 1; i >= 0; i-- {
		ch := dest[i]

		if ch == '\r' || ch == '\n' {
			crlfCount++
		} else {
			break
		}
	}

	return string(dest[:lineLen-crlfCount]), err != nil, nil
}

// ReadLine will read a line from an unbuffered io.Reader where it considers lines to be separated by newlines (\n).
// The data returned will be a string with \r\n characters removed from the end, a bool which says whether the end of
// the stream has been reached, and any errors that have been encountered (other than eof which is treated as the end of
// the final line)
func ReadLine(br *bufio.Reader) (line string, done bool, err error) {
	line, err = br.ReadString('\n')
	if err != nil {
		if err != io.EOF {
			return "", true, err
		}
	}

	crlfCount := 0
	lineLen := len(line)
	for i := lineLen - 1; i >= 0; i-- {
		ch := line[i]

		if ch == '\r' || ch == '\n' {
			crlfCount++
		} else {
			break
		}
	}

	return line[:lineLen-crlfCount], err != nil, nil
}

/*func ReadLineFromJSON(br *bufio.Reader) (line map[string]interface{}, done bool, err error) {
	line, err = br.ReadMap()
}*/

var ErrThroughput = errors.New("throughput below minimum allowable")

type MinThroughputCheckParams struct {
	MinBytesPerSec int64
	CheckInterval  time.Duration
	NumIntervals   int
}

type datapoint struct {
	ts  time.Time
	val int64
}

type datapoints []datapoint

func (dps datapoints) getThroughput(duration time.Duration) (datapoints, int64) {
	if len(dps) == 0 {
		return dps, 0
	}

	now := time.Now()

	for len(dps) > 1 {
		nextDelta := now.Sub(dps[1].ts)

		if nextDelta > duration {
			dps = dps[1:]
		} else {
			break
		}
	}

	if len(dps) <= 1 {
		return dps, 0
	}

	elapsed := now.Sub(dps[0].ts)
	bytesRead := dps[len(dps)-1].val - dps[0].val

	return dps, int64(float64(bytesRead) / elapsed.Seconds())
}

func safeCloseReader(c io.Closer) {
	defer func() {
		recover()
	}()

	c.Close()
}

type readResults struct {
	bytes []byte
	err   error
}

func ReadAllWithProgress(r io.Reader, n int64, bytesReadSync *int64) ([]byte, error) {
	var totalRead int64
	bytes := make([]byte, n)

	var err error
	for totalRead < n && err == nil {
		var read int
		read, err = r.Read(bytes[totalRead:])

		if err != nil && err != io.EOF {
			break
		}

		totalRead += int64(read)

		if bytesReadSync != nil {
			atomic.StoreInt64(bytesReadSync, totalRead)
		}

		if err == io.EOF {
			err = nil
			if totalRead != n {
				err = io.ErrUnexpectedEOF
			}
		}
	}

	return bytes[:totalRead], err
}

func ReadWithMinThroughput(r io.ReadCloser, n int64, mtcParams MinThroughputCheckParams) ([]byte, error) {
	wg := &sync.WaitGroup{}

	var atomicRes atomic.Value
	var bytesReadSync int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { recover() }()

		bytes, err := ReadAllWithProgress(r, n, &bytesReadSync)
		atomicRes.Store(readResults{bytes, err})
	}()

	checkDuration := mtcParams.CheckInterval * time.Duration(mtcParams.NumIntervals)
	ticker := time.NewTicker(mtcParams.CheckInterval)
	defer ticker.Stop()

	var points datapoints
	var throughputErr bool
	for !throughputErr {
		if val := atomicRes.Load(); val != nil {
			res := val.(readResults)
			return res.bytes, res.err
		}

		<-ticker.C
		read := atomic.LoadInt64(&bytesReadSync)
		points = append(points, datapoint{time.Now(), read})

		if len(points) >= mtcParams.NumIntervals {
			var bps int64
			points, bps = points.getThroughput(checkDuration)

			if bps < mtcParams.MinBytesPerSec {
				safeCloseReader(r)
				throughputErr = true
			}
		}
	}

	wg.Wait()

	if val := atomicRes.Load(); val != nil {
		res := val.(readResults)
		err := res.err

		if throughputErr {
			err = ErrThroughput
		}

		return res.bytes, err
	}

	panic("bug.  Should never reach here.")
}
