package ldio

import (
	"bufio"
	"io"
)

type ErrPreservingReader struct {
	R   io.Reader
	Err error
}

func (r *ErrPreservingReader) Read(p []byte) (int, error) {
	n := 0

	if r.Err == nil {
		n, r.Err = r.R.Read(p)
	}

	return n, r.Err
}

func ReadLine(br *bufio.Reader) (string, bool, error) {
	line, err := br.ReadString('\n')
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

func ReadLineNoBuf(r io.Reader) (string, bool, error) {
	var err error
	var dest []byte
	var oneByte [1]byte

	for {
		n, err := r.Read(oneByte[:])

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
