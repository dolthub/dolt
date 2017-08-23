package ipns

import "io"

type WriteAtBuf interface {
	io.WriterAt
	Bytes() []byte
}

type writerAt struct {
	buf []byte
}

func NewWriterAtFromBytes(b []byte) WriteAtBuf {
	return &writerAt{b}
}

// TODO: make this better in the future, this is just a quick hack for now
func (wa *writerAt) WriteAt(p []byte, off int64) (int, error) {
	if off+int64(len(p)) > int64(len(wa.buf)) {
		wa.buf = append(wa.buf, make([]byte, (int(off)+len(p))-len(wa.buf))...)
	}
	copy(wa.buf[off:], p)
	return len(p), nil
}

func (wa *writerAt) Bytes() []byte {
	return wa.buf
}
