package multistream

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

var ErrTooLarge = errors.New("incoming message was too large")

const ProtocolID = "/multistream/1.0.0"

type HandlerFunc func(string, io.ReadWriteCloser) error

type Handler struct {
	MatchFunc func(string) bool
	Handle    HandlerFunc
	AddName   string
}

type MultistreamMuxer struct {
	handlerlock sync.Mutex
	handlers    []Handler
}

func NewMultistreamMuxer() *MultistreamMuxer {
	return new(MultistreamMuxer)
}

func writeUvarint(w io.Writer, i uint64) error {
	varintbuf := make([]byte, 16)
	n := binary.PutUvarint(varintbuf, i)
	_, err := w.Write(varintbuf[:n])
	if err != nil {
		return err
	}
	return nil
}

func delimWriteBuffered(w io.Writer, mes []byte) error {
	bw := bufio.NewWriter(w)
	err := delimWrite(bw, mes)
	if err != nil {
		return err
	}

	return bw.Flush()
}

func delimWrite(w io.Writer, mes []byte) error {
	err := writeUvarint(w, uint64(len(mes)+1))
	if err != nil {
		return err
	}

	_, err = w.Write(mes)
	if err != nil {
		return err
	}

	_, err = w.Write([]byte{'\n'})
	if err != nil {
		return err
	}
	return nil
}

func Ls(rw io.ReadWriter) ([]string, error) {
	err := delimWriteBuffered(rw, []byte("ls"))
	if err != nil {
		return nil, err
	}

	n, err := binary.ReadUvarint(&byteReader{rw})
	if err != nil {
		return nil, err
	}

	var out []string
	for i := uint64(0); i < n; i++ {
		val, err := lpReadBuf(rw)
		if err != nil {
			return nil, err
		}
		out = append(out, string(val))
	}

	return out, nil
}

func fulltextMatch(s string) func(string) bool {
	return func(a string) bool {
		return a == s
	}
}

func (msm *MultistreamMuxer) AddHandler(protocol string, handler HandlerFunc) {
	msm.AddHandlerWithFunc(protocol, fulltextMatch(protocol), handler)
}

func (msm *MultistreamMuxer) AddHandlerWithFunc(protocol string, match func(string) bool, handler HandlerFunc) {
	msm.handlerlock.Lock()
	msm.removeHandler(protocol)
	msm.handlers = append(msm.handlers, Handler{
		MatchFunc: fulltextMatch(protocol),
		Handle:    handler,
		AddName:   protocol,
	})
	msm.handlerlock.Unlock()
}

func (msm *MultistreamMuxer) RemoveHandler(protocol string) {
	msm.handlerlock.Lock()
	defer msm.handlerlock.Unlock()

	msm.removeHandler(protocol)
}

func (msm *MultistreamMuxer) removeHandler(protocol string) {
	for i, h := range msm.handlers {
		if h.AddName == protocol {
			msm.handlers = append(msm.handlers[:i], msm.handlers[i+1:]...)
			return
		}
	}
}

func (msm *MultistreamMuxer) Protocols() []string {
	var out []string
	msm.handlerlock.Lock()
	for _, h := range msm.handlers {
		out = append(out, h.AddName)
	}
	msm.handlerlock.Unlock()
	return out
}

var ErrIncorrectVersion = errors.New("client connected with incorrect version")

func (msm *MultistreamMuxer) findHandler(proto string) *Handler {
	msm.handlerlock.Lock()
	defer msm.handlerlock.Unlock()

	for _, h := range msm.handlers {
		if h.MatchFunc(proto) {
			return &h
		}
	}

	return nil
}

func (msm *MultistreamMuxer) NegotiateLazy(rwc io.ReadWriteCloser) (Multistream, string, HandlerFunc, error) {
	pval := make(chan string, 1)
	writeErr := make(chan error, 1)
	defer close(pval)

	lzc := &lazyConn{
		con:        rwc,
		rhandshake: true,
		rhsync:     true,
	}

	// take lock here to prevent a race condition where the reads below from
	// finishing and taking the write lock before this goroutine can
	lzc.whlock.Lock()

	go func() {
		defer close(writeErr)
		defer lzc.whlock.Unlock()
		lzc.whsync = true

		if err := delimWriteBuffered(rwc, []byte(ProtocolID)); err != nil {
			lzc.werr = err
			writeErr <- err
			return
		}

		for proto := range pval {
			if err := delimWriteBuffered(rwc, []byte(proto)); err != nil {
				lzc.werr = err
				writeErr <- err
				return
			}
		}
		lzc.whandshake = true
	}()

	line, err := ReadNextToken(rwc)
	if err != nil {
		return nil, "", nil, err
	}

	if line != ProtocolID {
		rwc.Close()
		return nil, "", nil, ErrIncorrectVersion
	}

loop:
	for {
		// Now read and respond to commands until they send a valid protocol id
		tok, err := ReadNextToken(rwc)
		if err != nil {
			return nil, "", nil, err
		}

		switch tok {
		case "ls":
			select {
			case pval <- "ls":
			case err := <-writeErr:
				return nil, "", nil, err
			}
		default:
			h := msm.findHandler(tok)
			if h == nil {
				select {
				case pval <- "na":
				case err := <-writeErr:
					return nil, "", nil, err
				}
				continue loop
			}

			select {
			case pval <- tok:
			case <-writeErr:
				// explicitly ignore this error. It will be returned to any
				// writers and if we don't plan on writing anything, we still
				// want to complete the handshake
			}

			// hand off processing to the sub-protocol handler
			return lzc, tok, h.Handle, nil
		}
	}
}

func (msm *MultistreamMuxer) Negotiate(rwc io.ReadWriteCloser) (string, HandlerFunc, error) {
	// Send our protocol ID
	err := delimWriteBuffered(rwc, []byte(ProtocolID))
	if err != nil {
		return "", nil, err
	}

	line, err := ReadNextToken(rwc)
	if err != nil {
		return "", nil, err
	}

	if line != ProtocolID {
		rwc.Close()
		return "", nil, ErrIncorrectVersion
	}

loop:
	for {
		// Now read and respond to commands until they send a valid protocol id
		tok, err := ReadNextToken(rwc)
		if err != nil {
			return "", nil, err
		}

		switch tok {
		case "ls":
			err := msm.Ls(rwc)
			if err != nil {
				return "", nil, err
			}
		default:
			h := msm.findHandler(tok)
			if h == nil {
				err := delimWriteBuffered(rwc, []byte("na"))
				if err != nil {
					return "", nil, err
				}
				continue loop
			}

			err := delimWriteBuffered(rwc, []byte(tok))
			if err != nil {
				return "", nil, err
			}

			// hand off processing to the sub-protocol handler
			return tok, h.Handle, nil
		}
	}

}

func (msm *MultistreamMuxer) Ls(w io.Writer) error {
	buf := new(bytes.Buffer)
	msm.handlerlock.Lock()
	err := writeUvarint(buf, uint64(len(msm.handlers)))
	if err != nil {
		return err
	}

	for _, h := range msm.handlers {
		err := delimWrite(buf, []byte(h.AddName))
		if err != nil {
			msm.handlerlock.Unlock()
			return err
		}
	}
	msm.handlerlock.Unlock()
	ll := make([]byte, 16)
	nw := binary.PutUvarint(ll, uint64(buf.Len()))

	r := io.MultiReader(bytes.NewReader(ll[:nw]), buf)

	_, err = io.Copy(w, r)
	return err
}

func (msm *MultistreamMuxer) Handle(rwc io.ReadWriteCloser) error {
	p, h, err := msm.Negotiate(rwc)
	if err != nil {
		return err
	}
	return h(p, rwc)
}

func ReadNextToken(rw io.ReadWriter) (string, error) {
	tok, err := ReadNextTokenBytes(rw)
	if err != nil {
		return "", err
	}

	return string(tok), nil
}

func ReadNextTokenBytes(rw io.ReadWriter) ([]byte, error) {
	data, err := lpReadBuf(rw)
	switch err {
	case nil:
		return data, nil
	case ErrTooLarge:
		err := delimWriteBuffered(rw, []byte("messages over 64k are not allowed"))
		if err != nil {
			return nil, err
		}
		return nil, ErrTooLarge
	default:
		return nil, err
	}
}

func lpReadBuf(r io.Reader) ([]byte, error) {
	var br byteReaderIface
	if mbr, ok := r.(byteReaderIface); ok {
		br = mbr
	} else {
		br = &byteReader{r}
	}

	length, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}

	if length > 64*1024 {
		return nil, ErrTooLarge
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(br, buf)
	if err != nil {
		return nil, err
	}

	if len(buf) == 0 || buf[length-1] != '\n' {
		return nil, errors.New("message did not have trailing newline")
	}

	// slice off the trailing newline
	buf = buf[:length-1]

	return buf, nil

}

type byteReaderIface interface {
	Read([]byte) (int, error)
	ReadByte() (byte, error)
}

// byteReader implements the ByteReader interface that ReadUVarint requires
type byteReader struct {
	io.Reader
}

func (br *byteReader) ReadByte() (byte, error) {
	var b [1]byte
	_, err := br.Read(b[:])

	if err != nil {
		return 0, err
	}
	return b[0], nil
}
