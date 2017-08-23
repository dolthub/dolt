package corehttp

import (
	"io"
	"net"
	"net/http"

	core "github.com/ipfs/go-ipfs/core"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
)

type writeErrNotifier struct {
	w    io.Writer
	errs chan error
}

func newWriteErrNotifier(w io.Writer) (io.WriteCloser, <-chan error) {
	ch := make(chan error, 1)
	return &writeErrNotifier{
		w:    w,
		errs: ch,
	}, ch
}

func (w *writeErrNotifier) Write(b []byte) (int, error) {
	n, err := w.w.Write(b)
	if err != nil {
		select {
		case w.errs <- err:
		default:
		}
	}
	if f, ok := w.w.(http.Flusher); ok {
		f.Flush()
	}
	return n, err
}

func (w *writeErrNotifier) Close() error {
	select {
	case w.errs <- io.EOF:
	default:
	}
	return nil
}

func LogOption() ServeOption {
	return func(n *core.IpfsNode, _ net.Listener, mux *http.ServeMux) (*http.ServeMux, error) {
		mux.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
			wnf, errs := newWriteErrNotifier(w)
			logging.WriterGroup.AddWriter(wnf)
			log.Event(n.Context(), "log API client connected")
			<-errs
		})
		return mux, nil
	}
}
