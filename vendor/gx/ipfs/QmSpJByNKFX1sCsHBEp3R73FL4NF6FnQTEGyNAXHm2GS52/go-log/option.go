package log

import (
	"io"

	logging "gx/ipfs/QmQvJiADDe7JR4m968MwXobTCCzUqQkP87aRHe29MEBGHV/go-logging"
)

// Global writer group for logs to output to
var WriterGroup = NewMirrorWriter()

type Option func()

// Configure applies the provided options sequentially from left to right
func Configure(options ...Option) {
	for _, f := range options {
		f()
	}
}

// LdJSONFormatter Option formats the event log as line-delimited JSON
var LdJSONFormatter = func() {
	logging.SetFormatter(&PoliteJSONFormatter{})
}

// TextFormatter Option formats the event log as human-readable plain-text
var TextFormatter = func() {
	logging.SetFormatter(logging.DefaultFormatter)
}

func Output(w io.Writer) Option {
	return func() {
		backend := logging.NewLogBackend(w, "", 0)
		logging.SetBackend(backend)
		// TODO return previous Output option
	}
}

// LevelDebug Option sets the log level to debug
var LevelDebug = func() {
	logging.SetLevel(logging.DEBUG, "")
}

// LevelError Option sets the log level to error
var LevelError = func() {
	logging.SetLevel(logging.ERROR, "")
}

// LevelInfo Option sets the log level to info
var LevelInfo = func() {
	logging.SetLevel(logging.INFO, "")
}
