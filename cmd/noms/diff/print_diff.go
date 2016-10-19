// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"io"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/writers"
	"github.com/dustin/go-humanize"
)

type prefixOp string

const (
	ADD = "+   "
	DEL = "-   "
)

type (
	printFunc func(w io.Writer, op prefixOp, key, val types.Value) error
)

// PrintDiff writes a textual reprensentation of the diff from |v1| to |v2|
// to |w|. If |leftRight| is true then the left-right diff is used for ordered
// sequences - see Diff vs DiffLeftRight in Set and Map.
func PrintDiff(w io.Writer, v1, v2 types.Value, leftRight bool) (err error) {
	// In the case where the diff involves two simple values, just print out the
	// diff and return. This is needed because the code below assumes that the
	// values being compared have a parent.
	if !shouldDescend(v1, v2) {
		line(w, DEL, nil, v1)
		return line(w, ADD, nil, v2)
	}

	dChan := make(chan Difference, 16)
	stopChan := make(chan struct{})
	stopDiff := func() {
		close(stopChan)
		for range dChan {
		}
	}

	// From here on, we can assume that every Difference will have at least one
	// element in the Path
	go func() {
		Diff(v1, v2, dChan, stopChan, leftRight)
		close(dChan)
	}()

	var lastParentPath types.Path
	wroteHdr := false
	firstTime := true

	for d := range dChan {
		parentPath := d.Path[:len(d.Path)-1]
		parentPathChanged := !parentPath.Equals(lastParentPath)
		lastParentPath = parentPath
		if parentPathChanged && wroteHdr {
			err = writeFooter(w, &wroteHdr)
		}
		if parentPathChanged || firstTime {
			firstTime = false
			err = writeHeader(w, parentPath, &wroteHdr)
		}

		lastPart := d.Path[len(d.Path)-1]
		parentEl := parentPath.Resolve(v1)

		var key types.Value
		var pfunc printFunc = line

		switch parentEl.(type) {
		case types.Map:
			key = lastPart.(types.IndexPath).Index
		case types.Set:
			// default values are ok
		case types.Struct:
			key = types.String(lastPart.(types.FieldPath).Name)
			pfunc = field
		case types.List:
			// default values are ok
		}

		if d.OldValue != nil {
			err = pfunc(w, DEL, key, d.OldValue)
		}
		if d.NewValue != nil {
			err = pfunc(w, ADD, key, d.NewValue)
		}
		if err != nil {
			stopDiff()
			break
		}
	}
	err = writeFooter(w, &wroteHdr)
	return
}

func writeHeader(w io.Writer, p types.Path, wroteHdr *bool) error {
	if *wroteHdr {
		return nil
	}
	*wroteHdr = true
	hdr := "(root)"
	if len(p) > 0 {
		hdr = p.String()
	}
	return write(w, []byte(hdr+" {\n"))
}

func writeFooter(w io.Writer, wroteHdr *bool) error {
	if !*wroteHdr {
		return nil
	}
	*wroteHdr = false
	return write(w, []byte("  }\n"))
}

func line(w io.Writer, op prefixOp, key, val types.Value) error {
	genPrefix := func(w *writers.PrefixWriter) []byte {
		return []byte(op)
	}
	pw := &writers.PrefixWriter{Dest: w, PrefixFunc: genPrefix, NeedsPrefix: true}
	if key != nil {
		writeEncodedValue(pw, key)
		write(w, []byte(": "))
	}
	writeEncodedValue(pw, val)
	return write(w, []byte("\n"))
}

func field(w io.Writer, op prefixOp, name, val types.Value) error {
	genPrefix := func(w *writers.PrefixWriter) []byte {
		return []byte(op)
	}
	pw := &writers.PrefixWriter{Dest: w, PrefixFunc: genPrefix, NeedsPrefix: true}
	write(pw, []byte(name.(types.String)))
	write(w, []byte(": "))
	writeEncodedValue(pw, val)
	return write(w, []byte("\n"))
}

func writeEncodedValue(w io.Writer, v types.Value) error {
	if v.Type().Kind() != types.BlobKind {
		return types.WriteEncodedValue(w, v)
	}
	write(w, []byte("Blob ("))
	write(w, []byte(humanize.Bytes(v.(types.Blob).Len())))
	return write(w, []byte(")"))
}

func write(w io.Writer, b []byte) error {
	_, err := w.Write(b)
	return err
}
