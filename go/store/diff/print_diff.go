// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"context"
	"io"

	"github.com/dustin/go-humanize"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/liquidata-inc/ld/dolt/go/store/util/writers"
)

type prefixOp string

const (
	ADD = "+   "
	DEL = "-   "
)

type (
	printFunc func(ctx context.Context, w io.Writer, op prefixOp, key, val types.Value) error
)

// PrintDiff writes a textual reprensentation of the diff from |v1| to |v2|
// to |w|. If |leftRight| is true then the left-right diff is used for ordered
// sequences - see Diff vs DiffLeftRight in Set and Map.
func PrintDiff(ctx context.Context, w io.Writer, v1, v2 types.Value, leftRight bool) (err error) {
	// In the case where the diff involves two simple values, just print out the
	// diff and return. This is needed because the code below assumes that the
	// values being compared have a parent.
	if !ShouldDescend(v1, v2) {
		line(ctx, w, DEL, nil, v1)
		return line(ctx, w, ADD, nil, v2)
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
		Diff(ctx, v1, v2, dChan, stopChan, leftRight, nil)
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
		parentEl := parentPath.Resolve(ctx, types.Format_7_18, v1, nil)

		var key types.Value
		var pfunc printFunc = line

		switch parent := parentEl.(type) {
		case types.Map:
			if indexPath, ok := lastPart.(types.IndexPath); ok {
				key = indexPath.Index
			} else if hip, ok := lastPart.(types.HashIndexPath); ok {
				// In this case, the map has a non-primitive key so the value
				// is a ref to the key. We need the actual key, not a ref to it.
				hip1 := hip
				hip1.IntoKey = true
				key = hip1.Resolve(ctx, types.Format_7_18, parent, nil)
			} else {
				panic("unexpected Path type")
			}
		case types.Set:
			// default values are ok
		case types.Struct:
			key = types.String(lastPart.(types.FieldPath).Name)
			pfunc = field
		case types.List:
			// default values are ok
		}

		if d.OldValue != nil {
			err = pfunc(ctx, w, DEL, key, d.OldValue)
		}
		if d.NewValue != nil {
			err = pfunc(ctx, w, ADD, key, d.NewValue)
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

func line(ctx context.Context, w io.Writer, op prefixOp, key, val types.Value) error {
	genPrefix := func(w *writers.PrefixWriter) []byte {
		return []byte(op)
	}
	pw := &writers.PrefixWriter{Dest: w, PrefixFunc: genPrefix, NeedsPrefix: true}
	if key != nil {
		writeEncodedValue(ctx, pw, key)
		write(w, []byte(": "))
	}
	writeEncodedValue(ctx, pw, val)
	return write(w, []byte("\n"))
}

func field(ctx context.Context, w io.Writer, op prefixOp, name, val types.Value) error {
	genPrefix := func(w *writers.PrefixWriter) []byte {
		return []byte(op)
	}
	pw := &writers.PrefixWriter{Dest: w, PrefixFunc: genPrefix, NeedsPrefix: true}
	write(pw, []byte(name.(types.String)))
	write(w, []byte(": "))
	writeEncodedValue(ctx, pw, val)
	return write(w, []byte("\n"))
}

func writeEncodedValue(ctx context.Context, w io.Writer, v types.Value) error {
	if v.Kind() != types.BlobKind {
		return types.WriteEncodedValue(ctx, types.Format_7_18, w, v)
	}
	write(w, []byte("Blob ("))
	write(w, []byte(humanize.Bytes(v.(types.Blob).Len())))
	return write(w, []byte(")"))
}

func write(w io.Writer, b []byte) error {
	_, err := w.Write(b)
	return err
}
