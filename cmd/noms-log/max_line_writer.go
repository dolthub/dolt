// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"io"
)

var (
	maxLinesError = MaxLinesError{"Maximum number of lines written"}
)

type MaxLinesError struct {
	msg string
}

func (e MaxLinesError) Error() string { return e.msg }

type maxLineWriter struct {
	numLines    int
	maxLines    int
	node        LogNode
	dest        io.Writer
	needsPrefix bool
}

func (w *maxLineWriter) writeMax(data []byte, enforceMax bool) (byteCnt int, err error) {
	for _, b := range data {
		if w.needsPrefix {
			w.needsPrefix = false
			w.numLines++
			if *showGraph {
				s := genGraph(w.node, w.numLines)
				_, err = w.dest.Write([]byte(s))
			}
			if err != nil {
				return
			}
		}
		byteCnt++
		if enforceMax && w.maxLines >= 0 && w.numLines >= w.maxLines {
			err = maxLinesError
			return
		}
		// TODO: This is not technically correct due to utf-8, but ... meh.
		w.needsPrefix = b == byte('\n')
		_, err = w.dest.Write(data[byteCnt-1 : byteCnt])
		if err != nil {
			return
		}
	}
	return
}

func (w *maxLineWriter) Write(data []byte) (byteCnt int, err error) {
	return w.writeMax(data, true)
}

func (w *maxLineWriter) forceWrite(data []byte) (byteCnt int, err error) {
	return w.writeMax(data, false)
}
