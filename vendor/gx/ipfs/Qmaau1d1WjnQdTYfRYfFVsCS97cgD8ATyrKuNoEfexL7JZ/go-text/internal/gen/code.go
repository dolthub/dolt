// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gen

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log"
	"os"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"
)

// This file contains utilities for generating code.

// TODO: other write methods like:
// - slices, maps, types, etc.

// CodeWriter is a utility for writing structured code. It computes the content
// hash and size of written content. It ensures there are newlines between
// written code blocks.
type CodeWriter struct {
	buf  bytes.Buffer
	Size int
	Hash hash.Hash32 // content hash
	gob  *gob.Encoder
	// For comments we skip the usual one-line separator if they are followed by
	// a code block.
	skipSep bool
}

func (w *CodeWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

// NewCodeWriter returns a new CodeWriter.
func NewCodeWriter() *CodeWriter {
	h := fnv.New32()
	return &CodeWriter{Hash: h, gob: gob.NewEncoder(h)}
}

// WriteGoFile appends the buffer with the total size of all created structures
// and writes it as a Go file to the the given file with the given package name.
func (w *CodeWriter) WriteGoFile(filename, pkg string) {
	f, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Could not create file %s: %v", filename, err)
	}
	defer f.Close()
	if _, err = w.WriteGo(f, pkg); err != nil {
		log.Fatalf("Error writing file %s: %v", filename, err)
	}
}

// WriteGo appends the buffer with the total size of all created structures and
// writes it as a Go file to the the given writer with the given package name.
func (w *CodeWriter) WriteGo(out io.Writer, pkg string) (n int, err error) {
	sz := w.Size
	w.WriteComment("Total table size %d bytes (%dKiB); checksum: %X\n", sz, sz/1024, w.Hash.Sum32())
	defer w.buf.Reset()
	return WriteGo(out, pkg, w.buf.Bytes())
}

func (w *CodeWriter) printf(f string, x ...interface{}) {
	fmt.Fprintf(w, f, x...)
}

func (w *CodeWriter) insertSep() {
	if w.skipSep {
		w.skipSep = false
		return
	}
	// Use at least two newlines to ensure a blank space between the previous
	// block. WriteGoFile will remove extraneous newlines.
	w.printf("\n\n")
}

// WriteComment writes a comment block. All line starts are prefixed with "//".
// Initial empty lines are gobbled. The indentation for the first line is
// stripped from consecutive lines.
func (w *CodeWriter) WriteComment(comment string, args ...interface{}) {
	s := fmt.Sprintf(comment, args...)
	s = strings.Trim(s, "\n")

	// Use at least two newlines to ensure a blank space between the previous
	// block. WriteGoFile will remove extraneous newlines.
	w.printf("\n\n// ")
	w.skipSep = true

	// strip first indent level.
	sep := "\n"
	for ; len(s) > 0 && (s[0] == '\t' || s[0] == ' '); s = s[1:] {
		sep += s[:1]
	}

	strings.NewReplacer(sep, "\n// ", "\n", "\n// ").WriteString(w, s)

	w.printf("\n")
}

func (w *CodeWriter) writeSizeAndElementInfo(size, n int) {
	w.printf("// Size: %d bytes, %d elements\n", size, n)
}

func (w *CodeWriter) writeSizeInfo(size int) {
	w.printf("// Size: %d bytes\n", size)
}

// WriteConst writes a constant of the given name and value.
func (w *CodeWriter) WriteConst(name string, x interface{}) {
	w.insertSep()
	v := reflect.ValueOf(x)

	switch v.Type().Kind() {
	case reflect.String:
		w.writeSizeInfo(v.Len())
		// See golang.org/issue/13145.
		const arbitraryCutoff = 16
		if v.Len() > arbitraryCutoff {
			w.printf("var %s %s = ", name, typeName(x))
		} else {
			w.printf("const %s %s = ", name, typeName(x))
		}
		w.WriteString(v.String())
		w.printf("\n")
	default:
		w.printf("const %s = %#v\n", name, x)
	}
}

// WriteVar writes a variable of the given name and value.
func (w *CodeWriter) WriteVar(name string, x interface{}) {
	w.insertSep()
	v := reflect.ValueOf(x)
	sz := int(v.Type().Size())

	switch v.Type().Kind() {
	case reflect.String:
		w.writeSizeInfo(v.Len() + sz)
		w.Size += sz
		w.printf("var %s %s = ", name, typeName(x))
		w.WriteString(v.String())
	case reflect.Slice:
		w.writeSizeAndElementInfo(sizeOfArray(x)+sz, v.Len())
		w.Size += sz
		w.printf("var %s = ", name)
		w.writeSlice(x, false, true)
	case reflect.Array:
		w.writeSizeAndElementInfo(sz, v.Len())
		w.printf("var %s = ", name)
		w.writeSlice(x, true, true)
	default:
		w.printf("var %s %s = ", name, typeName(x))
		w.Size += sz
		// TODO: size info?
		w.gob.Encode(x)
		w.printf("%#v", x)
	}
	w.printf("\n")
}

// WriteString writes a string literal.
func (w *CodeWriter) WriteString(s string) {
	io.WriteString(w.Hash, s) // content hash
	w.Size += len(s)

	const maxInline = 40
	if len(s) <= maxInline {
		w.printf("%q", s)
		return
	}

	// We will render the string as a multi-line string.
	const maxWidth = 80 - 4 - len(`"`) - len(`" +`)

	// When starting on its own line, go fmt indents line 2+ an extra level.
	n, max := maxWidth, maxWidth-4

	// Print "" +\n, if a string does not start on its own line.
	b := w.buf.Bytes()
	if p := len(bytes.TrimRight(b, " \t")); p > 0 && b[p-1] != '\n' {
		w.printf("\"\" +\n")
		n, max = maxWidth, maxWidth
	}

	w.printf(`"`)

	for sz, p := 0, 0; p < len(s); {
		var r rune
		r, sz = utf8.DecodeRuneInString(s[p:])
		out := s[p : p+sz]
		chars := 1
		if !unicode.IsPrint(r) || r == utf8.RuneError {
			switch sz {
			case 1:
				out = fmt.Sprintf("\\x%02x", s[p])
			case 2, 3:
				out = fmt.Sprintf("\\u%04x", r)
			case 4:
				out = fmt.Sprintf("\\U%08x", r)
			}
			chars = len(out)
		}
		if n -= chars; n < 0 {
			w.printf("\" +\n\"")
			n = max - len(out)
		}
		w.printf("%s", out)
		p += sz
	}
	w.printf(`"`)
}

// WriteSlice writes a slice value.
func (w *CodeWriter) WriteSlice(x interface{}) {
	w.writeSlice(x, false, false)
}

// WriteArray writes an array value.
func (w *CodeWriter) WriteArray(x interface{}) {
	w.writeSlice(x, true, false)
}

func (w *CodeWriter) writeSlice(x interface{}, isArray, isVar bool) {
	v := reflect.ValueOf(x)
	w.gob.Encode(v.Len())
	w.Size += v.Len() * int(v.Type().Elem().Size())

	name := typeName(x)
	if isArray {
		name = fmt.Sprintf("[%d]%s", v.Len(), name[strings.Index(name, "]")+1:])
	}
	if isArray || isVar {
		w.printf("%s{\n", name)
	} else {
		w.printf("%s{ // %d elements\n", name, v.Len())
	}

	switch kind := v.Type().Elem().Kind(); kind {
	case reflect.String:
		for _, s := range x.([]string) {
			w.WriteString(s)
			w.printf(",\n")
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// nLine and nBlock are the number of elements per line and block.
		nLine, nBlock, format := 8, 64, "%d,"
		switch kind {
		case reflect.Uint8:
			format = "%#02x,"
		case reflect.Uint16:
			format = "%#04x,"
		case reflect.Uint32:
			nLine, nBlock, format = 4, 32, "%#08x,"
		case reflect.Uint, reflect.Uint64:
			nLine, nBlock, format = 4, 32, "%#016x,"
		case reflect.Int8:
			nLine = 16
		}
		n := nLine
		for i := 0; i < v.Len(); i++ {
			if i%nBlock == 0 && v.Len() > nBlock {
				w.printf("// Entry %X - %X\n", i, i+nBlock-1)
			}
			x := v.Index(i).Interface()
			w.gob.Encode(x)
			w.printf(format, x)
			if n--; n == 0 {
				n = nLine
				w.printf("\n")
			}
		}
		w.printf("\n")
	case reflect.Struct:
		for i := 0; i < v.Len(); i++ {
			x := v.Index(i).Interface()
			w.gob.EncodeValue(v)
			line := fmt.Sprintf("%#v,\n", x)
			line = line[strings.IndexByte(line, '{'):]
			w.printf(line)
		}
	default:
		panic("gen: slice type not supported")
	}
	w.printf("}")
}

func sizeOfArray(x interface{}) int {
	v := reflect.ValueOf(x)

	size := v.Len() * int(v.Type().Elem().Size())
	switch v.Type().Elem().Kind() {
	case reflect.String:
		for _, s := range x.([]string) {
			size += len(s)
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			size += sizeOfArray(v.Index(i).Interface())
		}
	case reflect.Array, reflect.Ptr, reflect.Map:
		panic("gen: array element not supported.")
	}
	return size
}

// WriteType writes a definition of the type of the given value and returns the
// type name.
func (w *CodeWriter) WriteType(x interface{}) string {
	t := reflect.TypeOf(x)
	w.printf("type %s struct {\n", t.Name())
	for i := 0; i < t.NumField(); i++ {
		w.printf("\t%s %s\n", t.Field(i).Name, t.Field(i).Type)
	}
	w.printf("}\n")
	return t.Name()
}

// typeName returns the name of the go type of x.
func typeName(x interface{}) string {
	t := reflect.ValueOf(x).Type()
	return strings.Replace(fmt.Sprint(t), "main.", "", 1)
}
