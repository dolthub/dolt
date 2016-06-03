package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/util"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/util/outputpager"
)

const (
	addPrefix = "+   "
	subPrefix = "-   "
)

var (
	showHelp = flag.Bool("help", false, "show help text")
	diffQ    = NewDiffQueue()
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Shows the difference between two objects\n")
		fmt.Fprintln(os.Stderr, "Usage: noms diff <object1> <object2>\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nSee \"Spelling Objects\" at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object argument.\n\n")
	}

	flag.Parse()
	if *showHelp {
		flag.Usage()
		return
	}

	if len(flag.Args()) != 2 {
		util.CheckError(errors.New("expected exactly two arguments"))
	}

	spec1, err := flags.ParsePathSpec(flag.Arg(0))
	util.CheckError(err)
	spec2, err := flags.ParsePathSpec(flag.Arg(1))
	util.CheckError(err)

	db1, value1, err := spec1.Value()
	util.CheckError(err)
	defer db1.Close()

	db2, value2, err := spec2.Value()
	util.CheckError(err)
	defer db2.Close()

	di := diffInfo{
		path: types.NewPath().AddField("/"),
		key:  nil,
		v1:   value1,
		v2:   value2,
	}
	diffQ.PushBack(di)

	waitChan := outputpager.PageOutput(!*outputpager.NoPager)

	diff(os.Stdout)
	fmt.Fprintf(os.Stdout, "\n")

	if waitChan != nil {
		os.Stdout.Close()
		<-waitChan
	}
}

func isPrimitiveOrRef(v1 types.Value) bool {
	kind := v1.Type().Kind()
	return types.IsPrimitiveKind(kind) || kind == types.RefKind
}

func canCompare(v1, v2 types.Value) bool {
	return !isPrimitiveOrRef(v1) && v1.Type().Kind() == v2.Type().Kind()
}

func diff(w io.Writer) {
	for di, ok := diffQ.PopFront(); ok; di, ok = diffQ.PopFront() {
		p, key, v1, v2 := di.path, di.key, di.v1, di.v2

		v1.Type().Kind()
		if v1 == nil && v2 != nil {
			line(w, addPrefix, key, v2)
		}
		if v1 != nil && v2 == nil {
			line(w, subPrefix, key, v1)
		}
		if !v1.Equals(v2) {
			if !canCompare(v1, v2) {
				line(w, subPrefix, key, v1)
				line(w, addPrefix, key, v2)
			} else {
				switch v1.Type().Kind() {
				case types.ListKind:
					diffLists(w, p, v1.(types.List), v2.(types.List))
				case types.MapKind:
					diffMaps(w, p, v1.(types.Map), v2.(types.Map))
				case types.SetKind:
					diffSets(w, p, v1.(types.Set), v2.(types.Set))
				case types.StructKind:
					diffStructs(w, p, v1.(types.Struct), v2.(types.Struct))
				default:
					panic("Unrecognized type in diff function")
				}
			}
		}
	}
}

func diffLists(w io.Writer, p types.Path, v1, v2 types.List) {
	wroteHeader := false
	splices, _ := v2.Diff(v1)
	for _, splice := range splices {
		if splice.SpRemoved == splice.SpAdded {
			for i := uint64(0); i < splice.SpRemoved; i++ {
				lastEl := v1.Get(splice.SpAt + i)
				newEl := v2.Get(splice.SpFrom + i)
				if canCompare(lastEl, newEl) {
					idx := types.Number(splice.SpAt + i)
					p1 := p.AddIndex(idx)
					diffQ.PushBack(diffInfo{p1, idx, lastEl, newEl})
				} else {
					wroteHeader = writeHeader(w, wroteHeader, p)
					line(w, subPrefix, nil, v1.Get(splice.SpAt+i))
					line(w, addPrefix, nil, v2.Get(splice.SpFrom+i))
				}
			}
		} else {
			for i := uint64(0); i < splice.SpRemoved; i++ {
				wroteHeader = writeHeader(w, wroteHeader, p)
				line(w, subPrefix, nil, v1.Get(splice.SpAt+i))
			}
			for i := uint64(0); i < splice.SpAdded; i++ {
				wroteHeader = writeHeader(w, wroteHeader, p)
				line(w, addPrefix, nil, v2.Get(splice.SpFrom+i))
			}
		}
	}
	writeFooter(w, wroteHeader)
}

func diffMaps(w io.Writer, p types.Path, v1, v2 types.Map) {
	wroteHeader := false

	added, removed, modified := v2.Diff(v1)
	for _, k := range added {
		wroteHeader = writeHeader(w, wroteHeader, p)
		line(w, addPrefix, k, v2.Get(k))
	}
	for _, k := range removed {
		wroteHeader = writeHeader(w, wroteHeader, p)
		line(w, subPrefix, k, v1.Get(k))
	}
	for _, k := range modified {
		c1, c2 := v1.Get(k), v2.Get(k)
		if canCompare(c1, c2) {
			buf := bytes.NewBuffer(nil)
			types.WriteEncodedValueWithTags(buf, k)
			p1 := p.AddField(buf.String())
			diffQ.PushBack(diffInfo{path: p1, key: k, v1: c1, v2: c2})
		} else {
			wroteHeader = writeHeader(w, wroteHeader, p)
			line(w, subPrefix, k, v1.Get(k))
			line(w, addPrefix, k, v2.Get(k))
		}
	}
	writeFooter(w, wroteHeader)
}

func diffStructs(w io.Writer, p types.Path, v1, v2 types.Struct) {
	changed := types.StructDiff(v1, v2)
	wroteHeader := false
	for _, field := range changed {
		f1 := v1.Get(field)
		f2 := v2.Get(field)
		if canCompare(f1, f2) {
			p1 := p.AddField(field)
			diffQ.PushBack(diffInfo{path: p1, key: types.NewString(field), v1: f1, v2: f2})
		} else {
			wroteHeader = writeHeader(w, wroteHeader, p)
			line(w, subPrefix, types.NewString(field), f1)
			line(w, addPrefix, types.NewString(field), f2)
		}
	}
}

func diffSets(w io.Writer, p types.Path, v1, v2 types.Set) {
	wroteHeader := false
	added, removed := v2.Diff(v1)
	if len(added) == 1 && len(removed) == 1 && canCompare(added[0], removed[0]) {
		p1 := p.AddField(added[0].Hash().String())
		diffQ.PushBack(diffInfo{path: p1, key: types.NewString(""), v1: removed[0], v2: added[0]})
	} else {
		for _, value := range removed {
			wroteHeader = writeHeader(w, wroteHeader, p)
			line(w, subPrefix, nil, value)
		}
		for _, value := range added {
			wroteHeader = writeHeader(w, wroteHeader, p)
			line(w, addPrefix, nil, value)
		}
	}
	writeFooter(w, wroteHeader)
}

type prefixWriter struct {
	w      io.Writer
	prefix []byte
}

// todo: Not sure if we want to use a writer to do this for the longterm but, if so, we can
// probably do better than writing byte by byte
func (pw prefixWriter) Write(bytes []byte) (n int, err error) {
	for i, b := range bytes {
		_, err = pw.w.Write([]byte{b})
		if err != nil {
			return i, err
		}
		if b == '\n' {
			_, err := pw.w.Write(pw.prefix)
			if err != nil {
				return i, err
			}
		}
	}
	return len(bytes), nil
}

func line(w io.Writer, start string, key, v2 types.Value) {
	pw := prefixWriter{w: w, prefix: []byte(start)}
	w.Write([]byte(start))
	if key != nil {
		types.WriteEncodedValueWithTags(pw, key)
		w.Write([]byte(": "))
	}
	types.WriteEncodedValueWithTags(pw, v2)
	w.Write([]byte("\n"))
}

func writeHeader(w io.Writer, wroteHeader bool, p types.Path) bool {
	if !wroteHeader {
		w.Write([]byte(p.String()))
		w.Write([]byte(" {\n"))
		wroteHeader = true
	}
	return wroteHeader
}

func writeFooter(w io.Writer, wroteHeader bool) {
	if wroteHeader {
		w.Write([]byte("  }\n"))
	}
}
