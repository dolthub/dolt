// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/diff"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/datetime"
	"github.com/attic-labs/noms/go/util/functions"
	"github.com/attic-labs/noms/go/util/outputpager"
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/attic-labs/noms/go/util/writers"
	flag "github.com/juju/gnuflag"
	"github.com/mgutz/ansi"
)

var (
	useColor   = false
	color      int
	maxLines   int
	maxCommits int
	oneline    bool
	showGraph  bool
	showValue  bool
)

const parallelism = 16

var nomsLog = &util.Command{
	Run:       runLog,
	UsageLine: "log [options] <path-spec>",
	Short:     "Displays the history of a path",
	Long:      "Displays the history of a path. See Spelling Values at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the <path-spec> parameter.",
	Flags:     setupLogFlags,
	Nargs:     1,
}

func setupLogFlags() *flag.FlagSet {
	logFlagSet := flag.NewFlagSet("log", flag.ExitOnError)
	logFlagSet.IntVar(&color, "color", -1, "value of 1 forces color on, 0 forces color off")
	logFlagSet.IntVar(&maxLines, "max-lines", 9, "max number of lines to show per commit (-1 for all lines)")
	logFlagSet.IntVar(&maxCommits, "n", 0, "max number of commits to display (0 for all commits)")
	logFlagSet.BoolVar(&oneline, "oneline", false, "show a summary of each commit on a single line")
	logFlagSet.BoolVar(&showGraph, "graph", false, "show ascii-based commit hierarchy on left side of output")
	logFlagSet.BoolVar(&showValue, "show-value", false, "show commit value rather than diff information")
	outputpager.RegisterOutputpagerFlags(logFlagSet)
	verbose.RegisterVerboseFlags(logFlagSet)
	return logFlagSet
}

func runLog(args []string) int {
	useColor = shouldUseColor()
	cfg := config.NewResolver()

	resolved := cfg.ResolvePathSpec(args[0])
	sp, err := spec.ForPath(resolved)
	d.CheckErrorNoUsage(err)
	defer sp.Close()

	pinned, ok := sp.Pin()
	if !ok {
		fmt.Fprintf(os.Stderr, "Cannot resolve spec: %s\n", args[0])
		return 1
	}
	defer pinned.Close()
	database := pinned.GetDatabase()

	absPath := pinned.Path
	path := absPath.Path
	if len(path) == 0 {
		path = types.MustParsePath(".value")
	}

	origCommit, ok := database.ReadValue(absPath.Hash).(types.Struct)
	if !ok || !datas.IsCommit(origCommit) {
		d.CheckError(fmt.Errorf("%s does not reference a Commit object", args[0]))
	}

	iter := NewCommitIterator(database, origCommit)
	displayed := 0
	if maxCommits <= 0 {
		maxCommits = math.MaxInt32
	}

	bytesChan := make(chan chan []byte, parallelism)

	var done = false

	go func() {
		for ln, ok := iter.Next(); !done && ok && displayed < maxCommits; ln, ok = iter.Next() {
			ch := make(chan []byte)
			bytesChan <- ch

			go func(ch chan []byte, node LogNode) {
				buff := &bytes.Buffer{}
				printCommit(node, path, buff, database)
				ch <- buff.Bytes()
			}(ch, ln)

			displayed++
		}
		close(bytesChan)
	}()

	pgr := outputpager.Start()
	defer pgr.Stop()

	for ch := range bytesChan {
		commitBuff := <-ch
		_, err := io.Copy(pgr.Writer, bytes.NewReader(commitBuff))
		if err != nil {
			done = true
			for range bytesChan {
				// drain the output
			}
		}
	}

	return 0
}

// Prints the information for one commit in the log, including ascii graph on left side of commits if
// -graph arg is true.
func printCommit(node LogNode, path types.Path, w io.Writer, db datas.Database) (err error) {
	maxMetaFieldNameLength := func(commit types.Struct) int {
		maxLen := 0
		if m, ok := commit.MaybeGet(datas.MetaField); ok {
			meta := m.(types.Struct)
			types.TypeOf(meta).Desc.(types.StructDesc).IterFields(func(name string, t *types.Type, optional bool) {
				maxLen = max(maxLen, len(name))
			})
		}
		return maxLen
	}

	hashStr := node.commit.Hash().String()
	if useColor {
		hashStr = ansi.Color("commit "+hashStr, "red+h")
	}

	maxFieldNameLen := maxMetaFieldNameLength(node.commit)

	parentLabel := "Parent"
	parentValue := "None"
	parents := commitRefsFromSet(node.commit.Get(datas.ParentsField).(types.Set))
	if len(parents) > 1 {
		pstrings := make([]string, len(parents))
		for i, p := range parents {
			pstrings[i] = p.TargetHash().String()
		}
		parentLabel = "Merge"
		parentValue = strings.Join(pstrings, " ")
	} else if len(parents) == 1 {
		parentValue = parents[0].TargetHash().String()
	}

	if oneline {
		parentStr := fmt.Sprintf("%s %s", parentLabel+":", parentValue)
		fmt.Fprintf(w, "%s (%s)\n", hashStr, parentStr)
		return
	}

	maxFieldNameLen = max(maxFieldNameLen, len(parentLabel))
	parentStr := fmt.Sprintf("%-*s %s", maxFieldNameLen+1, parentLabel+":", parentValue)
	fmt.Fprintf(w, "%s%s\n", genGraph(node, 0), hashStr)
	fmt.Fprintf(w, "%s%s\n", genGraph(node, 1), parentStr)
	lineno := 1

	if maxLines != 0 {
		lineno, err = writeMetaLines(node, maxLines, lineno, maxFieldNameLen, w)
		if err != nil && err != writers.MaxLinesErr {
			fmt.Fprintf(w, "error: %s\n", err)
			return
		}

		if showValue {
			_, err = writeCommitLines(node, path, maxLines, lineno, w, db)
		} else {
			_, err = writeDiffLines(node, path, db, maxLines, lineno, w)
		}
	}
	return
}

// Generates ascii graph chars to display on the left side of the commit info if -graph arg is true.
func genGraph(node LogNode, lineno int) string {
	if !showGraph {
		return ""
	}

	// branchCount is the number of branches that we need to graph for this commit and determines the
	// length of prefix string. The string will change from line to line to indicate whether the new
	// branches are getting created or currently displayed branches need to be merged with other branches.
	// Normally we want the maximum number of branches so we have enough room to display them all, however
	// if node.Shrunk() is true, we only need to display the minimum number of branches.
	branchCount := max(node.startingColCount, node.endingColCount)
	if node.Shrunk() {
		branchCount = min(node.startingColCount, node.endingColCount)
	}

	// Create the basic prefix string indicating the number of branches that are being tracked.
	p := strings.Repeat("| ", max(branchCount, 1))
	buf := []rune(p)

	// The first line of a commit has a '*' in the graph to indicate what branch it resides in.
	if lineno == 0 {
		if node.Expanding() {
			buf[(branchCount-1)*2] = ' '
		}
		buf[node.col*2] = '*'
		return string(buf)
	}

	// If expanding, change all the '|' chars to '\' chars after the inserted branch
	if node.Expanding() && lineno == 1 {
		for i := node.newCols[0]; i < branchCount; i++ {
			buf[(i*2)-1] = '\\'
			buf[i*2] = ' '
		}
	}

	// if one branch is getting folded into another, show '/' where necessary to indicate that.
	if node.Shrinking() {
		foldingDistance := node.foldedCols[1] - node.foldedCols[0]
		ch := ' '
		if lineno < foldingDistance+1 {
			ch = '/'
		}
		for _, col := range node.foldedCols[1:] {
			buf[(col*2)-1] = ch
			buf[(col * 2)] = ' '
		}
	}

	return string(buf)
}

func writeMetaLines(node LogNode, maxLines, lineno, maxLabelLen int, w io.Writer) (int, error) {
	if m, ok := node.commit.MaybeGet(datas.MetaField); ok {
		genPrefix := func(w *writers.PrefixWriter) []byte {
			return []byte(genGraph(node, int(w.NumLines)))
		}
		meta := m.(types.Struct)
		mlw := &writers.MaxLineWriter{Dest: w, MaxLines: uint32(maxLines), NumLines: uint32(lineno)}
		pw := &writers.PrefixWriter{Dest: mlw, PrefixFunc: genPrefix, NeedsPrefix: true, NumLines: uint32(lineno)}
		err := d.Try(func() {
			types.TypeOf(meta).Desc.(types.StructDesc).IterFields(func(fieldName string, t *types.Type, optional bool) {
				v := meta.Get(fieldName)
				fmt.Fprintf(pw, "%-*s", maxLabelLen+2, strings.Title(fieldName)+":")
				if types.TypeOf(v).Equals(datetime.DateTimeType) {
					var dt datetime.DateTime
					dt.UnmarshalNoms(v)
					fmt.Fprintf(pw, dt.Format(spec.CommitMetaDateFormat))
				} else {
					types.WriteEncodedValue(pw, v)
				}
				fmt.Fprintf(pw, "\n")
			})
		})
		return int(pw.NumLines), err
	}
	return lineno, nil
}

func writeCommitLines(node LogNode, path types.Path, maxLines, lineno int, w io.Writer, db datas.Database) (lineCnt int, err error) {
	genPrefix := func(pw *writers.PrefixWriter) []byte {
		return []byte(genGraph(node, int(pw.NumLines)+1))
	}
	mlw := &writers.MaxLineWriter{Dest: w, MaxLines: uint32(maxLines), NumLines: uint32(lineno)}
	pw := &writers.PrefixWriter{Dest: mlw, PrefixFunc: genPrefix, NeedsPrefix: true, NumLines: uint32(lineno)}
	v := path.Resolve(node.commit, db)
	if v == nil {
		pw.Write([]byte("<nil>\n"))
	} else {
		err = types.WriteEncodedValue(pw, v)
		mlw.MaxLines = 0
		if err != nil {
			d.PanicIfNotType(writers.MaxLinesErr, err)
			pw.NeedsPrefix = true
			pw.Write([]byte("...\n"))
			err = nil
		} else {
			pw.NeedsPrefix = false
			pw.Write([]byte("\n"))
		}
		if !node.lastCommit {
			pw.NeedsPrefix = true
			pw.Write([]byte("\n"))
		}
	}
	return int(pw.NumLines), err
}

func writeDiffLines(node LogNode, path types.Path, db datas.Database, maxLines, lineno int, w io.Writer) (lineCnt int, err error) {
	genPrefix := func(w *writers.PrefixWriter) []byte {
		return []byte(genGraph(node, int(w.NumLines)+1))
	}
	mlw := &writers.MaxLineWriter{Dest: w, MaxLines: uint32(maxLines), NumLines: uint32(lineno)}
	pw := &writers.PrefixWriter{Dest: mlw, PrefixFunc: genPrefix, NeedsPrefix: true, NumLines: uint32(lineno)}
	parents := node.commit.Get(datas.ParentsField).(types.Set)
	var parent types.Value
	if parents.Len() > 0 {
		parent = parents.First()
	}
	if parent == nil {
		_, err = fmt.Fprint(pw, "\n")
		return 1, err
	}

	parentCommit := parent.(types.Ref).TargetValue(db).(types.Struct)
	var old, neu types.Value
	functions.All(
		func() { old = path.Resolve(parentCommit, db) },
		func() { neu = path.Resolve(node.commit, db) },
	)

	// TODO: It would be better to treat this as an add or remove, but that requires generalization
	// of some of the code in PrintDiff() because it cannot tolerate nil parameters.
	if neu == nil {
		fmt.Fprintf(pw, "new (#%s%s) not found\n", node.commit.Hash().String(), path.String())
	}
	if old == nil {
		fmt.Fprintf(pw, "old (#%s%s) not found\n", parentCommit.Hash().String(), path.String())
	}

	if old != nil && neu != nil {
		err = diff.PrintDiff(pw, old, neu, true)
		mlw.MaxLines = 0
		if err != nil {
			d.PanicIfNotType(err, writers.MaxLinesErr)
			pw.NeedsPrefix = true
			pw.Write([]byte("...\n"))
			err = nil
		}
	}
	if !node.lastCommit {
		pw.NeedsPrefix = true
		pw.Write([]byte("\n"))
	}
	return int(pw.NumLines), err
}

func shouldUseColor() bool {
	if color != 1 && color != 0 {
		return outputpager.IsStdoutTty()
	}
	return color == 1
}

func max(i, j int) int {
	if i > j {
		return i
	}
	return j
}

func min(i, j int) int {
	if i < j {
		return i
	}
	return j
}
