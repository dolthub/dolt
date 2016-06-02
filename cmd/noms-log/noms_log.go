// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/util"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/util/outputpager"
	"github.com/mgutz/ansi"
)

var (
	color         = flag.Int("color", -1, "value of 1 forces color on, 2 forces color off")
	maxLines      = flag.Int("max-lines", 10, "max number of lines to show per commit (-1 for all lines)")
	maxCommits    = flag.Int("n", 0, "max number of commits to display (0 for all commits)")
	showHelp      = flag.Bool("help", false, "show help text")
	showGraph     = flag.Bool("graph", false, "show ascii-based commit hierarcy on left side of output")
	useColor      = false
	maxLinesError = errors.New("Maximum number of lines written")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Displays the history of a Noms dataset\n")
		fmt.Fprintln(os.Stderr, "Usage: noms log <commitObject>")
		fmt.Fprintln(os.Stderr, "commitObject must be a dataset or object spec that refers to a commit.")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nSee \"Spelling Objects\" at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object argument.\n\n")
	}

	flag.Parse()
	if *showHelp {
		flag.Usage()
		return
	}

	if len(flag.Args()) != 1 {
		util.CheckError(errors.New("expected exactly one argument"))
	}

	useColor = shouldUseColor()

	spec, err := flags.ParsePathSpec(flag.Arg(0))
	util.CheckError(err)
	database, value, err := spec.Value()
	if err != nil {
		util.CheckErrorNoUsage(err)
	}
	defer database.Close()

	waitChan := outputpager.PageOutput(!*outputpager.NoPager)

	origCommit, ok := value.(types.Struct)
	if !ok || !origCommit.Type().Equals(datas.CommitType()) {
		util.CheckError(fmt.Errorf("%s does not reference a Commit object", spec))
	}

	iter := NewCommitIterator(database, origCommit)
	displayed := 0
	if *maxCommits <= 0 {
		*maxCommits = math.MaxInt32
	}
	for ln, ok := iter.Next(); ok && displayed < *maxCommits; ln, ok = iter.Next() {
		if printCommit(ln) != nil {
			break
		}
		displayed++
	}

	if waitChan != nil {
		os.Stdout.Close()
		<-waitChan
	}
}

// Prints the information for one commit in the log, including ascii graph on left side of commits if
// -graph arg is true.
func printCommit(node LogNode) (err error) {
	lineno := 0
	doColor := func(s string) string { return s }
	if useColor {
		doColor = ansi.ColorFunc("red+h")
	}

	fmt.Printf("%s%s\n", genGraph(node, lineno), doColor(node.commit.Hash().String()))
	parents := commitRefsFromSet(node.commit.Get(datas.ParentsField).(types.Set))
	lineno++
	if len(parents) > 1 {
		pstrings := []string{}
		for _, cr := range parents {
			pstrings = append(pstrings, cr.TargetHash().String())
		}
		fmt.Printf("%sMerge: %s\n", genGraph(node, lineno), strings.Join(pstrings, " "))
	} else if len(parents) == 1 {
		fmt.Printf("%sParent: %s\n", genGraph(node, lineno), parents[0].TargetHash().String())
	} else {
		fmt.Printf("%sParent: None\n", genGraph(node, lineno))
	}
	if *maxLines != 0 {
		var n int
		n, err = writeCommitLines(node, *maxLines, lineno, os.Stdout)
		lineno += n
	}
	if !node.lastCommit {
		fmt.Printf("%s\n", genGraph(node, lineno))
	}
	return
}

// Generates ascii graph chars to display on the left side of the commit info if -graph arg is true.
func genGraph(node LogNode, lineno int) string {
	if !*showGraph {
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

type maxLineWriter struct {
	numLines int
	maxLines int
	node     LogNode
	dest     io.Writer
	first    bool
}

func (w *maxLineWriter) Write(data []byte) (n int, err error) {
	doGraph := func() error {
		var err error
		w.numLines++
		if *showGraph {
			s := genGraph(w.node, w.numLines)
			_, err = w.dest.Write([]byte(s))
		}
		return err
	}

	if w.first && len(data) > 0 {
		w.first = false
		err = doGraph()
		if err != nil {
			return
		}
	}

	for _, b := range data {
		n++
		if w.numLines == w.maxLines {
			err = maxLinesError
			return
		}
		// TODO: This is not technically correct due to utf-8, but ... meh.
		newLine := b == byte('\n')
		_, err = w.dest.Write(data[n-1 : n])
		if err != nil {
			return
		}
		if newLine {
			err = doGraph()
		}
		if err != nil {
			return
		}
	}
	return
}

func writeCommitLines(node LogNode, maxLines, lineno int, w io.Writer) (int, error) {
	out := &maxLineWriter{numLines: lineno, maxLines: maxLines, node: node, dest: w, first: true}
	err := types.WriteEncodedValueWithTags(out, node.commit.Get(datas.ValueField))
	if err == maxLinesError {
		fmt.Fprint(w, "...")
		out.numLines++
		err = nil
	}
	fmt.Fprintln(w)
	return out.numLines, err
}

func shouldUseColor() bool {
	if *color != 1 && *color != 0 {
		return outputpager.IsStdoutTty()
	}
	return *color == 1
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
