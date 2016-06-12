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

	"github.com/attic-labs/noms/cmd/noms-diff/diff"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/outputpager"
	"github.com/attic-labs/noms/samples/go/util"
	"github.com/mgutz/ansi"
)

var (
	color, maxLines, maxCommits    *int
	showHelp, showGraph, showValue *bool
	useColor                       = false
)

func main() {
	color = flag.Int("color", -1, "value of 1 forces color on, 2 forces color off")
	maxLines = flag.Int("max-lines", 10, "max number of lines to show per commit (-1 for all lines)")
	maxCommits = flag.Int("n", 0, "max number of commits to display (0 for all commits)")
	showHelp = flag.Bool("help", false, "show help text")
	showGraph = flag.Bool("graph", false, "show ascii-based commit hierarcy on left side of output")
	showValue = flag.Bool("show-value", false, "show commit value rather than diff information -- this is temporary")

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

	spec, err := spec.ParsePathSpec(flag.Arg(0))
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
		if printCommit(ln, database) != nil {
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
func printCommit(node LogNode, db datas.Database) (err error) {
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
		if *showValue {
			n, err = writeCommitLines(node, *maxLines, lineno, os.Stdout)
		} else {
			n, err = writeDiffLines(node, db, *maxLines, lineno, os.Stdout)
		}
		lineno += n
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

func writeCommitLines(node LogNode, maxLines, lineno int, w io.Writer) (lineCnt int, err error) {
	mlw := &maxLineWriter{numLines: lineno, maxLines: maxLines, node: node, dest: w, needsPrefix: true}
	err = types.WriteEncodedValueWithTags(mlw, node.commit.Get(datas.ValueField))
	if err != nil {
		mlw.forceWrite([]byte("..."))
		mlw.numLines++
		err = nil
	}
	mlw.forceWrite([]byte("\n"))
	if !node.lastCommit {
		mlw.forceWrite([]byte("\n"))
	}
	return mlw.numLines, err
}

func writeDiffLines(node LogNode, db datas.Database, maxLines, lineno int, w io.Writer) (lineCnt int, err error) {
	mlw := &maxLineWriter{numLines: lineno, maxLines: maxLines, node: node, dest: w, needsPrefix: true}
	parents := node.commit.Get(datas.ParentsField).(types.Set)
	var parent types.Value = nil
	if parents.Len() > 0 {
		parent = parents.First()
	}
	if parent == nil {
		_, err = fmt.Fprint(mlw, "\n")
		return 1, err
	}

	parentCommit := parent.(types.Ref).TargetValue(db).(types.Struct)
	err = diff.Diff(mlw, parentCommit.Get(datas.ValueField), node.commit.Get(datas.ValueField))
	if err != nil {
		mlw.forceWrite([]byte("...\n"))
		mlw.numLines++
		err = nil
	}
	if !node.lastCommit {
		mlw.forceWrite([]byte("\n"))
	}
	return mlw.numLines, err
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
