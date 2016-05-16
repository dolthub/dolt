package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/util"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
	goisatty "github.com/mattn/go-isatty"
	"github.com/mgutz/ansi"
)

var (
	color       = flag.Int("color", -1, "value of 1 forces color on, 2 forces color off")
	maxLines    = flag.Int("max-lines", 10, "max number of lines to show per commit (-1 for all lines)")
	showHelp    = flag.Bool("help", false, "show help text")
	showGraph   = flag.Bool("graph", false, "show ascii-based commit hierarcy on left side of output")
	stdoutIsTty = flag.Int("stdout-is-tty", -1, "value of 1 forces tty ouput, 0 forces non-tty output (provided for use by other programs)")
	useColor    = false
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Displays the history of a Noms dataset\n")
		fmt.Fprintln(os.Stderr, "Usage: noms log <dataset>")
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

	spec, err := flags.ParseDatasetSpec(flag.Arg(0))
	util.CheckError(err)
	dataset, err := spec.Dataset()
	util.CheckError(err)

	origCommit, ok := dataset.MaybeHead()

	if ok {
		iter := NewCommitIterator(dataset.Store(), origCommit)
		for ln, ok := iter.Next(); ok; ln, ok = iter.Next() {
			printCommit(ln)
		}
	}

	dataset.Store().Close()
}

// Prints the information for one commit in the log, including ascii graph on left side of commits if
// -graph arg is true.
func printCommit(node LogNode) {
	lineno := 0
	doColor := func(s string) string { return s }
	if useColor {
		doColor = ansi.ColorFunc("red+h")
	}

	fmt.Printf("%s%s\n", genGraph(node, lineno), doColor(node.commit.Ref().String()))
	parents := commitRefsFromSet(node.commit.Get(datas.ParentsField).(types.Set))
	lineno++
	if len(parents) > 1 {
		pstrings := []string{}
		for _, cr := range parents {
			pstrings = append(pstrings, cr.TargetRef().String())
		}
		fmt.Printf("%sMerge: %s\n", genGraph(node, lineno), strings.Join(pstrings, " "))
	} else if len(parents) == 1 {
		fmt.Printf("%sParent: %s\n", genGraph(node, lineno), parents[0].TargetRef().String())
	} else {
		fmt.Printf("%sParent: None\n", genGraph(node, lineno))
	}
	if *maxLines != 0 {
		lineno += writeCommitLines(node, *maxLines, lineno, os.Stdout)
	}
	if !node.lastCommit {
		fmt.Printf("%s\n", genGraph(node, lineno))
	}
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
			err = io.EOF
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

func writeCommitLines(node LogNode, maxLines, lineno int, w io.Writer) int {
	// TODO(aa): Teach WriteEncodedValueWithTags how to deal with EOF
	doWrite := func() int {
		out := &maxLineWriter{numLines: lineno, maxLines: maxLines, node: node, dest: w, first: true}
		defer func() {
			if r := recover(); r != nil {
				// This is hacky but for some reason e is not io.EOF here, but some weird long string.
				if e, ok := r.(string); !ok || !strings.Contains(e, "EOF") {
					panic(r)
				} else {
					out.dest.Write([]byte("..."))
					out.numLines++
				}
			}
			out.dest.Write([]byte("\n"))
			return
		}()
		types.WriteEncodedValueWithTags(out, node.commit.Get(datas.ValueField))
		return out.numLines
	}
	return doWrite()
}

func isStdoutTty() bool {
	if *stdoutIsTty != 1 && *stdoutIsTty != 0 {
		return goisatty.IsTerminal(os.Stdout.Fd())
	}
	return *stdoutIsTty == 1
}

func shouldUseColor() bool {
	if *color != 1 && *color != 0 {
		return isStdoutTty()
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
