package gorocheck

import (
	"bufio"
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

var pkgName string

func init() {
	t := reflect.TypeOf(Goroutine{})
	parts := strings.Split(t.PkgPath(), "/")

	pkgName = parts[len(parts)-1]
}

type Goroutine struct {
	Function string
	Number   int
	Action   string
	Stack    []string
}

func funcNameFromStackLine(s string) string {
	parts := strings.Split(s, "(")
	s = parts[0]
	parts = strings.Split(s, "/")
	return parts[len(parts)-1]
}

func parseStack() []*Goroutine {
	stkbuf := make([]byte, 1000000)
	n := runtime.Stack(stkbuf, true)
	stkbuf = stkbuf[:n]

	var out []*Goroutine
	var cur *Goroutine
	scan := bufio.NewScanner(bytes.NewReader(stkbuf))
	for scan.Scan() {
		if strings.HasPrefix(scan.Text(), "goroutine") {
			parts := strings.Split(scan.Text(), " ")

			var num int
			fmt.Sscanf(parts[1], "%d", &num)

			if !scan.Scan() {
				panic("bad format")
			}

			fname := funcNameFromStackLine(scan.Text())

			g := &Goroutine{
				Number:   num,
				Function: fname,
				Stack:    []string{scan.Text()},
			}
			out = append(out, g)
			cur = g
		} else if cur != nil {
			cur.Stack = append(cur.Stack, scan.Text())
		}
	}
	return out
}

func filterSystemRoutines(gs []*Goroutine) []*Goroutine {
	sys := map[string]struct{}{
		"testing.RunTests":      struct{}{},
		pkgName + ".parseStack": struct{}{},
		"signal.loop":           struct{}{},
		"signal.signal_recv":    struct{}{},
		"runtime.goexit":        struct{}{},
	}

	var out []*Goroutine
	for _, g := range gs {
		if _, found := sys[g.Function]; !found {
			out = append(out, g)
		}
	}
	return out
}

func CheckForLeaks(filter func(*Goroutine) bool) error {
	goros := filterSystemRoutines(parseStack())
	var gorosFiltered []*Goroutine
	if filter != nil {
		for _, g := range goros {
			if !filter(g) {
				gorosFiltered = append(gorosFiltered, g)
			}
		}
	}
	goros = gorosFiltered
	if len(goros) > 0 {
		return fmt.Errorf("had %d goroutines still running. First on list: %s", len(goros), goros[0].Stack)
	}
	return nil
}
