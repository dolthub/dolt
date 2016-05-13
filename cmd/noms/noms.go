package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"syscall"

	"github.com/attic-labs/noms/d"
	goisatty "github.com/mattn/go-isatty"
)

const (
	cmdPrefix = "noms-"
)

var (
	noPager = flag.Bool("no-pager", false, "suppress paging functionality")
)

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [-no-pager] [command] [command-args]\n\n", path.Base(os.Args[0]))
	fmt.Fprintf(os.Stderr, "Flags:\n\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nCommands:\n\n")
	for _, cmd := range findCmds() {
		fmt.Fprintf(os.Stderr, "  %s\n", cmd)
	}
	fmt.Fprintf(os.Stderr, "\nSee noms <command> -h for information on each available command.\n\n")
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() == 0 || flag.Arg(0) == "help" {
		usage()
		return
	}

	cmdName := cmdPrefix + flag.Arg(0)
	executable, err := exec.LookPath(cmdName)
	if err != nil {
		if execErr, ok := err.(*exec.Error); ok {
			d.Chk.Equal(exec.ErrNotFound, execErr.Err)
			fmt.Fprintf(os.Stderr, "error: %s is not an available command\n", flag.Arg(0))
		} else {
			d.Chk.NoError(err)
		}
		usage()
		return
	}

	executeCmd(executable)
}

func findCmds() []string {
	paths := strings.Split(os.Getenv("PATH"), string(os.PathListSeparator))
	cmds := []string{}
	prefixLen := len(cmdPrefix)
	for _, p := range paths {
		dir, err := os.Open(p)
		if err == nil {
			names, err := dir.Readdirnames(0)
			if err == nil {
				for _, n := range names {
					if strings.HasPrefix(n, cmdPrefix) && len(n) > prefixLen {
						fi, err := os.Stat(path.Join(p, n))
						d.Chk.NoError(err)
						if !fi.IsDir() && fi.Mode()&0111 != 0 {
							cmds = append(cmds, n[prefixLen:])
						}
					}
				}
			}
		}
	}
	sort.Strings(cmds)
	return cmds
}

func executeCmd(executable string) {
	lessCmd, err := exec.LookPath("less")
	if err != nil {
		*noPager = true
	}
	args := flag.Args()[1:]
	if len(args) == 0 {
		args = append(args, "-help")
	}
	if !*noPager {
		arg := "-stdout-is-tty=0"
		if goisatty.IsTerminal(os.Stdout.Fd()) {
			arg = "-stdout-is-tty=1"
		}
		args = append([]string{arg}, args...)
	}
	c1 := exec.Command(executable, args...)
	c1.Stdin = os.Stdin
	c1.Stdout = os.Stdout
	c1.Stderr = os.Stderr

	if !*noPager {
		c1.Stdout = nil
		c2 := exec.Command(lessCmd, []string{"-FSRX"}...)
		c2.Stdin, _ = c1.StdoutPipe()
		c2.Stdout = os.Stdout
		c2.Stderr = os.Stderr
		c2.Start()
		err = c1.Run()
		c2.Wait()
	} else {
		err = c1.Run()
	}

	if err != nil {
		switch t := err.(type) {
		case *exec.ExitError:
			status := t.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()
			os.Exit(status)
		default:
			fmt.Fprintf(os.Stderr, "error: %s\n", err)
			os.Exit(-1)
		}
	}
}
