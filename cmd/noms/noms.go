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
	fmt.Fprintf(os.Stderr, "  %s\n", strings.Join(listCmds(), "\n  "))
	fmt.Fprintf(os.Stderr, "\nSee noms <command> -h for information on each available command.\n\n")
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() == 0 || flag.Arg(0) == "help" {
		usage()
		os.Exit(1)
	}

	cmd := findCmd(flag.Arg(0))
	if cmd == "" {
		fmt.Fprintf(os.Stderr, "error: %s is not an available command\n", flag.Arg(0))
		usage()
		os.Exit(1)
	}

	executeCmd(cmd)
}

func findCmd(name string) (cmd string) {
	nomsName := cmdPrefix + name
	forEachDir(func(dir *os.File) (stop bool) {
		if isNomsExecutable(dir, nomsName) {
			cmd = path.Join(dir.Name(), nomsName)
			stop = true
		}
		return
	})
	return
}

func listCmds() []string {
	cmds := []string{}

	forEachDir(func(dir *os.File) (stop bool) {
		// dir.Readdirnames may return an error, but |names| may still contain valid files.
		names, _ := dir.Readdirnames(0)
		for _, n := range names {
			if isNomsExecutable(dir, n) {
				cmds = append(cmds, n[len(cmdPrefix):])
			}
		}
		return
	})

	sort.Strings(cmds)
	return cmds
}

func forEachDir(cb func(dir *os.File) bool) {
	lookups := []struct {
		Env    string
		Suffix string
	}{
		{"PATH", ""},
		{"GOPATH", "bin"},
	}

	seen := map[string]bool{}

	for _, lookup := range lookups {
		env := os.Getenv(lookup.Env)
		if env == "" {
			continue
		}

		paths := strings.Split(env, string(os.PathListSeparator))
		for _, p := range paths {
			p := path.Join(p, lookup.Suffix)

			if seen[p] {
				continue
			}

			seen[p] = true

			if dir, err := os.Open(p); err == nil && cb(dir) {
				return
			}
		}
	}
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

func isNomsExecutable(dir *os.File, name string) bool {
	if !strings.HasPrefix(name, cmdPrefix) || len(name) == len(cmdPrefix) {
		return false
	}

	fi, err := os.Stat(path.Join(dir.Name(), name))
	return err == nil && !fi.IsDir() && fi.Mode()&0111 != 0
}
