// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package outputpager

import (
	"io"
	"os"
	"os/exec"
	"sync"

	flag "github.com/juju/gnuflag"
	"github.com/liquidata-inc/ld/dolt/go/store/go/d"
	goisatty "github.com/mattn/go-isatty"
)

var (
	noPager bool
)

type Pager struct {
	Writer        io.Writer
	stdin, stdout *os.File
	mtx           *sync.Mutex
	doneCh        chan struct{}
}

func Start() *Pager {
	if noPager || !IsStdoutTty() {
		return &Pager{os.Stdout, nil, nil, nil, nil}
	}

	lessPath, err := exec.LookPath("less")
	d.Chk.NoError(err)

	// -F ... Quit if entire file fits on first screen.
	// -S ... Chop (truncate) long lines rather than wrapping.
	// -R ... Output "raw" control characters.
	// -X ... Don't use termcap init/deinit strings.
	cmd := exec.Command(lessPath, "-FSRX")

	stdin, stdout, err := os.Pipe()
	d.Chk.NoError(err)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = stdin
	cmd.Start()

	p := &Pager{stdout, stdin, stdout, &sync.Mutex{}, make(chan struct{})}
	go func() {
		err := cmd.Wait()
		d.Chk.NoError(err)
		p.closePipe()
		p.doneCh <- struct{}{}
	}()
	return p
}

func (p *Pager) Stop() {
	if p.Writer != os.Stdout {
		p.closePipe()
		// Wait until less has fully exited, otherwise it might not have printed the terminal restore characters.
		<-p.doneCh
	}
}

func (p *Pager) closePipe() {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if p.stdin != nil {
		// Closing the pipe will cause any outstanding writes to stdout fail, and fail from now on.
		p.stdin.Close()
		p.stdout.Close()
		p.stdin, p.stdout = nil, nil
	}
}

func RegisterOutputpagerFlags(flags *flag.FlagSet) {
	flags.BoolVar(&noPager, "no-pager", false, "suppress paging functionality")
}

func IsStdoutTty() bool {
	return goisatty.IsTerminal(os.Stdout.Fd())
}
