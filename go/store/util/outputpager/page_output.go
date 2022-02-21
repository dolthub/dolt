// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package outputpager

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"

	flag "github.com/juju/gnuflag"
	goisatty "github.com/mattn/go-isatty"

	"github.com/dolthub/dolt/go/store/d"
)

var (
	noPager bool
	testing = false
)

type Pager struct {
	Writer        io.Writer
	stdin, stdout *os.File
	mtx           *sync.Mutex
	doneCh        chan struct{}
}

func Start() *Pager {
	// `testing` is set to true only to test this function because when testing this function, stdout is not Terminal.
	// otherwise, it must be always false.
	if !testing {
		if noPager || !IsStdoutTty() {
			return &Pager{os.Stdout, nil, nil, nil, nil}
		}
	}

	var lessPath string
	var err error
	var cmd *exec.Cmd

	lessPath, err = exec.LookPath("less")
	if err != nil {
		lessPath, err = exec.LookPath("more")
		d.Chk.NoError(err)
		cmd = exec.Command(lessPath)
	} else {
		d.Chk.NoError(err)
		// -F ... Quit if entire file fits on first screen.
		// -S ... Chop (truncate) long lines rather than wrapping.
		// -R ... Output "raw" control characters.
		// -X ... Don't use termcap init/deinit strings.
		cmd = exec.Command(lessPath, "-FSRX")
	}

	stdin, stdout, err := os.Pipe()
	d.Chk.NoError(err)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = stdin
	cmd.Start()

	p := &Pager{stdout, stdin, stdout, &sync.Mutex{}, make(chan struct{})}

	interruptChannel := make(chan os.Signal, 1)
	signal.Notify(interruptChannel, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		for {
			select {
			case _, ok := <-interruptChannel:
				if ok {
					p.closePipe()
					p.doneCh <- struct{}{}
				}
			default:
			}
		}
	}()

	go func() {
		err := cmd.Wait()
		if err != nil {
			fmt.Printf("error occurred during exit: %s ", err)
		}
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

func SetTestingArg(s bool) {
	testing = s
}
