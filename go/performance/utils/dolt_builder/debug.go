// Copyright 2019-2022 Dolthub, Inc.
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

package dolt_builder

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

var Debug bool

const envDebug = "DEBUG"

const maxCapturedOutputBytes = 64 * 1024

func init() {
	if os.Getenv(envDebug) != "" {
		Debug = true
	}
}

func ExecCommand(ctx context.Context, name string, arg ...string) *exec.Cmd {
	e := exec.CommandContext(ctx, name, arg...)
	if Debug {
		e.Stdout = os.Stdout
		e.Stderr = os.Stderr
	}
	return e
}

// RunCommand runs cmd.
//
// When DEBUG is not set, stdout+stderr are captured and attached to any error so
// failures have actionable output. When DEBUG is set, output streams directly to
// the console.
func RunCommand(cmd *exec.Cmd) error {
	_, err := RunCommandOutput(cmd)
	return err
}

// RunCommandOutput runs cmd and returns combined stdout+stderr output when DEBUG
// is not set. When DEBUG is set, output streams directly and returned output is
// nil.
func RunCommandOutput(cmd *exec.Cmd) ([]byte, error) {
	if Debug {
		return nil, cmd.Run()
	}

	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	out := buf.Bytes()

	if err != nil {
		return out, fmt.Errorf("%w\ncommand: %s\ndir: %s\noutput:\n%s", err, cmd.String(), cmd.Dir, formatOutput(out))
	}

	return out, nil
}

func formatOutput(out []byte) string {
	if len(out) == 0 {
		return "(no output)"
	}
	if len(out) <= maxCapturedOutputBytes {
		return strings.TrimRight(string(out), "\n")
	}
	trimmed := out[len(out)-maxCapturedOutputBytes:]
	return fmt.Sprintf("... (truncated; showing last %d bytes)\n%s", maxCapturedOutputBytes, strings.TrimRight(string(trimmed), "\n"))
}
