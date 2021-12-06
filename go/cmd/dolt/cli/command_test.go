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

package cli

import (
	"context"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const (
	appName = "app"
)

type trackedCommand struct {
	name        string
	description string
	called      bool
	cmdStr      string
	args        []string
}

var _ Command = (*trackedCommand)(nil)

func (cmd *trackedCommand) ArgParser() *argparser.ArgParser {
	return nil
}

func NewTrackedCommand(name, desc string) *trackedCommand {
	return &trackedCommand{name, desc, false, "", nil}
}

func (cmd *trackedCommand) wasCalled() bool {
	return cmd.called
}

func (cmd *trackedCommand) Name() string {
	return cmd.name
}

func (cmd *trackedCommand) Description() string {
	return cmd.description
}

func (cmd *trackedCommand) CreateMarkdown(wr io.Writer, commandStr string) error {
	return nil
}

func (cmd *trackedCommand) RequiresRepo() bool {
	return false
}

func (cmd *trackedCommand) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	cmd.called = true
	cmd.cmdStr = commandStr
	cmd.args = args
	return 0
}

func (cmd *trackedCommand) equalsState(called bool, cmdStr string, args []string) bool {
	return called == cmd.called && cmdStr == cmd.cmdStr && reflect.DeepEqual(args, cmd.args)
}

func TestCommands(t *testing.T) {
	grandChild1 := NewTrackedCommand("grandchild1", "child2's first child")
	child2 := NewSubCommandHandler("child2", "second child command", []Command{grandChild1})
	child1 := NewTrackedCommand("child1", "first child command")
	commands := NewSubCommandHandler(appName, "test application", []Command{
		child1,
		child2,
	})

	res := runCommand(commands, "app")

	if res == 0 {
		t.Error("bad return should be non-zero")
	}

	res = runCommand(commands, "app invalid")

	if res == 0 {
		t.Error("bad return. should be non-zero")
	}

	if !child1.equalsState(false, "", nil) || !grandChild1.equalsState(false, "", nil) {
		t.Fatal("Bad initial state")
	}

	res = runCommand(commands, "app child1 -flag -param=value arg0 arg1")

	if !child1.equalsState(true, "app child1", []string{"-flag", "-param=value", "arg0", "arg1"}) ||
		!grandChild1.equalsState(false, "", nil) {
		t.Fatal("Bad state after running child1")
	}

	res = runCommand(commands, "app child2 -flag -param=value arg0 arg1")

	if !child1.equalsState(true, "app child1", []string{"-flag", "-param=value", "arg0", "arg1"}) ||
		!grandChild1.equalsState(false, "", nil) {
		t.Fatal("Bad state before running grandChild1")
	}

	res = runCommand(commands, "app child2 grandchild1 -flag -param=value arg0 arg1")

	if !child1.equalsState(true, "app child1", []string{"-flag", "-param=value", "arg0", "arg1"}) ||
		!grandChild1.equalsState(true, "app child2 grandchild1", []string{"-flag", "-param=value", "arg0", "arg1"}) {
		t.Fatal("Bad state after running grandchild1")
	}
}

func runCommand(root Command, commandLine string) int {
	tokens := strings.Split(commandLine, " ")

	if tokens[0] != appName {
		panic("Invalid test command line")
	}

	return root.Exec(context.Background(), appName, tokens[1:], nil)
}

func TestHasHelpFlag(t *testing.T) {
	assert.False(t, hasHelpFlag([]string{}))
	assert.False(t, hasHelpFlag([]string{"help"}))
	assert.True(t, hasHelpFlag([]string{"--help"}))
	assert.True(t, hasHelpFlag([]string{"-h"}))
	assert.False(t, hasHelpFlag([]string{"--param", "value", "--flag", "help", "arg2", "arg3"}))
	assert.True(t, hasHelpFlag([]string{"--param", "value", "-f", "--help", "arg1", "arg2"}))
	assert.True(t, hasHelpFlag([]string{"--param", "value", "--flag", "-h", "arg1", "arg2"}))
}
