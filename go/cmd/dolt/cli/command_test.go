// Copyright 2019 Liquidata, Inc.
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
	"reflect"
	"strings"
	"testing"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
)

const (
	appName = "app"
)

type trackedCommandFunc struct {
	called bool
	cmdStr string
	args   []string
}

func (tf *trackedCommandFunc) wasCalled() bool {
	return tf.called
}

func (tf *trackedCommandFunc) commandFunc(ctx context.Context, cmdStr string, args []string, dEnv *env.DoltEnv) int {
	tf.called = true
	tf.cmdStr = cmdStr
	tf.args = args
	return 0
}

func (tf *trackedCommandFunc) equalsState(called bool, cmdStr string, args []string) bool {
	return called == tf.called && cmdStr == tf.cmdStr && reflect.DeepEqual(args, tf.args)
}

func TestCommands(t *testing.T) {
	child1 := &trackedCommandFunc{}
	grandChild1 := &trackedCommandFunc{}
	commands := &Command{Name: appName, Desc: "test application", Func: GenSubCommandHandler([]*Command{
		{Name: "child1", Desc: "first child command", Func: child1.commandFunc},
		{Name: "child2", Desc: "second child command", Func: GenSubCommandHandler([]*Command{
			{Name: "grandchild1", Desc: "child2's first child", Func: grandChild1.commandFunc},
		})},
	})}

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

func runCommand(root *Command, commandLine string) int {
	tokens := strings.Split(commandLine, " ")

	if tokens[0] != appName {
		panic("Invalid test commandh line")
	}

	return root.Func(context.Background(), appName, tokens[1:], nil)
}
