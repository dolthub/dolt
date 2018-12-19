package cli

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"reflect"
	"strings"
	"testing"
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

func (tf *trackedCommandFunc) commandFunc(cmdStr string, args []string, cliEnv *env.DoltCLIEnv) int {
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
	commands := &Command{appName, "test application", GenSubCommandHandler([]*Command{
		{"child1", "first child command", child1.commandFunc, false},
		{"child2", "second child command", GenSubCommandHandler([]*Command{
			{"grandchild1", "child2's first child", grandChild1.commandFunc, false},
		}), false},
	}), false}

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

	return root.Func(appName, tokens[1:], nil)
}
