package cli

import (
	"strings"
	"testing"

	cmds "github.com/ipfs/go-ipfs/commands"
)

func TestSynopsisGenerator(t *testing.T) {
	command := &cmds.Command{
		Arguments: []cmds.Argument{
			cmds.StringArg("required", true, false, ""),
			cmds.StringArg("variadic", false, true, ""),
		},
		Options: []cmds.Option{
			cmds.StringOption("opt", "o", "Option"),
		},
		Helptext: cmds.HelpText{
			SynopsisOptionsValues: map[string]string{
				"opt": "OPTION",
			},
		},
	}
	syn := generateSynopsis(command, "cmd")
	t.Logf("Synopsis is: %s", syn)
	if !strings.HasPrefix(syn, "cmd ") {
		t.Fatal("Synopsis should start with command name")
	}
	if !strings.Contains(syn, "[--opt=<OPTION> | -o]") {
		t.Fatal("Synopsis should contain option descriptor")
	}
	if !strings.Contains(syn, "<required>") {
		t.Fatal("Synopsis should contain required argument")
	}
	if !strings.Contains(syn, "<variadic>...") {
		t.Fatal("Synopsis should contain variadic argument")
	}
	if !strings.Contains(syn, "[<variadic>...]") {
		t.Fatal("Synopsis should contain optional argument")
	}
	if !strings.Contains(syn, "[--]") {
		t.Fatal("Synopsis should contain options finalizer")
	}
}
