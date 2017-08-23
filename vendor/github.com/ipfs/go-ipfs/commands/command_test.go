package commands

import "testing"

func noop(req Request, res Response) {
}

func TestOptionValidation(t *testing.T) {
	cmd := Command{
		Options: []Option{
			IntOption("b", "beep", "enables beeper"),
			StringOption("B", "boop", "password for booper"),
		},
		Run: noop,
	}

	opts, _ := cmd.GetOptions(nil)

	req, _ := NewRequest(nil, nil, nil, nil, nil, opts)
	req.SetOption("beep", true)
	res := cmd.Call(req)
	if res.Error() == nil {
		t.Error("Should have failed (incorrect type)")
	}

	req, _ = NewRequest(nil, nil, nil, nil, nil, opts)
	req.SetOption("beep", 5)
	res = cmd.Call(req)
	if res.Error() != nil {
		t.Error(res.Error(), "Should have passed")
	}

	req, _ = NewRequest(nil, nil, nil, nil, nil, opts)
	req.SetOption("beep", 5)
	req.SetOption("boop", "test")
	res = cmd.Call(req)
	if res.Error() != nil {
		t.Error("Should have passed")
	}

	req, _ = NewRequest(nil, nil, nil, nil, nil, opts)
	req.SetOption("b", 5)
	req.SetOption("B", "test")
	res = cmd.Call(req)
	if res.Error() != nil {
		t.Error("Should have passed")
	}

	req, _ = NewRequest(nil, nil, nil, nil, nil, opts)
	req.SetOption("foo", 5)
	res = cmd.Call(req)
	if res.Error() != nil {
		t.Error("Should have passed")
	}

	req, _ = NewRequest(nil, nil, nil, nil, nil, opts)
	req.SetOption(EncShort, "json")
	res = cmd.Call(req)
	if res.Error() != nil {
		t.Error("Should have passed")
	}

	req, _ = NewRequest(nil, nil, nil, nil, nil, opts)
	req.SetOption("b", "100")
	res = cmd.Call(req)
	if res.Error() != nil {
		t.Error("Should have passed")
	}

	req, _ = NewRequest(nil, nil, nil, nil, &cmd, opts)
	req.SetOption("b", ":)")
	res = cmd.Call(req)
	if res.Error() == nil {
		t.Error("Should have failed (string value not convertible to int)")
	}

	err := req.SetOptions(map[string]interface{}{
		"b": 100,
	})
	if err != nil {
		t.Error("Should have passed")
	}

	err = req.SetOptions(map[string]interface{}{
		"b": ":)",
	})
	if err == nil {
		t.Error("Should have failed (string value not convertible to int)")
	}
}

func TestRegistration(t *testing.T) {
	cmdA := &Command{
		Options: []Option{
			IntOption("beep", "number of beeps"),
		},
		Run: noop,
	}

	cmdB := &Command{
		Options: []Option{
			IntOption("beep", "number of beeps"),
		},
		Run: noop,
		Subcommands: map[string]*Command{
			"a": cmdA,
		},
	}

	cmdC := &Command{
		Options: []Option{
			StringOption("encoding", "data encoding type"),
		},
		Run: noop,
	}

	path := []string{"a"}
	_, err := cmdB.GetOptions(path)
	if err == nil {
		t.Error("Should have failed (option name collision)")
	}

	_, err = cmdC.GetOptions(nil)
	if err == nil {
		t.Error("Should have failed (option name collision with global options)")
	}
}

func TestResolving(t *testing.T) {
	cmdC := &Command{}
	cmdB := &Command{
		Subcommands: map[string]*Command{
			"c": cmdC,
		},
	}
	cmdB2 := &Command{}
	cmdA := &Command{
		Subcommands: map[string]*Command{
			"b": cmdB,
			"B": cmdB2,
		},
	}
	cmd := &Command{
		Subcommands: map[string]*Command{
			"a": cmdA,
		},
	}

	cmds, err := cmd.Resolve([]string{"a", "b", "c"})
	if err != nil {
		t.Error(err)
	}
	if len(cmds) != 4 || cmds[0] != cmd || cmds[1] != cmdA || cmds[2] != cmdB || cmds[3] != cmdC {
		t.Error("Returned command path is different than expected", cmds)
	}
}

func TestWalking(t *testing.T) {
	cmdA := &Command{
		Subcommands: map[string]*Command{
			"b": &Command{},
			"B": &Command{},
		},
	}
	i := 0
	cmdA.Walk(func(c *Command) {
		i = i + 1
	})
	if i != 3 {
		t.Error("Command tree walk didn't work, expected 3 got:", i)
	}
}

func TestHelpProcessing(t *testing.T) {
	cmdB := &Command{
		Helptext: HelpText{
			ShortDescription: "This is other short",
		},
	}
	cmdA := &Command{
		Helptext: HelpText{
			ShortDescription: "This is short",
		},
		Subcommands: map[string]*Command{
			"a": cmdB,
		},
	}
	cmdA.ProcessHelp()
	if len(cmdA.Helptext.LongDescription) == 0 {
		t.Error("LongDescription was not set on basis of ShortDescription")
	}
	if len(cmdB.Helptext.LongDescription) == 0 {
		t.Error("LongDescription was not set on basis of ShortDescription")
	}
}
