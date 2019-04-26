package main

import (
	"context"
	"github.com/gizak/termui/v3"
)

type ColonPromptCommand string

const (
	InvalidCommand      ColonPromptCommand = "invalid"
	NoCommand           ColonPromptCommand = "none"
	QuitCommand         ColonPromptCommand = "quit"
	QuitNoSaveCommand   ColonPromptCommand = "quit_no_save"
	WriteAndQuitCommand ColonPromptCommand = "write_and_quit"
)

var ColonPromptCommands = map[string]ColonPromptCommand{
	"":   NoCommand,
	"q":  QuitCommand,
	"q!": QuitNoSaveCommand,
	"e":  QuitCommand,
	"e!": QuitNoSaveCommand,
	"wq": WriteAndQuitCommand,
}

type ColonPrompt struct {
	in  *Input
	dim *Dim
}

func NewColonPrompt(dim *Dim) *ColonPrompt {
	in := NewInput(":", "", true)
	in.Render()

	return &ColonPrompt{in, dim}
}

func (cp *ColonPrompt) Result() ColonPromptCommand {
	val := cp.in.Value

	if command, ok := ColonPromptCommands[val]; ok {
		return command
	}

	return InvalidCommand
}

func (cp *ColonPrompt) InHandler(ctx context.Context, e termui.Event) (exit, render, releaseFocus bool) {
	if e.ID == "<Escape>" {
		cp.in.Value = ""
		cp.in.Clear()
		return false, true, true
	} else if e.ID == "<Enter>" {
		cp.in.Clear()

		res := cp.Result()

		switch res {
		case QuitCommand:
			return !cp.dim.HasEdits(), false, true
		case QuitNoSaveCommand:
			return true, false, true
		case WriteAndQuitCommand:
			cp.dim.FlushEdits(ctx)
			return true, false, true
		}

		return false, true, true
	} else {
		cp.in.KBInputEvent(e)
	}

	return false, false, false
}
