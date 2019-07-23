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

package main

import (
	"github.com/gizak/termui/v3"
	"github.com/nsf/termbox-go"
)

type Input struct {
	prompt     string
	initialVal string
	cursor     int
	prevLen    int

	Value string
}

func NewInput(prompt, initialVal string, append bool) *Input {
	var cursor = 0
	if append {
		cursor = len(initialVal)
	}

	return &Input{prompt, initialVal, cursor, len(initialVal), initialVal}
}

func (in *Input) KBInputEvent(e termui.Event) {
	if e.ID == "<Delete>" {
		if in.cursor < len(in.Value) {
			in.Value = in.Value[:in.cursor] + in.Value[in.cursor+1:]
		}
	} else if e.ID == "<Backspace>" {
		if in.cursor > 0 {
			in.Value = in.Value[:in.cursor-1] + in.Value[in.cursor:]
			in.cursor--
		}
	} else if e.ID == "<Right>" {
		if in.cursor < len(in.Value) {
			in.cursor++
		}
	} else if e.ID == "<Left>" {
		if in.cursor > 0 {
			in.cursor--
		}
	} else {
		newVal := e.ID

		switch newVal {
		case "<Space>":
			newVal = " "
		case "<Tab>":
			newVal = "\t"
		}

		if len(newVal) == 1 {
			if in.cursor == 0 {
				in.Value = newVal + in.Value
			} else if in.cursor == len(in.Value) {
				in.Value = in.Value + newVal
			} else {
				in.Value = in.Value[:in.cursor] + newVal + in.Value[in.cursor:]
			}

			in.cursor += len(newVal)
		}
	}
	in.Render()
}

func (in *Input) Clear() {
	_, height := termbox.Size()
	lineNum := height - 1

	for x := 0; x < in.prevLen+1; x++ {
		termbox.SetCell(x, lineNum, ' ', termbox.ColorWhite, termbox.ColorBlack)
	}

	termbox.Flush()
}

func (in *Input) Render() {
	_, height := termbox.Size()
	lineNum := height - 1

	absCursor := len(in.prompt) + in.cursor
	fullText := in.prompt + in.Value

	x := 0
	for _, ch := range fullText {
		if x == absCursor {
			termbox.SetCell(x, lineNum, ch, termbox.ColorBlack, termbox.ColorWhite)
		} else {
			termbox.SetCell(x, lineNum, ch, termbox.ColorWhite, termbox.ColorBlack)
		}
		x++
	}

	prevLen := in.prevLen + 1
	in.prevLen = x
	for x < prevLen {
		if x == absCursor {
			termbox.SetCell(x, lineNum, ' ', termbox.ColorBlack, termbox.ColorWhite)
		} else {
			termbox.SetCell(x, lineNum, ' ', termbox.ColorWhite, termbox.ColorBlack)
		}
		x++
	}

	if x == absCursor {
		termbox.SetCell(x, lineNum, ' ', termbox.ColorBlack, termbox.ColorWhite)
	}

	termbox.Flush()
}
