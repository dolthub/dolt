package cli

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vbauerster/mpb/cwriter"
)

var (
	cursorUp           = fmt.Sprintf("%c[%dA", cwriter.ESC, 1)
	clearLine          = fmt.Sprintf("%c[2K\r", cwriter.ESC)
	clearCursorAndLine = cursorUp + clearLine
)

func TestEphemeralPrinter(t *testing.T) {
	t.Run("DisplayPutsCursorAtLineStart", func(t *testing.T) {
		old := CliOut
		defer func() {
			CliOut = old
		}()

		var b bytes.Buffer
		CliOut = &b
		p := NewEphemeralPrinter()
		p.Printf("Hi!")
		p.Display()

		assert.Equal(t, b.String(), "Hi!\n")

		p.Display()
		assert.Equal(t, "Hi!\n"+clearCursorAndLine, b.String())

		p.Printf("Newline!\n")
		p.Printf("And another one!")
		p.Printf("\nSomething else\n")
		p.Display()

		assert.Equal(t,
			"Hi!\n"+
				clearCursorAndLine+
				"Newline!\n"+
				"And another one!\n"+
				"Something else\n", b.String())

		p.Display()
		assert.Equal(t,
			"Hi!\n"+
				clearCursorAndLine+
				"Newline!\n"+
				"And another one!\n"+
				"Something else\n"+
				clearCursorAndLine+
				clearCursorAndLine+
				clearCursorAndLine, b.String())
	})
}
