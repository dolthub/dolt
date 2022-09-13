package cli

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vbauerster/mpb/cwriter"
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
		assert.Equal(t, "Hi!\n"+clearLinesTxt(1), b.String())

		p.Printf("Newline!\n")
		p.Printf("And another one!")
		p.Printf("\nSomething else\n")
		p.Display()

		assert.Equal(t,
			"Hi!\n"+
				clearLinesTxt(1)+
				"Newline!\n"+
				"And another one!\n"+
				"Something else\n", b.String())

		p.Display()
		assert.Equal(t,
			"Hi!\n"+
				clearLinesTxt(1)+
				"Newline!\n"+
				"And another one!\n"+
				"Something else\n"+
				clearLinesTxt(3), b.String())
	})
}

func clearLinesTxt(n int) string {
	return fmt.Sprintf("%c[%dA", cwriter.ESC, n) + fmt.Sprintf("%c[J", cwriter.ESC)
}
