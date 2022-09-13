// Copyright 2022 Dolthub, Inc.
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
