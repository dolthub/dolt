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

package editor

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"unicode"

	"github.com/google/uuid"
)

// OpenTempEditor allows user to write/edit message in temporary file
func OpenTempEditor(ed string, initialContents string, fileSuffix string) (string, error) {
	fileName := uuid.New().String() + fileSuffix
	filename := filepath.Join(os.TempDir(), fileName)
	err := os.WriteFile(filename, []byte(initialContents), os.ModePerm)

	if err != nil {
		return "", err
	}

	cmdName, cmdArgs := getCmdNameAndArgsForEditor(ed)

	if cmdName == "" {
		panic("No editor specified: " + ed)
	}

	cmdArgs = append(cmdArgs, filename)

	cmd := exec.Command(cmdName, cmdArgs...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Start()
	if err != nil {
		return "", err
	}
	fmt.Printf("Waiting for command to finish.\n")
	err = cmd.Wait()
	if err != nil {
		return "", err
	}

	data, err := os.ReadFile(filename)

	if err != nil {
		return "", err
	}

	return string(data), nil
}

func getCmdNameAndArgsForEditor(es string) (string, []string) {
	type span struct {
		start int
		end   int
	}
	spans := make([]span, 0, 32)

	lastQuote := rune(0)
	f := func(c rune) bool {
		switch {
		case c == lastQuote:
			lastQuote = rune(0)
			return true
		case lastQuote != rune(0):
			return false
		case unicode.In(c, unicode.Quotation_Mark):
			lastQuote = c
			return false
		default:
			return unicode.IsSpace(c)

		}
	}

	hasStarted := false
	start := 0
	for i, rune := range es {
		if f(rune) {
			if hasStarted {
				if unicode.In(rune, unicode.Quotation_Mark) {
					spans = append(spans, span{start: start + 1, end: i})
				} else {
					spans = append(spans, span{start: start, end: i})
				}

				hasStarted = false
			}
		} else {
			if !hasStarted {
				start = i
				hasStarted = true
			}
		}
	}

	if hasStarted {
		spans = append(spans, span{start, len(es)})
	}

	results := make([]string, len(spans))
	for i, span := range spans {
		results[i] = es[span.start:span.end]
	}

	return results[0], results[1:]
}
