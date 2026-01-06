// Copyright 2025 Dolthub, Inc.
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

package servercfg

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
)

type envPlaceholder struct {
	varName      string
	closingBrace int
}

// interpolateEnv expands environment variable placeholders in |data|.
//
// Supported syntax:
// - ${VAR}            : expands to VAR's value; error if VAR is unset or empty
// - $$               : escapes to a literal '$'
//
// Notes:
// - Expansion is applied to the input text only (env var values are inserted literally).
func interpolateEnv(data []byte) ([]byte, error) {
	out := make([]byte, 0, len(data))
	for i := 0; i < len(data); {
		relDollar := bytes.IndexByte(data[i:], '$')
		if relDollar == -1 {
			out = append(out, data[i:]...)
			break
		}

		dollarIdx := i + relDollar
		out = append(out, data[i:dollarIdx]...)

		// Escape sequence: $$ -> $
		if dollarIdx+1 < len(data) && data[dollarIdx+1] == '$' {
			out = append(out, '$')
			i = dollarIdx + 2
			continue
		}

		// Placeholder: ${...}
		if dollarIdx+1 < len(data) && data[dollarIdx+1] == '{' {
			ph, err := parseEnvPlaceholder(data, dollarIdx)
			if err != nil {
				return nil, err
			}

			out, err = appendEnvExpansion(out, ph)
			if err != nil {
				return nil, err
			}

			// Continue after the closing brace.
			i = ph.closingBrace + 1
			continue
		}

		// Leave stray '$' untouched
		out = append(out, '$')
		i = dollarIdx + 1
	}

	return out, nil
}

func parseEnvPlaceholder(data []byte, dollarIdx int) (envPlaceholder, error) {
	// data[dollarIdx] == '$' and data[dollarIdx+1] == '{' expected.
	start := dollarIdx + 2 // after ${

	rest := data[start:]

	relBrace := bytes.IndexByte(rest, '}')
	relNL := bytes.IndexByte(rest, '\n')
	relCR := bytes.IndexByte(rest, '\r')

	relLineEnd := minPositive(relNL, relCR)
	if relLineEnd != -1 && (relBrace == -1 || relLineEnd < relBrace) {
		// Placeholders must be on a single line. This prevents bad/missing braces from
		// consuming the rest of the file and producing giant error messages.
		return envPlaceholder{}, envErrorAt(data, dollarIdx, "unterminated environment placeholder")
	}

	if relBrace == -1 {
		return envPlaceholder{}, envErrorAt(data, dollarIdx, "unterminated environment placeholder")
	}

	closingBrace := start + relBrace

	expr := data[start:closingBrace]
	varName, err := parseEnvExpr(expr)
	if err != nil {
		return envPlaceholder{}, envErrorAt(data, dollarIdx, err.Error())
	}

	return envPlaceholder{
		varName:      varName,
		closingBrace: closingBrace,
	}, nil
}

func minPositive(a, b int) int {
	if a == -1 {
		return b
	}
	if b == -1 {
		return a
	}
	if a < b {
		return a
	}
	return b
}

func appendEnvExpansion(out []byte, ph envPlaceholder) ([]byte, error) {
	val, ok := os.LookupEnv(ph.varName)
	if !ok || val == "" {
		return nil, fmt.Errorf("environment variable %q is not set or empty", ph.varName)
	}
	return append(out, []byte(val)...), nil
}

func parseEnvExpr(expr []byte) (varName string, err error) {
	// Default expressions are intentionally unsupported to avoid silently masking misconfiguration.
	if bytes.Contains(expr, []byte(":-")) {
		return "", fmt.Errorf("environment variable default expressions are not supported (found %q)", string(expr))
	}

	if len(expr) == 0 {
		return "", fmt.Errorf("invalid environment placeholder: empty variable name")
	}
	if !isValidEnvVarName(expr) {
		return "", fmt.Errorf("invalid environment variable name %q", string(expr))
	}

	return string(expr), nil
}

var envVarRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func isValidEnvVarName(b []byte) bool {
	return envVarRe.Match(b)
}

func envErrorAt(data []byte, idx int, msg string) error {
	line, col := lineAndColAt(data, idx)
	return fmt.Errorf("%s (line %d, column %d)", msg, line, col)
}

func lineAndColAt(data []byte, idx int) (line int, col int) {
	if idx < 0 {
		idx = 0
	}
	if idx > len(data) {
		idx = len(data)
	}

	line = 1
	col = 1
	for i := 0; i < idx; i++ {
		switch data[i] {
		case '\n':
			line++
			col = 1
		case '\r':
			line++
			col = 1
			// Handle CRLF as a single newline.
			if i+1 < idx && data[i+1] == '\n' {
				i++
			}
		default:
			col++
		}
	}
	return line, col
}
