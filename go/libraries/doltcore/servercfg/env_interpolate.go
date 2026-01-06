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
	"fmt"
	"os"
)

type envPlaceholder struct {
	varName      string
	def          []byte
	hasDefault   bool
	closingBrace int
}

// interpolateEnv expands environment variable placeholders in |data|.
//
// Supported syntax:
// - ${VAR}            : expands to VAR's value; error if unset or empty
// - ${VAR:-default}   : expands to VAR's value if set and non-empty; otherwise to default
// - $$               : escapes to a literal '$'
//
// Notes:
// - Expansion is applied to the input text only (env var values are inserted literally).
// - Default expressions are expanded (i.e. nested placeholders inside defaults are supported).
func interpolateEnv(data []byte) ([]byte, error) {
	out := make([]byte, 0, len(data))
	for i := 0; i < len(data); i++ {
		b := data[i]
		if b != '$' {
			out = append(out, data[i])
			continue
		}

		// Escape sequence: $$ -> $
		if i+1 < len(data) && data[i+1] == '$' {
			out = append(out, '$')
			i++
			continue
		}

		// Placeholder: ${...}
		if i+1 >= len(data) || data[i+1] != '{' {
			// Leave stray '$' untouched
			out = append(out, '$')
			continue
		}

		ph, err := parseEnvPlaceholder(data, i)
		if err != nil {
			return nil, err
		}

		out, err = appendEnvExpansion(out, ph)
		if err != nil {
			return nil, err
		}

		// Skip to closing brace.
		i = ph.closingBrace
	}

	return out, nil
}

func parseEnvPlaceholder(data []byte, dollarIdx int) (envPlaceholder, error) {
	// data[dollarIdx] == '$' and data[dollarIdx+1] == '{' expected.
	start := dollarIdx + 2 // after ${

	closingBrace := start
	for ; closingBrace < len(data) && data[closingBrace] != '}'; closingBrace++ {
	}
	if closingBrace >= len(data) {
		return envPlaceholder{}, fmt.Errorf("unterminated environment placeholder starting at byte %d", dollarIdx)
	}

	expr := data[start:closingBrace]
	varName, def, hasDefault, err := parseEnvExpr(expr)
	if err != nil {
		return envPlaceholder{}, err
	}

	return envPlaceholder{
		varName:      varName,
		def:          def,
		hasDefault:   hasDefault,
		closingBrace: closingBrace,
	}, nil
}

func appendEnvExpansion(out []byte, ph envPlaceholder) ([]byte, error) {
	val, ok := os.LookupEnv(ph.varName)
	if ok && val != "" {
		return append(out, []byte(val)...), nil
	}

	if ph.hasDefault {
		expandedDef, err := interpolateEnv(ph.def)
		if err != nil {
			return nil, err
		}
		return append(out, expandedDef...), nil
	}

	return nil, fmt.Errorf("environment variable %q is not set", ph.varName)
}

func parseEnvExpr(expr []byte) (varName string, def []byte, hasDefault bool, err error) {
	// Split on the first occurrence of ":-"
	varPart := expr
	var defPart []byte
	for k := 0; k+1 < len(expr); k++ {
		if expr[k] == ':' && expr[k+1] == '-' {
			varPart = expr[:k]
			defPart = expr[k+2:]
			hasDefault = true
			break
		}
	}

	if len(varPart) == 0 {
		return "", nil, false, fmt.Errorf("invalid environment placeholder: empty variable name")
	}
	if !isValidEnvVarName(varPart) {
		return "", nil, false, fmt.Errorf("invalid environment variable name %q", string(varPart))
	}

	return string(varPart), defPart, hasDefault, nil
}

func isValidEnvVarName(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	for i := range b {
		c := b[i]
		if i == 0 {
			if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_') {
				return false
			}
		} else {
			if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
				return false
			}
		}
	}
	return true
}
