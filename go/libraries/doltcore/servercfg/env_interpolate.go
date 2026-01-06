package servercfg

import (
	"fmt"
	"os"
)

// envLookupFunc returns (value, true) if the env var exists.
// Note that an env var may exist but be empty.
type envLookupFunc func(string) (string, bool)

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
func interpolateEnv(data []byte, lookup envLookupFunc) ([]byte, error) {
	if lookup == nil {
		lookup = os.LookupEnv
	}

	out := make([]byte, 0, len(data))
	for i := 0; i < len(data); i++ {
		if data[i] != '$' {
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

		// Find closing brace.
		start := i + 2 // after ${
		j := start
		for ; j < len(data) && data[j] != '}'; j++ {
		}
		if j >= len(data) {
			return nil, fmt.Errorf("unterminated environment placeholder starting at byte %d", i)
		}

		expr := data[start:j]
		varName, def, hasDefault, err := parseEnvExpr(expr)
		if err != nil {
			return nil, err
		}

		val, ok := lookup(varName)
		if ok && val != "" {
			out = append(out, []byte(val)...)
		} else if hasDefault {
			expandedDef, err := interpolateEnv(def, lookup)
			if err != nil {
				return nil, err
			}
			out = append(out, expandedDef...)
		} else {
			return nil, fmt.Errorf("environment variable %q is not set", varName)
		}

		// Skip to closing brace.
		i = j
	}

	return out, nil
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

