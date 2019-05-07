package ref

import "strings"

type pattern interface {
	matches(string) (string, bool)
}

type strPattern string

func (sp strPattern) matches(s string) (string, bool) {
	return "", s == string(sp)
}

type wcPattern struct {
	prefixStr string
	suffixStr string
}

func newWildcardPattern(s string) wcPattern {
	tokens := strings.Split(s, "*")

	if len(tokens) != 2 {
		panic("invalid localPattern")
	}

	return wcPattern{tokens[0], tokens[1]}
}

func (wp wcPattern) matches(s string) (string, bool) {
	if strings.HasPrefix(s, wp.prefixStr) {
		s = s[len(wp.prefixStr):]
		if strings.HasSuffix(s, wp.suffixStr) {
			return s[:len(s)-len(wp.suffixStr)], true
		}
	}

	return "", false
}
