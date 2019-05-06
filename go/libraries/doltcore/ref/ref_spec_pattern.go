package ref

import "strings"

type PatternType int

type Pattern interface {
	Matches(string) (string, bool)
}

type StringPattern string

func (sp StringPattern) Matches(s string) (string, bool) {
	return "", s == string(sp)
}

type WildcardPattern struct {
	prefixStr string
	suffixStr string
}

func NewWildcardPattern(s string) WildcardPattern {
	tokens := strings.Split(s, "*")

	if len(tokens) != 2 {
		panic("invalid pattern")
	}

	return WildcardPattern{tokens[0], tokens[1]}
}

func (wp WildcardPattern) Matches(s string) (string, bool) {
	if strings.HasPrefix(s, wp.prefixStr) {
		s = s[len(wp.prefixStr):]
		if strings.HasSuffix(s, wp.suffixStr) {
			return s[:len(s)-len(wp.suffixStr)], true
		}
	}

	return "", false
}
