package strhelp

// NthToken returns the Nth token in s, delimited by delim. There is always at least one token: the zeroth token is the
// input string if delim doesn't occur in s. The second return value will be false if there is no Nth token.
func NthToken(s string, delim rune, n int) (string, bool) {
	if n < 0 {
		panic("invalid arguments.")
	}

	prev := 0
	curr := 0
	for ; curr < len(s); curr++ {
		if s[curr] == uint8(delim) {
			n--

			if n >= 0 {
				prev = curr + 1
			} else {
				break
			}
		}
	}

	if n <= 0 && prev <= curr {
		return s[prev:curr], true
	}

	return "", false
}
