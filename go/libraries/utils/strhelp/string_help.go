package strhelp

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
