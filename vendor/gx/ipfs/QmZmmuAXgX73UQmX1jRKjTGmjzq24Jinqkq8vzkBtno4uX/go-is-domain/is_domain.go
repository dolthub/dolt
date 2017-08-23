package isdomain

import "strings"

//go:generate bash regenerate-tlds.sh

// IsICANNTLD returns whether the given string is a TLD (Top Level Domain),
// according to ICANN. Well, really according to the TLDs listed in this
// package.
func IsICANNTLD(s string) bool {
	s = strings.ToUpper(s)
	_, found := TLDs[s]
	return found
}

// IsExtendedTLD returns whether the given string is a TLD (Top Level Domain),
// extended with a few other "TLDs": .bit, .onion
func IsExtendedTLD(s string) bool {
	s = strings.ToUpper(s)
	_, found := ExtendedTLDs[s]
	return found
}

// IsTLD returns whether the given string is a TLD (according to ICANN, or
// in the set of ExtendedTLDs listed in this package.
func IsTLD(s string) bool {
	return IsICANNTLD(s) || IsExtendedTLD(s)
}

// IsDomain returns whether given string is a domain.
// It first checks the TLD, and then uses a regular expression.
func IsDomain(s string) bool {
	if strings.HasSuffix(s, ".") {
		s = s[:len(s)-1]
	}

	split := strings.Split(s, ".")
	tld := split[len(split)-1]

	if !IsTLD(tld) {
		return false
	}

	s = strings.ToLower(s)
	return domainRegexp.MatchString(s)
}
