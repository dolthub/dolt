package isdomain

import "regexp"

// DomainRegexpStr is a regular expression string to validate domains.
const DomainRegexpStr = "^([a-z0-9]+(-[a-z0-9]+)*\\.)+[a-z]{2,}$"

var domainRegexp *regexp.Regexp

func init() {
	domainRegexp = regexp.MustCompile(DomainRegexpStr)
}
