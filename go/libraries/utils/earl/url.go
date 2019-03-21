package earl

import (
	"net/url"
	"regexp"
	"strings"
)

var validHostRegex = regexp.MustCompile("^[-.a-zA-z0-9]*$")
var validHostWithPortRegex = regexp.MustCompile("^[-.a-zA-z0-9]*:[0-9]*$")

func isValidHost(hostStr string) bool {
	if hostStr == "" {
		return false
	} else if hostStr == "localhost" {
		return true
	} else if strings.Index(hostStr, ".") == -1 {
		return false
	}

	return validHostRegex.MatchString(hostStr) || validHostWithPortRegex.MatchString(hostStr)
}

func Parse(urlStr string) (*url.URL, error) {
	u, err := parse(urlStr)

	if err != nil {
		return nil, err
	}

	// noramalize some things
	if len(u.Path) == 0 || u.Path[0] != '/' {
		u.Path = "/" + u.Path
	}

	return u, nil
}

func parse(urlStr string) (*url.URL, error) {
	if strings.Index(urlStr, "://") == -1 {
		u, err := url.Parse("http://" + urlStr)

		if err == nil && isValidHost(u.Host) {
			u.Scheme = ""
			return u, nil
		} else if err != nil {
			return nil, err
		}
	}

	return url.Parse(urlStr)
}
