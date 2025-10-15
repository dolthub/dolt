// Copyright 2019 Dolthub, Inc.
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

package earl

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/libraries/utils/osutil"
)

var validHostRegex = regexp.MustCompile("^[-.a-zA-z0-9]*$")
var validHostWithPortRegex = regexp.MustCompile("^[-.a-zA-z0-9]*:[0-9]*$")

func isValidHost(hostAndPortStr string) bool {
	hostStr := hostAndPortStr
	portStr := ""

	if idx := strings.IndexRune(hostAndPortStr, ':'); idx != -1 {
		hostStr = hostAndPortStr[:idx]
		portStr = strings.TrimSpace(hostAndPortStr[idx+1:])
	}

	if len(portStr) > 0 {
		if _, err := strconv.ParseUint(portStr, 10, 16); err != nil {
			return false
		}
	}

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

	// url.parse doesn't handle file paths that begin with . correctly
	if u.Scheme == "file" && strings.HasPrefix(u.Host, ".") {
		u.Path = u.Host + u.Path
		u.Host = ""
	}

	// if Path is e.g. "/C$/" for a network location, it should instead be "C:/"
	if len(u.Path) >= 3 && u.Path[0] == '/' && u.Path[1] >= 'A' && u.Path[1] <= 'Z' && u.Path[2] == '$' {
		u.Path = u.Path[1:2] + ":" + u.Path[3:]
	} else if !osutil.StartsWithWindowsVolume(u.Path) { // normalize some
		if len(u.Path) == 0 || (u.Path[0] != '/' && u.Path[0] != '.') {
			u.Path = "/" + u.Path
		}
	}
	u.Path = strings.ReplaceAll(u.Path, `\`, "/")

	return u, nil
}

func ParseRawWithAWSSupport(urlStr string) (*url.URL, error) {
	// XXX: This is a kludge to support AWS remote URLs. These URLs use a non-standard syntax to specify the s3 bucket and dynamodb table names, and they look like:
	// aws://[s3_bucket_name:dynamodb_table_name]/path/to/files/in/s3/and/db/key/in/dynamo
	//
	// This was supported by Go url.Parse until 1.25.2, where validation was added to the bracketed hostname component:
	// https://github.com/golang/go/issues/75678
	//
	// Here we explicitly kludge around the aws schema in a hard-coded way. Pretty gross for now.
	if strings.HasPrefix(urlStr, "aws://[") {
		hostStart := 7
		hostEnd := hostStart + strings.Index(urlStr[hostStart:], "]")
		if hostEnd == hostStart-1 {
			return nil, errors.New("could not parse aws schema url: expected aws://[s3_bucket:dynamodb_table] but did not find closing bracket.")
		}
		host := urlStr[hostStart:hostEnd]
		hostColon := strings.Index(host, ":")
		if hostColon == -1 {
			return nil, errors.New("could not parse aws schema url: expected aws://[s3_bucket:dynamodb_table] but did not find colon introducting dynamodb_table.")
		}

		rawBucketName := host[:hostColon]
		rawTableName := host[hostColon+1:]
		// For full compliance with previous beahvior, we pass both components through url.Parse as hostnames to get the same escape handling as we used to have.
		parsedBucketName, err := url.Parse("http://" + rawBucketName)
		if err != nil {
			return nil, fmt.Errorf("could not parse aws s3 bucket name as hostname: %w", err)
		}
		parsedTableName, err := url.Parse("http://" + rawTableName)
		if err != nil {
			return nil, fmt.Errorf("could not parse aws dynamodb table name as hostname: %w", err)
		}
		returnedHost := "[" + parsedBucketName.Host + ":" + parsedTableName.Host + "]"

		// Here we parse the original urlStr but with the host component replaced by a hard coded compliant value.  We then replace the Host in the *URL we return.
		parsed, err := url.Parse("aws://hostname" + urlStr[hostEnd+1:])
		if err != nil {
			return nil, fmt.Errorf("could not parse aws url: %w", err)
		}
		parsed.Host = returnedHost
		return parsed, nil
	}
	return url.Parse(urlStr)
}

func parse(urlStr string) (*url.URL, error) {
	if strIdx := strings.Index(urlStr, ":///"); strIdx != -1 && osutil.StartsWithWindowsVolume(urlStr[strIdx+4:]) {
		return &url.URL{
			Scheme: urlStr[:strIdx],
			Path:   urlStr[strIdx+4:],
		}, nil
	}
	if strIdx := strings.Index(urlStr, "://"); strIdx != -1 && osutil.StartsWithWindowsVolume(urlStr[strIdx+3:]) {
		return &url.URL{
			Scheme: urlStr[:strIdx],
			Path:   urlStr[strIdx+3:],
		}, nil
	}

	if strings.Index(urlStr, "://") == -1 {
		u, err := url.Parse("http://" + urlStr)

		if err == nil && isValidHost(u.Host) {
			u.Scheme = ""
			return u, nil
		} else if err != nil {
			return nil, err
		}
	}

	return ParseRawWithAWSSupport(urlStr)
}

// FileUrlFromPath returns a url for the given path with the "file" scheme i.e. file://...
func FileUrlFromPath(path string, separator rune) string {
	if osutil.StartsWithWindowsVolume(path) {
		path = "/" + path
	}

	if separator != '/' {
		path = strings.ReplaceAll(path, string(separator), "/")
	}

	u := &url.URL{Scheme: "file", Path: path}
	urlStr := u.String()
	return urlStr
}
