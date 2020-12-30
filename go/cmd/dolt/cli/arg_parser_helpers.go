// Copyright 2020 Dolthub, Inc.
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

package cli

import (
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

// we are more permissive than what is documented.
var SupportedLayouts = []string{
	"2006/01/02",
	"2006/01/02T15:04:05",
	"2006/01/02T15:04:05Z07:00",

	"2006.01.02",
	"2006.01.02T15:04:05",
	"2006.01.02T15:04:05Z07:00",

	"2006-01-02",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05Z07:00",
}

// Parses a date string. Used by multiple commands.
func ParseDate(dateStr string) (time.Time, error) {
	for _, layout := range SupportedLayouts {
		t, err := time.Parse(layout, dateStr)

		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, errors.New("error: '" + dateStr + "' is not in a supported format.")
}

// Parses the author flag for the commit method.
func ParseAuthor(authorStr string) (string, string, error) {
	if len(authorStr) == 0 {
		return "", "", errors.New("Option 'author' requires a value")
	}

	reg := regexp.MustCompile("(?m)([^)]+) \\<([^)]+)") // Regex matches Name <email
	matches := reg.FindStringSubmatch(authorStr)        // This function places the original string at the beginning of matches

	// If name and email are provided
	if len(matches) != 3 {
		return "", "", errors.New("Author not formatted correctly. Use 'Name <author@example.com>' format")
	}

	name := matches[1]
	email := strings.ReplaceAll(matches[2], ">", "")

	return name, email, nil
}

const (
	AllowEmptyFlag   = "allow-empty"
	DateParam        = "date"
	CommitMessageArg = "message"
	AuthorParam      = "author"
	ForceFlag        = "force"
	AllFlag          = "all"
	HardResetParam   = "hard"
	SoftResetParam   = "soft"
)

// Creates the argparser shared dolt commit cli and DOLT_COMMIT.
func CreateCommitArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(CommitMessageArg, "m", "msg", "Use the given {{.LessThan}}msg{{.GreaterThan}} as the commit message.")
	ap.SupportsFlag(AllowEmptyFlag, "", "Allow recording a commit that has the exact same data as its sole parent. This is usually a mistake, so it is disabled by default. This option bypasses that safety.")
	ap.SupportsString(DateParam, "", "date", "Specify the date used in the commit. If not specified the current system time is used.")
	ap.SupportsFlag(ForceFlag, "f", "Ignores any foreign key warnings and proceeds with the commit.")
	ap.SupportsString(AuthorParam, "", "author", "Specify an explicit author using the standard A U Thor <author@example.com> format.")
	ap.SupportsFlag(AllFlag, "a", "Adds all edited files in working to staged.")
	return ap
}

func CreateAddArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "Working table(s) to add to the list tables staged to be committed. The abbreviation '.' can be used to add all tables."})
	ap.SupportsFlag("all", "A", "Stages any and all changes (adds, deletes, and modifications).")
	return ap
}

func CreateResetArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(HardResetParam, "", "Resets the working tables and staged tables. Any changes to tracked tables in the working tree since {{.LessThan}}commit{{.GreaterThan}} are discarded.")
	ap.SupportsFlag(SoftResetParam, "", "Does not touch the working tables, but removes all tables staged to be committed.")
	return ap
}
