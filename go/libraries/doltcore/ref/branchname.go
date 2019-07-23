// Copyright 2019 Liquidata, Inc.
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

package ref

import (
	"regexp"
	"strings"
)

// The following list of patterns are all forbidden in a branch name.
var InvalidBranchNameRegex = regexp.MustCompile(strings.Join([]string{
	// Any appearance of the following characters: :, ?, [, \, ^, ~, SPACE, TAB, *
	`:`, `\?`, `\[`, `\\`, `\^`, `~`, ` `, `\t`, `\*`,
	// Any ASCII control character.
	`[\x00-\x1f]`, `\x7f`,
	// Any component starting with a "."
	`\A\.`, `/\.`,
	// Any component ending with ".lock"
	`\.lock\z`, `\.lock\/`,
	// An exact name of "", "HEAD" or "-"
	`\A\z`, `\AHEAD\z`, `\A-\z`,
	// A name that looks exactly like a commit id
	`\A[0-9a-v]{32}\z`,
	// Any appearance of ".." or "@{"
	`\.\.`, `@{`,
	// Any empty component; that is, starting or ending with "/" or any appearance of "//"
	`\/\/`, `\A\/`, `\/\z`,
}, "|"))

func IsValidBranchName(s string) bool {
	return !InvalidBranchNameRegex.MatchString(s)
}
