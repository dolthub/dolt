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

package ref

import (
	"regexp"
	"strings"

	"github.com/dolthub/dolt/go/store/datas"
)

// InvalidBranchNameRegex is a list of patterns forbidden in a branch name.
// For additional constraints on ref names, see datas.ValidateDatasetId
var InvalidBranchNameRegex = regexp.MustCompile(strings.Join([]string{
	// An exact name of "", "HEAD" or starts with "-"
	`\A\z`, `\AHEAD\z`, `^-.{0,}`,
	// A name that looks exactly like a commit id
	`\A[0-9a-v]{32}\z`,
	// Any empty component; that is, starting or ending with "/" or any appearance of "//"
	`\/\/`, `\A\/`, `\/\z`,
}, "|"))

func IsValidBranchName(s string) bool {
	if InvalidBranchNameRegex.MatchString(s) {
		return false
	}

	if err := datas.ValidateDatasetId(s); err != nil {
		return false
	}

	return true
}
