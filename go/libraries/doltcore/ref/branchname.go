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
	// Any appearance of ".." or "@{"
	`\.\.`, `@{`,
	// Any empty component; that is, starting or ending with "/" or any appearance of "//"
	`\/\/`, `\A\/`, `\/\z`,
}, "|"))

func IsValidBranchName(s string) bool {
	return !InvalidBranchNameRegex.MatchString(s)
}
