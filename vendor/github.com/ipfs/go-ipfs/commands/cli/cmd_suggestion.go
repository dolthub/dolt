package cli

import (
	"fmt"
	"sort"
	"strings"

	levenshtein "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/texttheater/golang-levenshtein/levenshtein"
	cmds "github.com/ipfs/go-ipfs/commands"
)

// Make a custom slice that can be sorted by its levenshtein value
type suggestionSlice []*suggestion

type suggestion struct {
	cmd         string
	levenshtein int
}

func (s suggestionSlice) Len() int {
	return len(s)
}

func (s suggestionSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s suggestionSlice) Less(i, j int) bool {
	return s[i].levenshtein < s[j].levenshtein
}

func suggestUnknownCmd(args []string, root *cmds.Command) []string {
	if root == nil {
		return nil
	}

	arg := args[0]
	var suggestions []string
	sortableSuggestions := make(suggestionSlice, 0)
	var sFinal []string
	const MIN_LEVENSHTEIN = 3

	var options levenshtein.Options = levenshtein.Options{
		InsCost: 1,
		DelCost: 3,
		SubCost: 2,
		Matches: func(sourceCharacter rune, targetCharacter rune) bool {
			return sourceCharacter == targetCharacter
		},
	}

	// Start with a simple strings.Contains check
	for name, _ := range root.Subcommands {
		if strings.Contains(arg, name) {
			suggestions = append(suggestions, name)
		}
	}

	// If the string compare returns a match, return
	if len(suggestions) > 0 {
		return suggestions
	}

	for name, _ := range root.Subcommands {
		lev := levenshtein.DistanceForStrings([]rune(arg), []rune(name), options)
		if lev <= MIN_LEVENSHTEIN {
			sortableSuggestions = append(sortableSuggestions, &suggestion{name, lev})
		}
	}
	sort.Sort(sortableSuggestions)

	for _, j := range sortableSuggestions {
		sFinal = append(sFinal, j.cmd)
	}
	return sFinal
}

func printSuggestions(inputs []string, root *cmds.Command) (err error) {

	suggestions := suggestUnknownCmd(inputs, root)
	if len(suggestions) > 1 {
		err = fmt.Errorf("Unknown Command \"%s\"\n\nDid you mean any of these?\n\n\t%s", inputs[0], strings.Join(suggestions, "\n\t"))
	} else if len(suggestions) > 0 {
		err = fmt.Errorf("Unknown Command \"%s\"\n\nDid you mean this?\n\n\t%s", inputs[0], suggestions[0])
	} else {
		err = fmt.Errorf("Unknown Command \"%s\"\n", inputs[0])
	}
	return
}
