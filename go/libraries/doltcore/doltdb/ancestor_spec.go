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

package doltdb

import (
	"errors"
	"strconv"
	"strings"
)

func isDigit(b byte) bool {
	return b >= byte('0') && b <= byte('9')
}

func isValidMergeSpec(num int) bool {
	return num == 1 || num == 2
}

func parseInstructions(aSpec string) ([]int, error) {
	instructions := make([]int, 0)

	for i := 0; i < len(aSpec); i++ {
		currInst := aSpec[i]

		start := i
		for i+1 < len(aSpec) && isDigit(aSpec[i+1]) {
			i++
		}

		num := 1

		if start != i {
			var err error
			numStr := aSpec[start+1 : i+1]
			num, err = strconv.Atoi(numStr)

			if err != nil {
				return nil, err
			}
		}

		switch currInst {
		case '^':
			if !isValidMergeSpec(num) {
				return nil, ErrInvalidAncestorSpec
			}
			instructions = append(instructions, num-1)
		case '~':
			for j := 0; j < num; j++ {
				instructions = append(instructions, 0)
			}
		default:
			return nil, errors.New("Invalid HEAD spec: " + aSpec)
		}
	}

	return instructions, nil
}

var emptyASpec = &AncestorSpec{"", []int{}}

// AncestorSpec supports using ^, ^N, and ~N together to specify an ancestor of a commit.
// ^ after a commit spec means the first parent of that commit. ^<n> means the <n>th parent (i.e. <rev>^ is equivalent
// to <rev>^1). As a special rule.
// ~<n> after a commit spec means the commit object that is the <n>th generation grand-parent of the named commit
// object, following only the first parents. I.e. <rev>~3 is equivalent to <rev>^^^ which is equivalent to
// <rev>^1^1^1. See below for an illustration of the usage of this form.
type AncestorSpec struct {

	// SpecStr is string representation of the AncestorSpec
	SpecStr string

	// Instructions is a slice of parent indices. As you walk up the ancestor tree the first instruction is the index of
	// the parent that should be used.  The second index is the index of that parents parent that should be used. etc.
	// When you've exhausted the instructions you've reached the referenced commit.
	Instructions []int
}

// NewAncestorSpec takes an input string and validates it and converts it to a set of instructions used in walking up
// the ancestor tree
func NewAncestorSpec(s string) (*AncestorSpec, error) {
	if s == "" {
		return emptyASpec, nil
	}

	inst, err := parseInstructions(s)

	if err != nil {
		return nil, err
	}

	return &AncestorSpec{s, inst}, nil
}

// SplitAncestorSpec takes a string that is a commit spec suffixed with an ancestor spec, and splits them apart.
// If there is no ancestor spec then only the commit spec will be returned and the ancestorSpec will have no empty.
func SplitAncestorSpec(s string) (string, *AncestorSpec, error) {
	cleanStr := strings.TrimSpace(s)

	cIdx := strings.IndexByte(cleanStr, '^')
	tIdx := strings.IndexByte(cleanStr, '~')

	if cIdx == -1 && tIdx == -1 {
		return cleanStr, emptyASpec, nil
	}

	idx := cIdx
	if cIdx == -1 || (tIdx != -1 && tIdx < cIdx) {
		idx = tIdx
	}

	commitSpec := cleanStr[:idx]
	as, err := NewAncestorSpec(s[idx:])

	if err != nil {
		return "", emptyASpec, err
	}

	return commitSpec, as, nil
}
