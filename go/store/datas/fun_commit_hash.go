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

package datas

import (
	"fmt"
	"regexp"
	"time"
)

// An alternate implementation of CommitMetaGenerator, which only produces hashes which begin with "d0lt" or similar.
type funHashCommitMetaGenerator struct {
	timestamp time.Time
	regex     *regexp.Regexp
	name      string
	email     string
	attempt   int
}

func MakeFunCommitMetaGenerator(name, email string, timestamp time.Time) CommitMetaGenerator {
	return &funHashCommitMetaGenerator{
		name:      name,
		email:     email,
		timestamp: timestamp,
		attempt:   0,
		regex:     regexp.MustCompile("^d[o0][1l]t"),
	}
}

// Each entry in this array represents a character in the default initial commit message
// that could be replaced with a Cyrillic homoglyph.
var descriptionReplacementCandidates = [][]rune{
	{'I', '\u0406'},
	{'i', '\u0456'},
	{'i', '\u0456'},
	{'a', '\u0430'},
	{'i', '\u0456'},
	{'e', '\u0435'},
	{'a', '\u0430'},
	{'a', '\u0430'},
	{'e', '\u0435'},
	{'o', '\u043e'},
	{'i', '\u0456'},
	{'o', '\u043e'},
}

func (g *funHashCommitMetaGenerator) Next() (*CommitMeta, error) {
	if g.attempt >= 1<<len(descriptionReplacementCandidates) {
		g.attempt = 0
		// The Time type uses nanosecond precision. Subtract one million nanoseconds (one ms)
		g.timestamp = g.timestamp.Add(-1_000_000)
	}

	// "Initialize data repository", with characters that could be Cyrillic replaced.
	descFmt := "%cn%ct%c%cl%cz%c d%ct%c r%cp%cs%ct%cry"
	choices := make([]any, 0, len(descriptionReplacementCandidates))
	for i := 0; i < len(descriptionReplacementCandidates); i++ {
		choices = append(choices, descriptionReplacementCandidates[i][(g.attempt>>i)%2])
	}
	description := fmt.Sprintf(descFmt, choices...)

	g.attempt += 1

	return NewCommitMetaWithUserTimestamp(g.name, g.email, description, g.timestamp)
}

func (g *funHashCommitMetaGenerator) IsGoodCommit(commit *Commit) bool {
	hashString := commit.Addr().String()
	return g.regex.MatchString(hashString)
}
