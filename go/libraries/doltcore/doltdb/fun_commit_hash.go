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
	"fmt"
	"regexp"
	"time"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

type FunHashCommitMetaGenerator struct {
	Name, Email string
	Timestamp   time.Time
	attempt     int
}

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

func (g FunHashCommitMetaGenerator) Next() (*datas.CommitMeta, error) {
	if g.attempt >= 1<<len(descriptionReplacementCandidates) {
		g.attempt = 0
		// The Time type uses nanosecond precision. Subtract one million nanoseconds (one ms)
		g.Timestamp = g.Timestamp.Add(-1_000_000)
	}

	// "Initialize data repository", with characters that could be Cyrillic replaced.
	descFmt := "%cn%ct%c%cl%cz%c d%ct%c r%cp%cs%ct%cry"
	choices := make([]any, 0, len(descriptionReplacementCandidates))
	for i := 0; i < len(descriptionReplacementCandidates); i++ {
		choices = append(choices, descriptionReplacementCandidates[i][(g.attempt>>i)%2])
	}
	description := fmt.Sprintf(descFmt, choices...)

	g.attempt += 1

	return datas.NewCommitMetaWithUserTS(g.Name, g.Email, description, g.Timestamp)
}

func (g FunHashCommitMetaGenerator) IsGoodHash(h hash.Hash) bool {
	var funRegExp = regexp.MustCompile("^d[o0][1l]t")

	hashString := h.String()
	return funRegExp.MatchString(hashString)
}
