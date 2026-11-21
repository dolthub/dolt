// Copyright 2026 Dolthub, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQualifiedDbName(t *testing.T) {
	tests := []struct {
		name     string
		base     string
		revision string
		str      string
		key      QualifiedDbKey
	}{
		{"MyDb", "MyDb", "", "MyDb", QualifiedDbKey{"mydb", ""}},
		{"MyDb/Main", "MyDb", "Main", "MyDb/Main", QualifiedDbKey{"mydb", "main"}},
		{"MyDb@Main", "MyDb", "Main", "MyDb/Main", QualifiedDbKey{"mydb", "main"}},
		{"MyDb/Feat/X", "MyDb", "Feat/X", "MyDb/Feat/X", QualifiedDbKey{"mydb", "feat/x"}},
		{"MyDb@Feat/X", "MyDb", "Feat/X", "MyDb/Feat/X", QualifiedDbKey{"mydb", "feat/x"}},
		{"MyDb/Feat@X", "MyDb", "Feat@X", "MyDb/Feat@X", QualifiedDbKey{"mydb", "feat@x"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := ParseQualifiedDbName(tt.name)
			assert.Equal(t, tt.base, n.Base)
			assert.Equal(t, tt.revision, n.Revision)
			assert.Equal(t, tt.revision != "", n.IsQualified())
			assert.Equal(t, tt.str, n.String())
			assert.Equal(t, tt.key, n.Key())
			assert.Equal(t, tt.key, NewQualifiedDbKey(tt.name))
			assert.Equal(t, tt.key.Db, NewDbKey(tt.name))
			assert.Equal(t, tt.key.Db, n.DbKey())
		})
	}

	assert.Equal(t, "mydb/feat/x", NewQualifiedDbKey("MyDb@Feat/X").String())
	assert.Equal(t, "mydb", NewQualifiedDbKey("MyDb").String())
	assert.Equal(t, QualifiedDbName{"MyDb", "b2"}, ParseQualifiedDbName("MyDb/b1").WithRevision("b2"))
	assert.Equal(t, QualifiedDbName{"MyDb", ""}, ParseQualifiedDbName("MyDb/b1").WithRevision(""))
}
