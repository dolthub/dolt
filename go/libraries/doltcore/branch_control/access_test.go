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

package branch_control

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ExprChange represents a singular change in the MatchNode structure.
type ExprChange struct {
	Insert bool
	Exprs  [4]string
	Perms  Permissions
	Result string // If this is not empty, then this checks the final string after all changes have run
}

func TestAccessTable(t *testing.T) {
	tests := []struct {
		Name    string
		Changes []ExprChange
		Result  string // If this is not empty, then this checks the string after this change has run
	}{
		{
			Name: "Simple root insertion",
			Changes: []ExprChange{
				{false, [4]string{`%`, `%`, `%`, `%`}, Permissions_None, `Node("|")`},
				{true, [4]string{`%`, `%`, `root`, `localhost`}, Permissions_Admin, `
Node("|")
 └─ Node("%|%|root|localhost", admin)`},
			},
		},
		{
			Name: "Users 'a' and 'b' on other branches",
			Changes: []ExprChange{
				{false, [4]string{`%`, `%`, `%`, `%`}, Permissions_None, `Node("|")`},
				{true, [4]string{`%`, `other`, `a`, `localhost`}, Permissions_Write, ``},
				{true, [4]string{`%`, `prefix%`, `a`, `localhost`}, Permissions_Admin, `
Node("|")
 └─ Node("%|")
     ├─ Node("other|a|localhost", write)
     └─ Node("prefix%|a|localhost", admin)`},
				{true, [4]string{`%`, `prefix1%`, `b`, `localhost`}, Permissions_Write, `
Node("|")
 └─ Node("%|")
     ├─ Node("other|a|localhost", write)
     └─ Node("prefix")
         ├─ Node("%|a|localhost", admin)
         └─ Node("1%|b|localhost", write)`},
				{false, [4]string{`%`, `prefix1%`, `b`, `localhost`}, Permissions_None, `
Node("|")
 └─ Node("%|")
     ├─ Node("other|a|localhost", write)
     └─ Node("prefix%|a|localhost", admin)`},
				{true, [4]string{`%`, `prefix1%`, `b`, `localhost`}, Permissions_Admin, `
Node("|")
 └─ Node("%|")
     ├─ Node("other|a|localhost", write)
     └─ Node("prefix")
         ├─ Node("%|a|localhost", admin)
         └─ Node("1%|b|localhost", admin)`},
			},
		},
		{
			Name: "Many deletions",
			Changes: []ExprChange{
				{false, [4]string{`%`, `%`, `%`, `%`}, Permissions_None, `Node("|")`},
				{true, [4]string{`%`, `%`, `root`, `localhost`}, Permissions_Admin, ``},
				{true, [4]string{`%`, `%`, `testuser`, `localhost_1`}, Permissions_Write, `
Node("|")
 └─ Node("%|%|")
     ├─ Node("root|localhost", admin)
     └─ Node("testuser|localhost_1", write)
`},
				{true, [4]string{`%`, `%`, `testuser`, `localhost_2`}, Permissions_Write, `
Node("|")
 └─ Node("%|%|")
     ├─ Node("root|localhost", admin)
     └─ Node("testuser|localhost_")
         ├─ Node("1", write)
         └─ Node("2", write)`},
				{true, [4]string{`%`, `%`, `testuser`, `localhost`}, Permissions_Write, `
Node("|")
 └─ Node("%|%|")
     ├─ Node("root|localhost", admin)
     └─ Node("testuser|localhost", write)
         └─ Node("_")
             ├─ Node("1", write)
             └─ Node("2", write)`},
				{true, [4]string{`%`, `%`, `testuser`, `localhost_3`}, Permissions_Write, `
Node("|")
 └─ Node("%|%|")
     ├─ Node("root|localhost", admin)
     └─ Node("testuser|localhost", write)
         └─ Node("_")
             ├─ Node("1", write)
             ├─ Node("2", write)
             └─ Node("3", write)`},
				{true, [4]string{`%`, `%`, `testuser`, `localhost_5`}, Permissions_Write, `
Node("|")
 └─ Node("%|%|")
     ├─ Node("root|localhost", admin)
     └─ Node("testuser|localhost", write)
         └─ Node("_")
             ├─ Node("1", write)
             ├─ Node("2", write)
             ├─ Node("3", write)
             └─ Node("5", write)`},
				{true, [4]string{`%`, `%`, `testuser`, `localhost_4`}, Permissions_Write, `
Node("|")
 └─ Node("%|%|")
     ├─ Node("root|localhost", admin)
     └─ Node("testuser|localhost", write)
         └─ Node("_")
             ├─ Node("1", write)
             ├─ Node("2", write)
             ├─ Node("3", write)
             ├─ Node("4", write)
             └─ Node("5", write)`},
				{false, [4]string{`%`, `%`, `testuser`, `localhost_2`}, Permissions_None, `
Node("|")
 └─ Node("%|%|")
     ├─ Node("root|localhost", admin)
     └─ Node("testuser|localhost", write)
         └─ Node("_")
             ├─ Node("1", write)
             ├─ Node("3", write)
             ├─ Node("4", write)
             └─ Node("5", write)`},
				{false, [4]string{`%`, `%`, `testuser`, `localhost_3`}, Permissions_None, `
Node("|")
 └─ Node("%|%|")
     ├─ Node("root|localhost", admin)
     └─ Node("testuser|localhost", write)
         └─ Node("_")
             ├─ Node("1", write)
             ├─ Node("4", write)
             └─ Node("5", write)`},
				{false, [4]string{`%`, `%`, `testuser`, `localhost_5`}, Permissions_None, `
Node("|")
 └─ Node("%|%|")
     ├─ Node("root|localhost", admin)
     └─ Node("testuser|localhost", write)
         └─ Node("_")
             ├─ Node("1", write)
             └─ Node("4", write)`},
				{false, [4]string{`%`, `%`, `testuser`, `localhost`}, Permissions_None, `
Node("|")
 └─ Node("%|%|")
     ├─ Node("root|localhost", admin)
     └─ Node("testuser|localhost_")
         ├─ Node("1", write)
         └─ Node("4", write)`},
				{false, [4]string{`%`, `%`, `testuser`, `localhost_1`}, Permissions_None, `
Node("|")
 └─ Node("%|%|")
     ├─ Node("root|localhost", admin)
     └─ Node("testuser|localhost_4", write)`},
				{false, [4]string{`%`, `%`, `testuser`, `localhost_4`}, Permissions_None, `
Node("|")
 └─ Node("%|%|root|localhost", admin)`},
				{false, [4]string{`%`, `%`, `root`, `localhost`}, Permissions_None, `Node("|")`},
			},
		},
		{
			Name: "Inserting superset",
			Changes: []ExprChange{
				{false, [4]string{`%`, `%`, `%`, `%`}, Permissions_None, `Node("|")`},
				{true, [4]string{`%`, `%`, `root`, `localhost`}, Permissions_Admin, ``},
				{true, [4]string{`%`, `prefix`, `testuser`, `localhost`}, Permissions_Admin, `
Node("|")
 └─ Node("%|")
     ├─ Node("%|root|localhost", admin)
     └─ Node("prefix|testuser|localhost", admin)`},
				{true, [4]string{`%`, `prefix1%`, `testuser`, `localhost`}, Permissions_Admin, `
Node("|")
 └─ Node("%|")
     ├─ Node("%|root|localhost", admin)
     └─ Node("prefix")
         ├─ Node("|testuser|localhost", admin)
         └─ Node("1%|testuser|localhost", admin)`},
				{true, [4]string{`%`, `prefix2_`, `testuser`, `localhost`}, Permissions_Admin, `
Node("|")
 └─ Node("%|")
     ├─ Node("%|root|localhost", admin)
     └─ Node("prefix")
         ├─ Node("|testuser|localhost", admin)
         ├─ Node("1%|testuser|localhost", admin)
         └─ Node("2_|testuser|localhost", admin)`},
				{true, [4]string{`%`, `prefix3_`, `testuser`, `localhost`}, Permissions_Admin, `
Node("|")
 └─ Node("%|")
     ├─ Node("%|root|localhost", admin)
     └─ Node("prefix")
         ├─ Node("|testuser|localhost", admin)
         ├─ Node("1%|testuser|localhost", admin)
         ├─ Node("2_|testuser|localhost", admin)
         └─ Node("3_|testuser|localhost", admin)`},
				{true, [4]string{`%`, `prefix2%`, `testuser`, `localhost`}, Permissions_Admin, `
Node("|")
 └─ Node("%|")
     ├─ Node("%|root|localhost", admin)
     └─ Node("prefix")
         ├─ Node("|testuser|localhost", admin)
         ├─ Node("1%|testuser|localhost", admin)
         ├─ Node("2")
         │   ├─ Node("%|testuser|localhost", admin)
         │   └─ Node("_|testuser|localhost", admin)
         └─ Node("3_|testuser|localhost", admin)`},
				{false, [4]string{`%`, `%`, `root`, `localhost`}, Permissions_None, `
Node("|")
 └─ Node("%|prefix")
     ├─ Node("|testuser|localhost", admin)
     ├─ Node("1%|testuser|localhost", admin)
     ├─ Node("2")
     │   ├─ Node("%|testuser|localhost", admin)
     │   └─ Node("_|testuser|localhost", admin)
     └─ Node("3_|testuser|localhost", admin)`},
			},
		},
		{
			Name: "Mixed operators",
			Changes: []ExprChange{
				{true, [4]string{`%`, `%`, `root`, `localhost`}, Permissions_Admin, `
Node("|")
 └─ Node("%|%|")
     ├─ Node("%|%", write)
     └─ Node("root|localhost", admin)`},
				{true, [4]string{`%wy`, `prefix%`, `%`, `%`}, Permissions_Write, `
Node("|")
 └─ Node("%")
     ├─ Node("|%|")
     │   ├─ Node("%|%", write)
     │   └─ Node("root|localhost", admin)
     └─ Node("wy|prefix%|%|%", write)`},
				{true, [4]string{`%wy`, `mask%`, `%`, `%`}, Permissions_Write, `
Node("|")
 └─ Node("%")
     ├─ Node("|%|")
     │   ├─ Node("%|%", write)
     │   └─ Node("root|localhost", admin)
     └─ Node("wy|")
         ├─ Node("mask%|%|%", write)
         └─ Node("prefix%|%|%", write)`},
			},
		},
		{
			Name: "Inserted Subsets", // This isn't valid in normal operation, but the underlying code must still work
			Changes: []ExprChange{
				{true, [4]string{`%`, `%`, `root`, `localhost`}, Permissions_Admin, ``},
				{true, [4]string{`_`, `%`, `__ot`, `localhost`}, Permissions_Write, `
Node("|")
 ├─ Node("%|%|")
 │   ├─ Node("%|%", write)
 │   └─ Node("root|localhost", admin)
 └─ Node("_|%|__ot|localhost", write)`},
				{true, [4]string{`a`, `%`, `%ot`, `%host`}, Permissions_Write, `
Node("|")
 ├─ Node("%|%|")
 │   ├─ Node("%|%", write)
 │   └─ Node("root|localhost", admin)
 ├─ Node("_|%|__ot|localhost", write)
 └─ Node("a|%|%ot|%host", write)`},
				{true, [4]string{`_%`, `_`, `%_`, `%%`} /*This is folded*/, Permissions_Admin, `
Node("|")
 ├─ Node("%|%|")
 │   ├─ Node("%|%", write)
 │   └─ Node("root|localhost", admin)
 ├─ Node("_")
 │   ├─ Node("|%|__ot|localhost", write)
 │   └─ Node("%|_|_%|%", admin)
 └─ Node("a|%|%ot|%host", write)`},
				{true, [4]string{`a`, `b`, `c`, `d`}, Permissions_Read, `
Node("|")
 ├─ Node("%|%|")
 │   ├─ Node("%|%", write)
 │   └─ Node("root|localhost", admin)
 ├─ Node("_")
 │   ├─ Node("|%|__ot|localhost", write)
 │   └─ Node("%|_|_%|%", admin)
 └─ Node("a|")
     ├─ Node("%|%ot|%host", write)
     └─ Node("b|c|d", read)`},
			},
		},
		{
			Name: "Delete to root",
			Changes: []ExprChange{
				{true, [4]string{`%`, `%`, `root`, `localhost`}, Permissions_Admin, `
Node("|")
 └─ Node("%|%|")
     ├─ Node("%|%", write)
     └─ Node("root|localhost", admin)`},
				{false, [4]string{`%`, `%`, `root`, `localhost`}, Permissions_None, `
Node("|")
 └─ Node("%|%|%|%", write)`},
				{false, [4]string{`%`, `%`, `%`, `%`}, Permissions_None, `Node("|")`},
				{true, [4]string{`%`, `%`, `root`, `localhost`}, Permissions_Admin, `
Node("|")
 └─ Node("%|%|root|localhost", admin)`},
				{false, [4]string{`%`, `%`, `root`, `localhost`}, Permissions_None, `Node("|")`},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			accessTbl := newAccess()
			accessTbl.insertDefaultRow()
			for _, change := range test.Changes {
				if change.Insert {
					accessTbl.Insert(change.Exprs[0], change.Exprs[1], change.Exprs[2], change.Exprs[3], change.Perms)
				} else {
					accessTbl.Delete(change.Exprs[0], change.Exprs[1], change.Exprs[2], change.Exprs[3])
				}
				if len(change.Result) > 0 {
					actual := accessTbl.Root.String(255)
					prefix := "DELETE"
					postfix := ""
					if change.Insert {
						prefix = "INSERT"
						switch change.Perms {
						case Permissions_Admin:
							postfix = ", admin"
						case Permissions_Write:
							postfix = ", write"
						case Permissions_Merge:
							postfix = ", merge"
						case Permissions_Read, Permissions_None:
							postfix = ", read"
						}
					}
					assert.Equal(t, strings.TrimSpace(change.Result), actual, fmt.Sprintf(
						`%s("%s", "%s", "%s", "%s"%s)`,
						prefix, change.Exprs[0], change.Exprs[1], change.Exprs[2], change.Exprs[3], postfix))
				}
			}
			if len(test.Result) > 0 {
				actual := accessTbl.Root.String(255)
				require.Equal(t, strings.TrimSpace(test.Result), actual)
			}
		})
	}
}
