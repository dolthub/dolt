// Copyright 2022 Dolthub, Inc.
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

package docscmds

import (
	"context"

	textdiff "github.com/andreyvit/diff"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var diffDocs = cli.CommandDocumentationContent{
	ShortDesc: "",
	LongDesc:  ``,
	Synopsis:  []string{},
}

type DiffCmd struct{}

// Name implements cli.Command.
func (cmd DiffCmd) Name() string {
	return "diff"
}

// Description implements cli.Command.
func (cmd DiffCmd) Description() string {
	return "Diffs Dolt Docs against their committed version."
}

// RequiresRepo implements cli.Command.
func (cmd DiffCmd) RequiresRepo() bool {
	return true
}

// Docs implements cli.Command.
func (cmd DiffCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(diffDocs, ap)
}

// ArgParser implements cli.Command.
func (cmd DiffCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// Exec implements cli.Command.
func (cmd DiffCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	textdiff.LineDiff("abc", "xyz")
	panic("dolt docs diff")
}

//func diffDoltDocs(ctx context.Context, dEnv *env.DoltEnv, dArgs *diffArgs) error {
//	_, docs, err := actions.GetTables(dArgs.docSet.AsSlice())
//
//	if err != nil {
//		return err
//	}
//
//	return printDocDiffs(ctx, dArgs.fromRoot, dArgs.toRoot, docs)
//}
//
//func printDocDiffs(ctx context.Context, from, to *doltdb.RootValue, docsFilter doltdocs.Docs) error {
//	bold := color.New(color.Bold)
//
//	comparisons, err := diff.DocsDiffToComparisons(ctx, from, to, docsFilter)
//	if err != nil {
//		return err
//	}
//
//	for _, doc := range docsFilter {
//		for _, comparison := range comparisons {
//			if doc.DocPk == comparison.DocName {
//				if comparison.OldText == nil && comparison.CurrentText != nil {
//					printAddedDoc(bold, comparison.DocName)
//				} else if comparison.OldText != nil {
//					older := string(comparison.OldText)
//					newer := string(comparison.CurrentText)
//
//					lines := textdiff.LineDiffAsLines(older, newer)
//
//					if comparison.CurrentText == nil {
//						printDeletedDoc(bold, comparison.DocName, lines)
//					} else if len(lines) > 0 && newer != older {
//						printModifiedDoc(bold, comparison.DocName, lines)
//					}
//				}
//			}
//		}
//	}
//
//	return nil
//}
//
//func printModifiedDoc(bold *color.Color, pk string, lines []string) {
//	_, _ = bold.Printf("diff --dolt a/%[1]s b/%[1]s\n", pk)
//	_, _ = bold.Printf("--- a/%s\n", pk)
//	_, _ = bold.Printf("+++ b/%s\n", pk)
//
//	printDiffLines(bold, lines)
//}
//
//func printAddedDoc(bold *color.Color, pk string) {
//	_, _ = bold.Printf("diff --dolt a/%[1]s b/%[1]s\n", pk)
//	_, _ = bold.Println("added doc")
//}
//
//func printDeletedDoc(bold *color.Color, pk string, lines []string) {
//	_, _ = bold.Printf("diff --dolt a/%[1]s b/%[1]s\n", pk)
//	_, _ = bold.Println("deleted doc")
//
//	printDiffLines(bold, lines)
//}
