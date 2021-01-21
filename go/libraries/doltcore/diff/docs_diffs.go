// Copyright 2020 Dolthub, Inc.
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

package diff

import (
	"context"
	"strconv"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

type docComparison struct {
	DocPk       string
	CurrentText []byte
	OldText     []byte
}

// DocsDiff returns the added, modified and removed docs when comparing a root value with an other (newer) value. If the other value,
// is not provided, then we compare the docs on the root value to the docDetails provided.
func DocsDiff(ctx context.Context, root *doltdb.RootValue, other *doltdb.RootValue, docDetails doltdocs.Docs) (added, modified, removed []string, err error) {
	oldTbl, oldTblFound, err := root.GetTable(ctx, doltdb.DocTableName)
	if err != nil {
		return nil, nil, nil, err
	}
	var oldSch schema.Schema
	if oldTblFound {
		sch, err := oldTbl.GetSchema(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		oldSch = sch
	}

	if other == nil {
		docComparisons, err := getDocComparisons(ctx, oldTbl, &oldSch, docDetails)
		if err != nil {
			return nil, nil, nil, err
		}
		a, m, r := computeDiffsFromDocComparisons(docComparisons)
		return a, m, r, nil
	}

	newTbl, newTblFound, err := other.GetTable(ctx, doltdb.DocTableName)
	if err != nil {
		return nil, nil, nil, err
	}

	var newSch schema.Schema
	if newTblFound {
		sch, err := newTbl.GetSchema(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		newSch = sch
	}

	docComparisonBtwnRoots, err := getDocComparisonsBtwnRoots(ctx, newTbl, newSch, newTblFound, oldTbl, oldSch, oldTblFound)
	if err != nil {
		return nil, nil, nil, err
	}

	a, m, r := computeDiffsFromDocComparisons(docComparisonBtwnRoots)
	return a, m, r, nil
}

func getDocComparisonsBtwnRoots(ctx context.Context, newTbl *doltdb.Table, newSch schema.Schema, newTblFound bool, oldTbl *doltdb.Table, oldSch schema.Schema, oldTblFound bool) ([]docComparison, error) {
	var docComparisonBtwnRoots []docComparison

	if newTblFound {
		newRows, err := newTbl.GetRowData(ctx)
		if err != nil {
			return nil, err
		}
		err = newRows.IterAll(ctx, func(key, val types.Value) error {
			newRow, err := row.FromNoms(newSch, key.(types.Tuple), val.(types.Tuple))
			if err != nil {
				return err
			}
			doc := doltdocs.Doc{}

			docPk, err := doltdocs.GetDocPKFromRow(newRow)
			if err != nil {
				return err
			}
			doc.DocPk = docPk

			text, err := doltdocs.GetDocTextFromRow(newRow)
			if err != nil {
				return err
			}
			doc.Text = text

			docComparison, err := newDocComparison(ctx, oldTbl, &oldSch, doc)
			if err != nil {
				return err
			}
			docComparisonBtwnRoots = append(docComparisonBtwnRoots, docComparison)

			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	if oldTblFound {
		oldRows, err := oldTbl.GetRowData(ctx)
		if err != nil {
			return nil, err
		}
		err = oldRows.IterAll(ctx, func(key, val types.Value) error {
			oldRow, err := row.FromNoms(oldSch, key.(types.Tuple), val.(types.Tuple))
			if err != nil {
				return err
			}
			doc := doltdocs.Doc{}

			docPk, err := doltdocs.GetDocPKFromRow(oldRow)
			if err != nil {
				return err
			}
			doc.DocPk = docPk

			docText, err := doltdocs.GetDocTextFromTbl(ctx, newTbl, &newSch, doc.DocPk)
			if err != nil {
				return err
			}
			doc.Text = docText

			docComparison, err := newDocComparison(ctx, oldTbl, &oldSch, doc)
			if err != nil {
				return err
			}

			if docComparison.OldText != nil && docComparison.CurrentText == nil {
				docComparisonBtwnRoots = append(docComparisonBtwnRoots, docComparison)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return docComparisonBtwnRoots, nil
}

func getDocComparisons(ctx context.Context, tbl *doltdb.Table, sch *schema.Schema, docDetails doltdocs.Docs) ([]docComparison, error) {
	docComparisons := make([]docComparison, len(docDetails))

	for i, _ := range docComparisons {
		cmp, err := newDocComparison(ctx, tbl, sch, docDetails[i])

		if err != nil {
			return nil, err
		}

		docComparisons[i] = cmp
	}

	return docComparisons, nil
}

func newDocComparison(ctx context.Context, tbl *doltdb.Table, sch *schema.Schema, doc doltdocs.Doc) (docComparison, error) {
	diff := docComparison{DocPk: doc.DocPk, CurrentText: doc.Text, OldText: nil}

	if tbl != nil && sch != nil {
		key, err := doltdocs.DocTblKeyFromName(tbl.Format(), doc.DocPk)
		if err != nil {
			return docComparison{}, err
		}

		docRow, ok, err := doltdocs.GetDocRow(ctx, tbl, *sch, key)
		if err != nil {
			return docComparison{}, err
		}

		if ok {
			docValue, _ := docRow.GetColVal(schema.DocTextTag)
			docStr, _ := strconv.Unquote(docValue.HumanReadableString())
			diff.OldText = []byte(docStr)
		}
	}

	return diff, nil
}

func computeDiffsFromDocComparisons(docComparisons []docComparison) (added, modified, removed []string) {
	added = []string{}
	modified = []string{}
	removed = []string{}
	for _, doc := range docComparisons {
		added, modified, removed = appendDocDiffs(added, modified, removed, doc.OldText, doc.CurrentText, doc.DocPk)
	}
	return added, modified, removed
}

func appendDocDiffs(added, modified, removed []string, olderVal []byte, newerVal []byte, docPk string) (add, mod, rem []string) {
	if olderVal == nil && newerVal != nil {
		added = append(added, docPk)
	} else if olderVal != nil {
		if newerVal == nil {
			removed = append(removed, docPk)
		} else if strconv.Quote(string(olderVal)) != strconv.Quote(string(newerVal)) {
			modified = append(modified, docPk)
		}
	}
	return added, modified, removed
}
