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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
	"strconv"
)

type docComparison struct {
	CurrentText  []byte
	DocPk string
	OldText []byte
}

// DocDiff returns the added, modified and removed docs when comparing a root value with an other (newer) value. If the other value,
// is not provided, then we compare the docs on the root value to the docDetails provided.
func DocDiff(ctx context.Context, root *doltdb.RootValue, other *doltdb.RootValue, docDetails []doltdb.DocDetails) (added, modified, removed []string, err error) {
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
			doc := doltdb.DocDetails{}
			updated, err := addDocPKToDocFromRow(newRow, &doc)
			if err != nil {
				return err
			}
			updated, err = addNewerTextToDocFromRow(newRow, &updated)
			if err != nil {
				return err
			}

			docComparison, err := getDocComparisonObjFromDocDetail(ctx, oldTbl, &oldSch, updated)
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
			doc := doltdb.DocDetails{}
			updated, err := addDocPKToDocFromRow(oldRow, &doc)
			if err != nil {
				return err
			}
			updated, err = addTextToDocFromTbl(ctx, newTbl, &newSch, updated)
			if err != nil {
				return err
			}

			docComparison, err := getDocComparisonObjFromDocDetail(ctx, oldTbl, &oldSch, updated)
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

func getDocComparisons(ctx context.Context, tbl *doltdb.Table, sch *schema.Schema, docDetails []doltdb.DocDetails) ([]docComparison, error) {
	docComparisons := make([]docComparison, len(docDetails))

	for i, _ := range docComparisons {
		cmp, err := getDocComparisonObjFromDocDetail(ctx, tbl, sch, docDetails[i])

		if err != nil {
			return nil, err
		}

		docComparisons[i] = cmp
	}

	return docComparisons, nil
}

func getDocComparisonObjFromDocDetail(ctx context.Context, tbl *doltdb.Table, sch *schema.Schema, docDetail doltdb.DocDetails) (docComparison, error) {
	diff := docComparison{DocPk: docDetail.DocPk, CurrentText: docDetail.Text, OldText: nil}

	if tbl != nil && sch != nil {
		key, err := doltdb.DocTblKeyFromName(tbl.Format(), docDetail.DocPk)
		if err != nil {
			return docComparison{}, err
		}

		docRow, ok, err := doltdb.GetDocRow(ctx, tbl, *sch, key)
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

// addTextToDocFromTbl updates the Text field of a docDetail using the provided table and schema.
func addTextToDocFromTbl(ctx context.Context, tbl *doltdb.Table, sch *schema.Schema, doc doltdb.DocDetails) (doltdb.DocDetails, error) {
	if tbl != nil && sch != nil {
		key, err := doltdb.DocTblKeyFromName(tbl.Format(), doc.DocPk)
		if err != nil {
			return doltdb.DocDetails{}, err
		}

		docRow, ok, err := doltdb.GetDocRow(ctx, tbl, *sch, key)
		if err != nil {
			return doltdb.DocDetails{}, err
		}
		if ok {
			docValue, _ := docRow.GetColVal(schema.DocTextTag)
			doc.Text = []byte(docValue.(types.String))
		} else {
			doc.Text = nil
		}
	} else {
		doc.Text = nil
	}
	return doc, nil
}

func addNewerTextToDocFromRow(r row.Row, doc *doltdb.DocDetails) (doltdb.DocDetails, error) {
	docValue, ok := r.GetColVal(schema.DocTextTag)
	if !ok {
		doc.Text = nil
	} else {
		docValStr, err := strconv.Unquote(docValue.HumanReadableString())
		if err != nil {
			return doltdb.DocDetails{}, err
		}
		doc.Text = []byte(docValStr)
	}
	return *doc, nil
}

func addDocPKToDocFromRow(r row.Row, doc *doltdb.DocDetails) (doltdb.DocDetails, error) {
	colVal, _ := r.GetColVal(schema.DocNameTag)
	if colVal == nil {
		doc.DocPk = ""
	} else {
		docName, err := strconv.Unquote(colVal.HumanReadableString())
		if err != nil {
			return doltdb.DocDetails{}, err
		}
		doc.DocPk = docName
	}

	return *doc, nil
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


