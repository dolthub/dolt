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
		detailsWithValues, err := addValuesToDocs(ctx, oldTbl, &oldSch, docDetails)
		if err != nil {
			return nil, nil, nil, err
		}
		a, m, r := GetDocDiffsFromDocDetails(detailsWithValues)
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

	docDetailsBtwnRoots, err := getDocDetailsBtwnRoots(ctx, newTbl, newSch, newTblFound, oldTbl, oldSch, oldTblFound)
	if err != nil {
		return nil, nil, nil, err
	}

	a, m, r := GetDocDiffsFromDocDetails(docDetailsBtwnRoots)
	return a, m, r, nil
}

func getDocDetailsBtwnRoots(ctx context.Context, newTbl *doltdb.Table, newSch schema.Schema, newTblFound bool, oldTbl *doltdb.Table, oldSch schema.Schema, oldTblFound bool) ([]doltdb.DocDetails, error) {
	var docDetailsBtwnRoots []doltdb.DocDetails
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
			updated, err = addNewerTextToDocFromRow(ctx, newRow, &updated)
			if err != nil {
				return err
			}
			updated, err = AddValueToDocFromTbl(ctx, oldTbl, &oldSch, updated)
			if err != nil {
				return err
			}
			docDetailsBtwnRoots = append(docDetailsBtwnRoots, updated)
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
			updated, err = AddValueToDocFromTbl(ctx, oldTbl, &oldSch, updated)
			if err != nil {
				return err
			}
			updated, err = AddNewerTextToDocFromTbl(ctx, newTbl, &newSch, updated)
			if err != nil {
				return err
			}

			if updated.Value != nil && updated.Text == nil {
				docDetailsBtwnRoots = append(docDetailsBtwnRoots, updated)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return docDetailsBtwnRoots, nil
}

// AddValueToDocFromTbl updates the Value field of a docDetail using the provided table and schema.
func AddValueToDocFromTbl(ctx context.Context, tbl *doltdb.Table, sch *schema.Schema, docDetail doltdb.DocDetails) (doltdb.DocDetails, error) {
	if tbl != nil && sch != nil {
		key, err := doltdb.DocTblKeyFromName(tbl.Format(), docDetail.DocPk)
		if err != nil {
			return doltdb.DocDetails{}, err
		}

		docRow, ok, err := doltdb.GetDocRow(ctx, tbl, *sch, key)
		if err != nil {
			return doltdb.DocDetails{}, err
		}

		if ok {
			docValue, _ := docRow.GetColVal(schema.DocTextTag)
			docDetail.Value = docValue
		} else {
			docDetail.Value = nil
		}
	} else {
		docDetail.Value = nil
	}
	return docDetail, nil
}

// AddNewerTextToDocFromTbl updates the Text field of a docDetail using the provided table and schema.
func AddNewerTextToDocFromTbl(ctx context.Context, tbl *doltdb.Table, sch *schema.Schema, doc doltdb.DocDetails) (doltdb.DocDetails, error) {
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

func addNewerTextToDocFromRow(ctx context.Context, r row.Row, doc *doltdb.DocDetails) (doltdb.DocDetails, error) {
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

func addValuesToDocs(ctx context.Context, tbl *doltdb.Table, sch *schema.Schema, docDetails []doltdb.DocDetails) ([]doltdb.DocDetails, error) {
	if tbl != nil && sch != nil {
		for i, details := range docDetails {
			newDetails, err := AddValueToDocFromTbl(ctx, tbl, sch, details)
			if err != nil {
				return nil, err
			}
			docDetails[i] = newDetails
		}
	}
	return docDetails, nil
}

func GetDocDiffsFromDocDetails(docDetails []doltdb.DocDetails) (added, modified, removed []string) {
	added = []string{}
	modified = []string{}
	removed = []string{}
	for _, doc := range docDetails {
		added, modified, removed = appendDocDiffs(added, modified, removed, doc.Value, doc.Text, doc.DocPk)
	}
	return added, modified, removed
}

func appendDocDiffs(added, modified, removed []string, olderVal types.Value, newerVal []byte, docPk string) (add, mod, rem []string) {
	if olderVal == nil && newerVal != nil {
		added = append(added, docPk)
	} else if olderVal != nil {
		if newerVal == nil {
			removed = append(removed, docPk)
		} else if olderVal.HumanReadableString() != strconv.Quote(string(newerVal)) {
			modified = append(modified, docPk)
		}
	}
	return added, modified, removed
}


