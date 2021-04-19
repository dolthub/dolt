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

package merge

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/store/types"
)

var WriteBufSize = 256 * 1024
var mergeVersionToLabel = map[MergeVersion]string{
	OurVersion:   "ours  ",
	TheirVersion: "theirs",
	BaseVersion:  "base  ",
	Blank:        "      ",
}
var diffTypeToOpLabel = map[types.DiffChangeType]string{
	types.DiffChangeAdded:    " + ",
	types.DiffChangeRemoved:  " - ",
	types.DiffChangeModified: " * ",
}

var deleteColor = color.New(color.FgRed, color.CrossedOut)
var modifiedColor = color.New(color.FgYellow)
var addedColor = color.New(color.FgGreen)

var diffTypeToColor = map[types.DiffChangeType]diff.ColorFunc{
	types.DiffChangeAdded:    addedColor.Sprint,
	types.DiffChangeModified: modifiedColor.Sprint,
	types.DiffChangeRemoved:  deleteColor.Sprint,
}

type ConflictSink struct {
	sch schema.Schema
	ttw *tabular.TextTableWriter
}

const (
	opColTag     = schema.ReservedTagMin
	sourceColTag = schema.ReservedTagMin + 1
)

func NewConflictSink(wr io.WriteCloser, sch schema.Schema, colSep string) (*ConflictSink, error) {
	_, additionalCols := untyped.NewUntypedSchemaWithFirstTag(opColTag, "op", "source")
	outSch, err := untyped.UntypedSchemaUnion(additionalCols, sch)

	if err != nil {
		return nil, err
	}

	ttw, err := tabular.NewTextTableWriter(wr, outSch)

	if err != nil {
		return nil, err
	}

	return &ConflictSink{outSch, ttw}, nil
}

// GetSchema gets the schema of the rows that this writer writes
func (cs *ConflictSink) GetSchema() schema.Schema {
	return cs.sch
}

var noColorFunc = func(i ...interface{}) string {
	if len(i) == 0 {
		return ""
	} else {
		return fmt.Sprint(i...)
	}
}

func (cs *ConflictSink) ProcRowWithProps(r row.Row, props pipeline.ReadableMap) error {
	taggedVals := make(row.TaggedValues)

	colorFunc := noColorFunc
	mergeVersion, ok := props.Get(mergeVersionProp)

	// The column header row won't have properties to read
	if !ok {
		mergeVersion = Blank
	}
	taggedVals[opColTag] = types.String("   ")
	taggedVals[sourceColTag] = types.String(mergeVersionToLabel[mergeVersion.(MergeVersion)])

	if mergeVersion != BaseVersion {
		mergeRowOp, ok := props.Get(mergeRowOperation)
		// The column header row won't have properties to read
		if ok {
			dt := mergeRowOp.(types.DiffChangeType)
			taggedVals[opColTag] = types.String(diffTypeToOpLabel[dt])
			colorFunc = diffTypeToColor[dt]
		} else {
			taggedVals[opColTag] = types.String("   ")
		}
	}

	err := cs.sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if val, ok := r.GetColVal(tag); ok {
			taggedVals[tag] = types.String(colorFunc(string(val.(types.String))))
		}
		return false, nil
	})

	if err != nil {
		return err
	}

	r, err = row.New(r.Format(), cs.sch, taggedVals)

	if err != nil {
		return err
	}

	return cs.ttw.WriteRow(context.TODO(), r)
}

// Close should release resources being held
func (cs *ConflictSink) Close() error {
	if cs.ttw != nil {
		if err := cs.ttw.Close(context.TODO()); err != nil {
			return err
		}

		cs.ttw = nil
		return nil
	} else {
		return errors.New("already closed")
	}
}
