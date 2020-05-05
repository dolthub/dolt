// Copyright 2019 Liquidata, Inc.
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

package mvdata

import (
	"context"
	"errors"
	"fmt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"strings"
	"sync/atomic"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/funcitr"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type MoveOperation string

const (
	OverwriteOp MoveOperation = "overwrite"
	ReplaceOp   MoveOperation = "replace"
	UpdateOp    MoveOperation = "update"
	InvalidOp   MoveOperation = "invalid"
)

type CsvOptions struct {
	Delim string
}

type XlsxOptions struct {
	SheetName string
}

type JSONOptions struct {
	TableName string
	SchFile   string
}

type MoveOptions struct {
	Operation   MoveOperation
	TableName   string
	ContOnErr   bool
	Force       bool
	SchFile     string
	MappingFile string
	PrimaryKey  string
	Src         DataLocation
	Dest        DataLocation
	SrcOptions  interface{}
}

func (m MoveOptions) isImport() bool {
	_, fromFile := m.Src.(FileDataLocation)
	_, toTable := m.Dest.(TableDataLocation)
	return fromFile && toTable
}

func (m MoveOptions) isCopy() bool {
	_, fromTable := m.Src.(TableDataLocation)
	_, toTable := m.Dest.(TableDataLocation)
	return fromTable && toTable
}

func (m MoveOptions) isExport() bool {
	_, fromTable := m.Src.(TableDataLocation)
	_, toFile := m.Dest.(FileDataLocation)
	return fromTable && toFile
}

func (m MoveOptions) WillOverwrite() bool {
	if _, isStdOut := m.Dest.(StreamDataLocation); isStdOut {
		return false  // can't overwrite StdOut
	}
	return m.Operation == OverwriteOp && !m.Force
}

type DataMover struct {
	Rd         table.TableReadCloser
	Transforms *pipeline.TransformCollection
	Wr         table.TableWriteCloser
	ContOnErr  bool
}

type DataMoverCreationErrType string

const (
	CreateReaderErr   DataMoverCreationErrType = "Create reader error"
	NomsKindSchemaErr DataMoverCreationErrType = "Invalid schema error"
	SchemaErr         DataMoverCreationErrType = "Schema error"
	MappingErr        DataMoverCreationErrType = "Mapping error"
	ReplacingErr      DataMoverCreationErrType = "Replacing error"
	CreateMapperErr   DataMoverCreationErrType = "Mapper creation error"
	CreateWriterErr   DataMoverCreationErrType = "Create writer error"
	CreateSorterErr   DataMoverCreationErrType = "Create sorter error"
)

type DataMoverCreationError struct {
	ErrType DataMoverCreationErrType
	Cause   error
}

func (dmce *DataMoverCreationError) String() string {
	return string(dmce.ErrType) + ": " + dmce.Cause.Error()
}

func NewDataMover(ctx context.Context, root *doltdb.RootValue, fs filesys.Filesys, mvOpts *MoveOptions, statsCB noms.StatsCB) (*DataMover, *DataMoverCreationError) {
	var rd table.TableReadCloser
	var err error

	rd, srcIsSorted, err := mvOpts.Src.NewReader(ctx, root, fs, mvOpts.SrcOptions)

	if err != nil {
		return nil, &DataMoverCreationError{CreateReaderErr, err}
	}

	defer func() {
		if rd != nil {
			rd.Close(ctx)
		}
	}()

	inSch := rd.GetSchema()
	outSch, err := outSchemaFromInSchema(ctx, inSch, root, fs, mvOpts)

	if err != nil {
		return nil, &DataMoverCreationError{ SchemaErr, err}
	}

	transforms, dmce := maybeMapFields(inSch, outSch, fs, mvOpts)

	if dmce != nil {
		return nil, dmce
	}

	var wr table.TableWriteCloser
	switch mvOpts.Operation {
	case OverwriteOp:
		wr, err = mvOpts.Dest.NewCreatingWriter(ctx, mvOpts, root, fs, srcIsSorted, outSch, statsCB)
	case ReplaceOp:
		wr, err = mvOpts.Dest.NewReplacingWriter(ctx, mvOpts, root, fs, srcIsSorted, outSch, statsCB)
	case UpdateOp:
		wr, err = mvOpts.Dest.NewUpdatingWriter(ctx, mvOpts, root, fs, srcIsSorted, outSch, statsCB)
	default:
		err = errors.New("invalid move operation")
	}

	if err != nil {
		return nil, &DataMoverCreationError{CreateWriterErr, err}
	}

	imp := &DataMover{rd, transforms, wr, mvOpts.ContOnErr}
	rd = nil

	return imp, nil
}

// Move is the method that executes the pipeline which will move data from the pipeline's source DataLocation to it's
// dest DataLocation.  It returns the number of bad rows encountered during import, and an error.
func (imp *DataMover) Move(ctx context.Context) (badRowCount int64, err error) {
	defer imp.Rd.Close(ctx)
	defer imp.Wr.Close(ctx)

	var badCount int64
	var rowErr error
	badRowCB := func(trf *pipeline.TransformRowFailure) (quit bool) {
		if !imp.ContOnErr {
			rowErr = trf
			return true
		}

		atomic.AddInt64(&badCount, 1)
		return false
	}

	p := pipeline.NewAsyncPipeline(
		pipeline.ProcFuncForReader(ctx, imp.Rd),
		pipeline.ProcFuncForWriter(ctx, imp.Wr),
		imp.Transforms,
		badRowCB)
	p.Start()

	err = p.Wait()

	if err != nil {
		return 0, err
	}

	if rowErr != nil {
		return 0, rowErr
	}

	return badCount, nil
}

func maybeMapFields(inSch schema.Schema, outSch schema.Schema, fs filesys.Filesys, mvOpts *MoveOptions) (trans *pipeline.TransformCollection, dmce *DataMoverCreationError) {
	var mapping *rowconv.FieldMapping
	var err error
	if mvOpts.MappingFile != "" {
		mapping, err = rowconv.MappingFromFile(mvOpts.MappingFile, fs, inSch, outSch)
	} else {
		mapping, err = rowconv.NameMapping(inSch, outSch)
	}

	if err != nil {
		return nil, &DataMoverCreationError{MappingErr, err}
	}

	if mvOpts.Operation == ReplaceOp || mvOpts.Operation == UpdateOp {
		if !mapping.MapsAllPKs() {
			err = fmt.Errorf("input primary keys do not match primary keys of existing table")
			return nil, &DataMoverCreationError{MappingErr, err}
		}
	}

	rconv, err := rowconv.NewImportRowConverter(mapping)

	if err != nil {
		return nil, &DataMoverCreationError{MappingErr, err}
	}

	transforms := pipeline.NewTransformCollection()
	if !rconv.IdentityConverter {
		nt := pipeline.NewNamedTransform("Mapping transform", rowconv.GetRowConvTransformFunc(rconv))
		transforms.AppendTransforms(nt)
	}

	return trans, nil
}

func outSchemaFromInSchema(ctx context.Context, inSch schema.Schema, root *doltdb.RootValue, fs filesys.ReadableFS, mvOpts *MoveOptions) (schema.Schema, error) {
	var outSch schema.Schema
	var err error

	if mvOpts.isExport()  {
		outSch = inSch
		return outSch, nil
	}

	if mvOpts.Operation == UpdateOp || mvOpts.Operation == ReplaceOp {
		t, ok := mvOpts.Dest.(TableDataLocation)
		if !ok {
			return nil, fmt.Errorf("%s and %s operations must be performed on a table", UpdateOp, ReplaceOp)
		}
		rd, _, err := t.NewReader(ctx, root, fs, nil)
		if err != nil {
			return nil, err
		}
		defer rd.Close(ctx)
		return rd.GetSchema(), nil
	}

	if mvOpts.SchFile != "" {
		var tn string
		tn, outSch, err = schAndTableNameFromFile(ctx, mvOpts.SchFile, fs, root)
		if err != nil {
			return nil, err
		}
		if tn != mvOpts.TableName {
			return nil, fmt.Errorf("table name '%s' from schema file %s does not match table arg '%s'", tn, mvOpts.SchFile, mvOpts.TableName)
		}
	} else {
		outSch, err = addPrimaryKey(inSch, mvOpts.PrimaryKey)
		if err != nil {
			return nil, err
		}
		outSch, err = MakeTagsUnique(ctx, root, mvOpts.TableName, outSch)
		if err != nil {
			return nil, err
		}
	}

	return outSch, nil
}

func schAndTableNameFromFile(ctx context.Context, path string, fs filesys.ReadableFS, root *doltdb.RootValue) (string, schema.Schema, error) {
	if path != "" {
		data, err := fs.ReadFile(path)

		if err != nil {
			return "", nil, err
		}

		return sqle.ParseCreateTableStatement(ctx, root, string(data))
	} else {
		return "", nil, errors.New("no schema file to parse")
	}
}

func addPrimaryKey(sch schema.Schema, explicitKey string) (schema.Schema, error) {
	if explicitKey != "" {
		keyCols := strings.Split(explicitKey, ",")
		trimmedCols := funcitr.MapStrings(keyCols, func(s string) string { return strings.TrimSpace(s) })
		keyColSet := set.NewStrSet(trimmedCols)

		foundPKCols := 0
		var updatedCols []schema.Column

		err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			if keyColSet.Contains(col.Name) {
				foundPKCols++
				col.IsPartOfPK = true
				col.Constraints = []schema.ColConstraint{schema.NotNullConstraint{}}
			} else {
				col.IsPartOfPK = false
				col.Constraints = nil
			}

			updatedCols = append(updatedCols, col)
			return false, nil
		})

		if err != nil {
			return nil, err
		}

		if keyColSet.Size() != foundPKCols {
			return nil, errors.New("could not find all pks: " + explicitKey)
		}

		updatedColColl, err := schema.NewColCollection(updatedCols...)

		if err != nil {
			return nil, err
		}

		return schema.SchemaFromCols(updatedColColl), nil
	}

	return sch, nil
}

func MakeTagsUnique(ctx context.Context, root *doltdb.RootValue, tblName string, sch schema.Schema) (schema.Schema, error) {
	var colNames []string
	var colKinds []types.NomsKind
	_ = sch.GetAllCols().Iter(func(_ uint64, col schema.Column) (stop bool, err error) {
		colNames = append(colNames, col.Name)
		colKinds = append(colKinds, col.Kind)
		return false, nil
	})

	tt, err := root.GenerateTagsForNewColumns(ctx, tblName, colNames, colKinds)

	if err != nil {
		return nil, err
	}

	cc, _ := schema.NewColCollection()
	for i, tag := range sch.GetAllCols().Tags {
		col, _ := sch.GetAllCols().GetByTag(tag)
		col.Tag = tt[i]
		cc, err = cc.Append(col)
		if err != nil {
			return nil, err
		}
	}
	return schema.SchemaFromCols(cc), nil
}
