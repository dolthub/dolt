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
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/funcitr"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
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
}

type MoveOptions struct {
	Operation   MoveOperation
	ContOnErr   bool
	SchFile     string
	TableName   string
	MappingFile string
	PrimaryKey  string
	Src         DataLocation
	Dest        DataLocation
	SrcOptions  interface{}
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
	transforms := pipeline.NewTransformCollection()

	rd, srcIsSorted, err := mvOpts.Src.NewReader(ctx, root, fs, mvOpts.SchFile, mvOpts.SrcOptions)

	if err != nil {
		return nil, &DataMoverCreationError{CreateReaderErr, err}
	}

	defer func() {
		if rd != nil {
			rd.Close(ctx)
		}
	}()

	outSch, err := getOutSchema(ctx, rd.GetSchema(), root, fs, mvOpts)

	if err != nil {
		if strings.Contains(err.Error(), "invalid noms kind") {
			return nil, &DataMoverCreationError{NomsKindSchemaErr, err}
		}
		return nil, &DataMoverCreationError{SchemaErr, err}
	}

	var mapping *rowconv.FieldMapping
	if mvOpts.MappingFile != "" {
		mapping, err = rowconv.MappingFromFile(mvOpts.MappingFile, fs, rd.GetSchema(), outSch)
	} else if mapByTag(mvOpts.Src, mvOpts.Dest) {
		mapping, err = rowconv.TagMapping(rd.GetSchema(), outSch)
	} else {
		mapping, err = rowconv.NameMapping(rd.GetSchema(), outSch)
	}

	if err != nil {
		return nil, &DataMoverCreationError{MappingErr, err}
	}

	err = maybeMapFields(transforms, mapping)

	if err != nil {
		return nil, &DataMoverCreationError{CreateMapperErr, err}
	}

	var wr table.TableWriteCloser
	if mvOpts.Operation == OverwriteOp {
		wr, err = mvOpts.Dest.NewCreatingWriter(ctx, mvOpts, root, fs, srcIsSorted, outSch, statsCB)
	} else if mvOpts.Operation == ReplaceOp {
		wr, err = mvOpts.Dest.NewReplacingWriter(ctx, mvOpts, root, fs, srcIsSorted, outSch, statsCB)
	} else {
		wr, err = mvOpts.Dest.NewUpdatingWriter(ctx, mvOpts, root, fs, srcIsSorted, outSch, statsCB)
	}

	if err != nil {
		return nil, &DataMoverCreationError{CreateWriterErr, err}
	}

	imp := &DataMover{rd, transforms, wr, mvOpts.ContOnErr}
	rd = nil

	return imp, nil
}

func (imp *DataMover) Move(ctx context.Context) error {
	defer imp.Rd.Close(ctx)
	defer imp.Wr.Close(ctx)

	var rowErr error
	badRowCB := func(trf *pipeline.TransformRowFailure) (quit bool) {
		if !imp.ContOnErr {
			rowErr = trf
			return true
		}

		return false
	}

	p := pipeline.NewAsyncPipeline(
		pipeline.ProcFuncForReader(ctx, imp.Rd),
		pipeline.ProcFuncForWriter(ctx, imp.Wr),
		imp.Transforms,
		badRowCB)
	p.Start()

	err := p.Wait()

	if err != nil {
		return err
	}

	return rowErr
}

func maybeMapFields(transforms *pipeline.TransformCollection, mapping *rowconv.FieldMapping) error {
	rconv, err := rowconv.NewRowConverter(mapping)

	if err != nil {
		return err
	}

	if !rconv.IdentityConverter {
		nt := pipeline.NewNamedTransform("Mapping transform", rowconv.GetRowConvTransformFunc(rconv))
		transforms.AppendTransforms(nt)
	}

	return nil
}

func getOutSchema(ctx context.Context, inSch schema.Schema, root *doltdb.RootValue, fs filesys.ReadableFS, mvOpts *MoveOptions) (schema.Schema, error) {
	if mvOpts.Operation == UpdateOp || mvOpts.Operation == ReplaceOp {
		// Get schema from target

		rd, _, err := mvOpts.Dest.NewReader(ctx, root, fs, mvOpts.SchFile, mvOpts.SrcOptions)

		if err != nil {
			return nil, err
		}

		defer rd.Close(ctx)

		return rd.GetSchema(), nil
	} else {
		sch, err := schFromFileOrDefault(mvOpts.SchFile, fs, inSch)

		if err != nil {
			return nil, err
		}

		sch, err = addPrimaryKey(sch, mvOpts.PrimaryKey)

		if err != nil {
			return nil, err
		}

		return sch, nil
	}

}

func schFromFileOrDefault(path string, fs filesys.ReadableFS, defSch schema.Schema) (schema.Schema, error) {
	if path != "" {
		data, err := fs.ReadFile(path)

		if err != nil {
			return nil, err
		}

		return encoding.UnmarshalJson(string(data))
	} else {
		return defSch, nil
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
