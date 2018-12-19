package mvdata

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema/jsonenc"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
)

type MoveOperation string

const (
	OverwriteOp MoveOperation = "overwrite"
	UpdateOp    MoveOperation = "update"
)

type MoveOptions struct {
	Operation   MoveOperation
	ContOnErr   bool
	SchFile     string
	MappingFile string
	PrimaryKey  string
	Src         *DataLocation
	Dest        *DataLocation
}

type DataMover struct {
	Rd         table.TableReadCloser
	Transforms []table.TransformFunc
	Wr         table.TableWriteCloser
	ContOnErr  bool
}

func NewDataMover(root *doltdb.RootValue, fs filesys.Filesys, mvOpts *MoveOptions) (*DataMover, errhand.VerboseError) {
	var rd table.TableReadCloser
	var err error
	var transforms []table.TransformFunc

	defer func() {
		if rd != nil {
			rd.Close()
		}
	}()

	rd, srcIsSorted, err := mvOpts.Src.CreateReader(root, fs)

	if err != nil {
		bdr := errhand.BuildDError("Error creating reader for %s.", mvOpts.Src.Path)
		bdr.AddDetails("When attempting to move data from %s to %s, could not open a reader.", mvOpts.Src.String(), mvOpts.Dest.String())
		return nil, bdr.AddCause(err).Build()
	}

	outSch, err := getOutSchema(rd.GetSchema(), root, fs, mvOpts)

	if err != nil {
		bdr := errhand.BuildDError("Error determining the output schema.")
		bdr.AddDetails("When attempting to move data from %s to %s, could not determine the output schema.", mvOpts.Src.String(), mvOpts.Dest.String())
		bdr.AddDetails(`Schema File: "%s"`, mvOpts.SchFile)
		bdr.AddDetails(`explicit pk: "%s"`, mvOpts.PrimaryKey)
		return nil, bdr.AddCause(err).Build()
	}

	var mapping *schema.FieldMapping
	if mvOpts.MappingFile != "" {
		mapping, err = schema.MappingFromFile(mvOpts.MappingFile, fs, rd.GetSchema(), outSch)
	} else {
		mapping, err = schema.NewInferredMapping(rd.GetSchema(), outSch)
	}

	if err != nil {
		bdr := errhand.BuildDError("Error determining the mapping from input fields to output fields.")
		bdr.AddDetails("When attempting to move data from %s to %s, determine the mapping from input fields t, output fields.", mvOpts.Src.String(), mvOpts.Dest.String())
		bdr.AddDetails(`Mapping File: "%s"`, mvOpts.MappingFile)
		return nil, bdr.AddCause(err).Build()
	}

	transforms, err = maybeMapFields(transforms, mapping)

	if err != nil {
		mappingJson, jmErr := json.Marshal(mapping.DestSch)

		bdr := errhand.BuildDError("Error creating input to output mapper.")
		details := fmt.Sprintf("When attempting to move data from %s to %s, could not create a mapper.", mvOpts.Src.String(), mvOpts.Dest.String())

		if jmErr == nil {
			details += "mapping: " + string(mappingJson)
		}

		bdr.AddDetails(details)

		return nil, bdr.AddCause(err).Build()
	}

	var wr table.TableWriteCloser
	if mvOpts.Operation == OverwriteOp {
		wr, err = mvOpts.Dest.CreateOverwritingDataWriter(root, fs, srcIsSorted, outSch)
	} else {
		wr, err = mvOpts.Dest.CreateUpdatingDataWriter(root, fs, srcIsSorted, outSch)
	}

	if err != nil {
		bdr := errhand.BuildDError("Error creating writer for %s.\n", mvOpts.Dest.Path)
		bdr.AddDetails("When attempting to move data from %s to %s, could not open a writer.", mvOpts.Src.String(), mvOpts.Dest.String())
		return nil, bdr.AddCause(err).Build()
	}

	wr, err = maybeSort(wr, outSch, srcIsSorted, mvOpts)

	if err != nil {
		bdr := errhand.BuildDError("Error creating sorting reader.")
		bdr.AddDetails("When attempting to move data from %s to %s, could not open create sorting reader.", mvOpts.Src.String(), mvOpts.Dest.String())
		return nil, bdr.AddCause(err).Build()
	}

	imp := &DataMover{rd, transforms, wr, mvOpts.ContOnErr}
	rd = nil

	return imp, nil
}

func (imp *DataMover) Move() error {
	defer imp.Rd.Close()
	defer imp.Wr.Close()

	var rowErr error
	badRowCB := func(transfName string, row *table.Row, errDetails string) (quit bool) {
		if !imp.ContOnErr {
			rowErr = errors.New(transfName + "failed. " + errDetails)
			return false
		}

		return true
	}

	pipeline := table.StartAsyncPipeline(imp.Rd, imp.Transforms, imp.Wr, badRowCB)
	err := pipeline.Wait()

	if err != nil {
		return err
	}

	return rowErr
}

func maybeMapFields(transforms []table.TransformFunc, mapping *schema.FieldMapping) ([]table.TransformFunc, error) {
	rconv, err := table.NewRowConverter(mapping)

	if err != nil {
		return nil, err
	}

	if !rconv.IdentityConverter {
		transformer := table.NewRowTransformer("Mapping transform", rconv.TransformRow)
		transforms = append(transforms, transformer)
	}

	return transforms, nil
}

func maybeSort(wr table.TableWriteCloser, outSch *schema.Schema, srcIsSorted bool, mvOpts *MoveOptions) (table.TableWriteCloser, error) {
	if !srcIsSorted && mvOpts.Dest.MustWriteSorted() {
		wr = table.NewSortingTableWriter(wr, outSch.GetPKIndex(), mvOpts.ContOnErr)
	}

	return wr, nil
}

func getOutSchema(inSch *schema.Schema, root *doltdb.RootValue, fs filesys.ReadableFS, mvOpts *MoveOptions) (*schema.Schema, error) {
	if mvOpts.Operation == UpdateOp {
		// Get schema from target
		rd, _, err := mvOpts.Dest.CreateReader(root, fs)

		if err != nil {
			return nil, err
		}

		defer rd.Close()

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

func schFromFileOrDefault(path string, fs filesys.ReadableFS, defSch *schema.Schema) (*schema.Schema, error) {
	if path != "" {
		data, err := fs.ReadFile(path)

		if err != nil {
			return nil, err
		}

		return jsonenc.SchemaFromJSON(data)
	} else {
		return defSch, nil
	}
}

func addPrimaryKey(sch *schema.Schema, explicitKey string) (*schema.Schema, error) {
	explicitKeyIdx := sch.GetFieldIndex(explicitKey)

	if explicitKey != "" {
		if explicitKeyIdx == -1 {
			return nil, fmt.Errorf("could not find a field named \"%s\" in the schema", explicitKey)
		} else {
			sch = sch.CopyWithoutConstraints()
			sch.AddConstraint(schema.NewConstraint(schema.PrimaryKey, []int{explicitKeyIdx}))
		}
	}

	return sch, nil
}
