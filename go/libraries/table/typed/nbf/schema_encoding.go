package nbf

import (
	"encoding/binary"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/iohelp"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"io"
)

// ReadBinarySchema reads a schema.Schema from an io.Reader
func ReadBinarySchema(r io.Reader) (*schema.Schema, error) {
	epr := &iohelp.ErrPreservingReader{r, nil}

	var numFields uint16
	err := binary.Read(epr, binary.BigEndian, &numFields)

	if err == nil {
		fields := make([]*schema.Field, numFields)
		for i := uint16(0); i < numFields && err == nil; i++ {
			fld, err := readField(epr)

			if err != nil {
				return nil, epr.Err
			}

			fields[i] = fld
		}

		sch := schema.NewSchema(fields)

		var numConstraints uint16
		err = binary.Read(epr, binary.BigEndian, &numConstraints)

		if err == nil {
			for i := uint16(0); i < numConstraints && err == nil; i++ {
				cnst, err := readConstraint(epr)

				if err != nil {
					return nil, epr.Err
				}

				sch.AddConstraint(cnst)
			}

			return sch, nil
		}
	}

	return nil, epr.Err
}

func readField(r *iohelp.ErrPreservingReader) (*schema.Field, error) {
	var size uint8
	err := binary.Read(r, binary.BigEndian, &size)
	if err == nil {
		nameBytes := make([]byte, size)
		_, err = r.Read(nameBytes)

		var kind types.NomsKind
		var required bool
		err = binary.Read(r, binary.BigEndian, &kind)
		err = binary.Read(r, binary.BigEndian, &required)

		if err == nil {
			f := schema.NewField(string(nameBytes), kind, required)
			return f, nil
		}
	}

	return nil, err
}

func readConstraint(r *iohelp.ErrPreservingReader) (*schema.Constraint, error) {
	var size uint8
	var err error

	if err = binary.Read(r, binary.BigEndian, &size); err == nil {
		var cTypeBytes []byte
		cTypeBytes, err = iohelp.ReadNBytes(r, int(size))

		if err = binary.Read(r, binary.BigEndian, &size); err == nil {
			var fldIndexBytes []byte
			fldIndexBytes, err = iohelp.ReadNBytes(r, int(size))

			if err == nil {
				fldIndices := make([]int, len(fldIndexBytes))
				for i := 0; i < len(fldIndexBytes); i++ {
					fldIndices[i] = int(fldIndexBytes[i])
				}

				return schema.NewConstraint(schema.ConstraintType(cTypeBytes), fldIndices), nil
			}
		}
	}

	return nil, err

}

// WriteBinarySchema writes a schema.Schema to an io.Writer
func WriteBinarySchema(sch *schema.Schema, w io.Writer) error {
	err := binary.Write(w, binary.BigEndian, uint16(sch.NumFields()))

	if err == nil {
		for i := 0; i < sch.NumFields(); i++ {
			f := sch.GetField(i)
			err = writeCol(f, w)

			if err != nil {
				return err
			}
		}

		err = binary.Write(w, binary.BigEndian, uint16(sch.TotalNumConstraints()))

		if err == nil {
			for i := 0; i < sch.TotalNumConstraints(); i++ {
				constraint := sch.GetConstraint(i)
				err = writeConstraint(constraint, w)

				if err != nil {
					return err
				}
			}
		}
	}

	return err
}

func writeCol(f *schema.Field, w io.Writer) error {
	name := f.NameStr()
	nameSize := uint8(len(name))
	err := iohelp.WritePrimIfNoErr(w, nameSize, nil)
	err = iohelp.WriteIfNoErr(w, []byte(name), err)
	err = iohelp.WritePrimIfNoErr(w, f.NomsKind(), err)
	return iohelp.WritePrimIfNoErr(w, f.IsRequired(), err)
}

func writeConstraint(constraint *schema.Constraint, w io.Writer) error {
	cTypeStr := string(constraint.ConType())
	cTypeStrLen := uint8(len(cTypeStr))
	err := iohelp.WritePrimIfNoErr(w, cTypeStrLen, nil)
	err = iohelp.WriteIfNoErr(w, []byte(cTypeStr), err)

	fldIndices := constraint.FieldIndices()
	indices := make([]byte, 0, len(fldIndices))
	for _, idx := range fldIndices {
		indices = append(indices, byte(idx))
	}

	err = iohelp.WritePrimIfNoErr(w, uint8(len(indices)), err)
	err = iohelp.WriteIfNoErr(w, indices, err)

	return err
}
