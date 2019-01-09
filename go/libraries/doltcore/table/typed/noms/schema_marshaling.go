package noms

import (
	"errors"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

type nomsFieldData struct {
	// Name is the name of the field
	Name string `noms:"name"`

	// Kind is the type of the field.  See types/noms_kind.go in the liquidata fork for valid values
	Kind string `noms:"kind"`

	// Required tells whether all rows require this value to be considered valid
	Required bool `noms:"required"`
}

func newNomsFieldData(field *schema.Field) nomsFieldData {
	return nomsFieldData{field.NameStr(), field.KindString(), field.IsRequired()}
}

func (nfd nomsFieldData) toField() *schema.Field {
	return schema.NewField(nfd.Name, schema.LwrStrToKind[nfd.Kind], nfd.Required)
}

type nomsConstraint struct {
	Type   string `noms:"constraint_type"`
	Fields []int  `noms:"field_indices"`
}

func newNomsConstraint(constraint *schema.Constraint) nomsConstraint {
	return nomsConstraint{constraint.ConType().String(), constraint.FieldIndices()}
}

func (nc nomsConstraint) toConstraint() *schema.Constraint {
	return schema.NewConstraint(schema.ConstraintFromString(nc.Type), nc.Fields)
}

type schemaData struct {
	Fields      []nomsFieldData  `noms:"fields"`
	Constraints []nomsConstraint `noms:"constraints"`
}

// MarshalAsNomsValue takes a Schema and converts it to a types.Value
func MarshalAsNomsValue(vrw types.ValueReadWriter, sch *schema.Schema) (types.Value, error) {
	fieldData := make([]nomsFieldData, sch.NumFields())
	for i := 0; i < sch.NumFields(); i++ {
		schField := sch.GetField(i)
		fieldData[i] = newNomsFieldData(schField)
	}

	var constraintData []nomsConstraint
	sch.IterConstraints(func(constraint *schema.Constraint) (stop bool) {
		constraintData = append(constraintData, newNomsConstraint(constraint))
		return false
	})

	sd := schemaData{fieldData, constraintData}
	val, err := marshal.Marshal(vrw, sd)

	if err != nil {
		return types.EmptyStruct, err
	}

	if _, ok := val.(types.Struct); ok {
		return val, nil
	}

	return types.EmptyStruct, errors.New("Table Schema could not be converted to types.Struct")
}

// UnmarshalNomsValue takes a types.Value instance and Unmarshalls it into a Schema.
func UnmarshalNomsValue(schemaVal types.Value) (*schema.Schema, error) {
	var sd schemaData
	err := marshal.Unmarshal(schemaVal, &sd)

	if err != nil {
		return nil, err
	}

	fields := make([]*schema.Field, len(sd.Fields))
	for i, currData := range sd.Fields {
		fields[i] = currData.toField()
	}

	sch := schema.NewSchema(fields)

	for _, cd := range sd.Constraints {
		constraint := cd.toConstraint()
		err = sch.AddConstraint(constraint)

		if err != nil {
			return nil, err
		}
	}

	return sch, nil
}
