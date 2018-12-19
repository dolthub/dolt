package jsonenc

import (
	"encoding/json"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/pkg/errors"
)

type jsonFieldData struct {
	// Name is the name of the field
	Name string `json:"name"`

	// Kind is the type of the field.  See types/noms_kind.go in the liquidata fork for valid values
	Kind string `json:"kind"`

	// Required tells whether all rows require this value to be considered valid
	Required bool `json:"required"`
}

func newJsonFieldData(field *schema.Field) jsonFieldData {
	return jsonFieldData{field.NameStr(), field.KindString(), field.IsRequired()}
}

func (jfd jsonFieldData) toField() *schema.Field {
	return schema.NewField(jfd.Name, schema.LwrStrToKind[jfd.Kind], jfd.Required)
}

type jsonConstraint struct {
	Type   string `json:"constraint_type"`
	Fields []int  `json:"field_indices"`
}

func newJsonConstraint(constraint *schema.Constraint) jsonConstraint {
	return jsonConstraint{constraint.ConType().String(), constraint.FieldIndices()}
}

func (jc jsonConstraint) toConstraint() *schema.Constraint {
	return schema.NewConstraint(schema.ConstraintFromString(jc.Type), jc.Fields)
}

func (jc jsonConstraint) isValid() bool {
	return schema.ConstraintFromString(jc.Type) != schema.Invalid
}

type schemaData struct {
	Fields      []jsonFieldData  `json:"fields"`
	Constraints []jsonConstraint `json:"constraints"`
}

// SchemaToJSON takes a Schema and JSON serializes it.
func SchemaToJSON(sch *schema.Schema) ([]byte, error) {
	fieldData := make([]jsonFieldData, sch.NumFields())
	for i := 0; i < sch.NumFields(); i++ {
		schField := sch.GetField(i)
		fieldData[i] = newJsonFieldData(schField)
	}

	var constraintData []jsonConstraint
	sch.IterConstraints(func(constraint *schema.Constraint) (stop bool) {
		constraintData = append(constraintData, newJsonConstraint(constraint))
		return false
	})

	sd := schemaData{fieldData, constraintData}
	return json.MarshalIndent(&sd, "", "  ")
}

// SchemaFromJson deserializes json stored in a byte slice as a Schema
func SchemaFromJSON(data []byte) (*schema.Schema, error) {
	var sd schemaData
	err := json.Unmarshal(data, &sd)

	if err != nil {
		return nil, err
	}

	fields := make([]*schema.Field, len(sd.Fields))
	for i, currData := range sd.Fields {
		fields[i] = currData.toField()
	}

	sch := schema.NewSchema(fields)

	for _, cd := range sd.Constraints {
		if !cd.isValid() {
			return sch, errors.New("Invalid constraint")
		}

		constraint := cd.toConstraint()
		err = sch.AddConstraint(constraint)

		if err != nil {
			return nil, err
		}
	}

	return sch, nil
}
