package schema

import (
	"errors"
	"github.com/liquidata-inc/ld/dolt/go/libraries/set"
)

// Schema holds a list of columns which describe a table.
type Schema struct {
	fields            []*Field
	constraintsByType map[ConstraintType][]*Constraint
	contstraints      []*Constraint
	nameToIndex       map[string]int
}

// NewSchema creates a new instance of Schema from a slice of fields
func NewSchema(fields []*Field) *Schema {
	nameToIndex := make(map[string]int, len(fields))
	for i, f := range fields {
		nameToIndex[f.NameStr()] = i
	}

	return &Schema{fields, make(map[ConstraintType][]*Constraint), []*Constraint{}, nameToIndex}
}

// GetFieldNames returns a slice containing all the field names
func (sch *Schema) GetFieldNames() []string {
	fldNames := make([]string, len(sch.fields))
	for i, fld := range sch.fields {
		fldNames[i] = fld.NameStr()
	}

	return fldNames
}

// NumFields returns the total number of fields in the table.
func (sch *Schema) NumFields() int {
	return len(sch.fields)
}

// GetField provides access to all columns by index. Iterating from 0 to NumFields() will
// give you all columns in the Schema
func (sch *Schema) GetField(index int) *Field {
	return sch.fields[index]
}

// GetFieldIndex gets a fields index by name.
func (sch *Schema) GetFieldIndex(fieldName string) int {
	if index, ok := sch.nameToIndex[fieldName]; ok {
		return index
	}

	return -1
}

func (sch *Schema) CopyWithoutConstraints() *Schema {
	return &Schema{sch.fields, make(map[ConstraintType][]*Constraint), []*Constraint{}, sch.nameToIndex}
}

// Equals compares each column in the schema versus every column in another schema.
func (sch *Schema) Equals(other *Schema) bool {
	if sch.NumFields() != other.NumFields() {
		return false
	}

	for i := 0; i < sch.NumFields(); i++ {
		f1 := sch.GetField(i)
		f2 := other.GetField(i)

		if !f1.Equals(f2) {
			return false
		}
	}

	for _, ct := range ConstraintTypes {
		cOfType1 := sch.constraintsByType[ct]
		cOfType2 := other.constraintsByType[ct]

		if len(cOfType1) != len(cOfType2) {
			return false
		}

		for i := 0; i < len(cOfType1); i++ {
			c1 := cOfType1[i]
			c2 := cOfType2[i]

			if len(c1.fieldIndices) != len(c2.fieldIndices) {
				return false
			}

			for j := 0; j < len(c1.fieldIndices); j++ {
				if c1.fieldIndices[j] != c2.fieldIndices[j] {
					return false
				}
			}
		}
	}

	return true
}

func (sch *Schema) TotalNumConstraints() int {
	return len(sch.contstraints)
}

// NumConstraintsOfType returns the number of constraints a schema has for a given ConstraintType
func (sch *Schema) NumConstraintsOfType(cType ConstraintType) int {
	constraints, _ := sch.constraintsByType[cType]
	return len(constraints)
}

// GetConstraint gets a constraint by index
func (sch *Schema) GetConstraint(n int) *Constraint {
	if n < 0 {
		panic("Index < 0")
	}

	return sch.contstraints[n]
}

// GetConstraintByType returns the nth constraint of a given ConstraintType
func (sch *Schema) GetConstraintByType(cType ConstraintType, n int) (*Constraint, bool) {
	if n < 0 {
		panic("Index < 0")
	}

	constraints, ok := sch.constraintsByType[cType]

	if !ok || n >= len(constraints) {
		return nil, false
	}

	return constraints[n], true
}

// AddConstraint adds a constraint to the schema.
func (sch *Schema) AddConstraint(c *Constraint) error {
	cType := c.ConType()
	if cType == Invalid {
		panic("Can't add invalid constraints")
	}

	sch.contstraints = append(sch.contstraints, c)

	constraints, _ := sch.constraintsByType[cType]
	if cType == PrimaryKey && len(constraints) != 0 {
		return errors.New("Schema already has a primary key")
	}

	constraints = append(constraints, c)
	sch.constraintsByType[cType] = constraints

	return nil
}

// GetPKIndex returns the index of the field that is the primary key.  If their is no PrimaryKey constraint for the
// schema then -1 will be returned.
func (sch *Schema) GetPKIndex() int {
	if c, ok := sch.GetConstraintByType(PrimaryKey, 0); ok {
		return c.FieldIndices()[0]
	}

	return -1
}

// IterConstraints iterates over each constraint making a callback for each constraint until all constraints are
// exhausted or until the callback function returns stop = true.
func (sch *Schema) IterConstraints(cb func(*Constraint) (stop bool)) {
	for _, constraints := range sch.constraintsByType {
		for _, constraint := range constraints {
			stop := cb(constraint)

			if stop {
				break
			}
		}
	}
}

// IterFields iterates over each field making a callback for each field until all fields are exhausted or until the
// callback function returns stop = true
func (sch *Schema) IterFields(cb func(*Field) (stop bool)) {
	for _, field := range sch.fields {
		stop := cb(field)

		if stop {
			break
		}
	}
}

// IntersectFields takes a slice of field names and checks them against the field names in the schema and returns
// 1 A slice of fields that are ony in the schema, 2 A slice of fields in both the schema and the supplied fields slice
// 3 The slice of fields that are only in the supplied fields slice.
func (sch *Schema) IntersectFields(fields []string) (schemaOnly []string, inBoth []string, fieldsOnly []string) {
	for _, fName := range fields {
		_, ok := sch.nameToIndex[fName]

		if ok {
			inBoth = append(inBoth, fName)
		} else {
			fieldsOnly = append(fieldsOnly, fName)
		}
	}

	fieldSet := set.NewStrSet(fields)
	for k := range sch.nameToIndex {
		if !fieldSet.Contains(k) {
			schemaOnly = append(schemaOnly, k)
		}
	}

	return schemaOnly, inBoth, fieldsOnly
}
