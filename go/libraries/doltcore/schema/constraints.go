package schema

// ConstraintType is a type of constraint you are forcing on a schema.
type ConstraintType string;

// ConstraintFromString converts a string to a constraint.
func ConstraintFromString(str string) ConstraintType {
	switch str {
	case string(PrimaryKey):
		return PrimaryKey
	}

	return Invalid
}

// String gives the readable string representation of the constraint type.
func (c ConstraintType) String() string{
	return string(c)
}

const (
	// Primary Key constraints define a single field that all rows will be keyed off of.  PrimaryKey constraints are
	// required on rows that are being written to nbf files, or dolt tables in noms.
	PrimaryKey ConstraintType = "primary_key"

	// Invalid represents an invalid constraint usually when the wrong string name for a constraint was used.
	Invalid    ConstraintType = "invalid"
)

// ConstraintTypes is a slice containing all the valid values of ConstraintType.
var ConstraintTypes = []ConstraintType{PrimaryKey}

// Constraint is made up of a Constraint type, and a list of fields being referenced by the constraint.
type Constraint struct {
	ct ConstraintType
	fieldIndices []int
}

// NewConstraint creates a Constraint instance after validating the parameters.
func NewConstraint(ct ConstraintType, fldInds []int) *Constraint {
	if ct == Invalid {
		panic("Invalid constraint type")
	} else if len(fldInds) == 0 {
		panic("Constraint on no fields not allowed.")
	}

	switch ct {
	case PrimaryKey:
		if len(fldInds) != 1 {
			panic("Primary key must be on a single column.")
		}
	}

	return &Constraint{ct, fldInds}
}

// ConType gets the ConstraintType for a Constraint.
func (c *Constraint) ConType() ConstraintType{
	return c.ct
}

// FieldIndices gets a slice of field indices referenced by the constraint.
func (c *Constraint) FieldIndices() []int {
	return c.fieldIndices
}