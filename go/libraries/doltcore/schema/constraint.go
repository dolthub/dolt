package schema

import "github.com/attic-labs/noms/go/types"

type ColConstraint interface {
	SatisfiesConstraint(value types.Value) bool
	GetConstraintType() string
	GetConstraintParams() map[string]string
}

const (
	NotNullConstraintType = "not_null"
)

func ColConstraintFromTypeAndParams(colCnstType string, params map[string]string) ColConstraint {
	switch colCnstType {
	case NotNullConstraintType:
		return NotNullConstraint{}
	}
	panic("Unknown column constraint type: " + colCnstType)
}

type NotNullConstraint struct{}

func (nnc NotNullConstraint) SatisfiesConstraint(value types.Value) bool {
	return !types.IsNull(value)
}

func (nnc NotNullConstraint) GetConstraintType() string {
	return NotNullConstraintType
}

func (nnc NotNullConstraint) GetConstraintParams() map[string]string {
	return nil
}

func ColConstraintsAreEqual(a, b []ColConstraint) bool {
	if len(a) != len(b) {
		return false
	} else if len(a) == 0 {
		return true
	}

	// kinda shitty.  Probably shouldn't require order to be identital
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]

		if ca.GetConstraintType() != cb.GetConstraintType() {
			return false
		} else {
			pa := ca.GetConstraintParams()
			pb := cb.GetConstraintParams()

			if len(pa) != len(pb) {
				return false
			} else if len(pa) != 0 {
				for k, va := range pa {
					vb, ok := pb[k]

					if !ok {
						return false
					} else if va != vb {
						return false
					}
				}
			}
		}
	}

	return true
}
