package resultset

import "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"

// Container type to package columns with their source schemas.
type ColWithSchema struct {
	Col schema.Column
	Sch schema.Schema
}
