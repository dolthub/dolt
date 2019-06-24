package sqltestutil

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

// This file defines test queries and expected results. The purpose of defining them here is to make them portable --
// usable in multiple contexts as we implement SQL support.

// Structure for a test of a select query
type SelectTest struct {
	name           string
	query          string
	expectedRows   []row.Row
	expectedSchema schema.Schema
	expectedErr    string
}


