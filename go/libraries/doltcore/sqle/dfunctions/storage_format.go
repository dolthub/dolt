// Copyright 2022 Dolthub, Inc.
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

package dfunctions

import (
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/types"
)

const StorageFormatFuncName = "dolt_storage_format"

type StorageFormat struct{}

// NewStorageFormat creates a new StorageFormat expression.
func NewStorageFormat() sql.Expression {
	return &StorageFormat{}
}

// Children implements the Expression interface.
func (*StorageFormat) Children() []sql.Expression {
	return nil
}

// Eval implements the Expression interface.
func (*StorageFormat) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	sesh := dsess.DSessFromSess(ctx.Session)
	if sesh.GetCurrentDatabase() == "" {
		return nil, sql.ErrNoDatabaseSelected.New()
	}

	dbName := sesh.GetCurrentDatabase()
	ddb, ok := sesh.GetDoltDB(ctx, dbName)
	if !ok {
		return nil, errors.New("failed to load underlying dolt db")
	}
	f := GetStorageFormatDisplayString(ddb.Format())

	return f, nil
}

func GetStorageFormatDisplayString(format *types.NomsBinFormat) string {
	if types.IsFormat_DOLT(format) {
		return fmt.Sprintf("NEW ( %s )", format.VersionString())
	} else {
		return fmt.Sprintf("OLD ( %s )", format.VersionString())
	}
}

// IsNullable implements the Expression interface.
func (*StorageFormat) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (*StorageFormat) Resolved() bool {
	return true
}

// String implements the Stringer interface.
func (*StorageFormat) String() string {
	return "DOLT_STORAGE_FORMAT"
}

// Type implements the Expression interface.
func (*StorageFormat) Type() sql.Type {
	return gmstypes.Text
}

// WithChildren implements the Expression interface.
func (v *StorageFormat) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(v, len(children), 0)
	}
	return NewVersion(), nil
}
