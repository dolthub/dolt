// Copyright 2021 Dolthub, Inc.
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

package fkconstrain

import (
	"errors"
	"fmt"
)

var ErrForeignKeyConstraintViolation = errors.New("foreign key constraint violation")

type ForeignKeyError struct {
	tableName           string
	referencedTableName string
	fkName              string
	keyStr              string
}

func (err *ForeignKeyError) Error() string {
	return fmt.Sprintf("Foreign key violation on fk: `%s`, table: `%s`, referenced table: `%s`, key: `%s`", err.fkName, err.tableName, err.referencedTableName, err.keyStr)
}

func (err *ForeignKeyError) Unwrap() error {
	return ErrForeignKeyConstraintViolation
}
