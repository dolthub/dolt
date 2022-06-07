// Copyright 2020 Dolthub, Inc.
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

package enginetest

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func ValidateDatabase(ctx *sql.Context, db sqle.Database) (err error) {
	for _, stage := range validationStages {
		if err = stage(ctx, db); err != nil {
			return err
		}
	}
	return
}

type validator func(ctx *sql.Context, db sqle.Database) error

var validationStages = []validator{
	sanityCheck,
}

func sanityCheck(ctx *sql.Context, db sqle.Database) error {
	return nil
}
