// Copyright 2023 Dolthub, Inc.
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

package cli

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/go-mysql-server/sql"
)

// Queryist is generic interface for executing queries.
type Queryist interface {
	Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error)
}

// LateBindQueryist is a function that will be called the first time Querist is needed for use. Input is a context which
// is appropriate for the call to comence. Output is a Queryist, a sql.Context, a closer function, and an error.
// The closer function is called when the Queryist is no longer needed, typically a defer right after getting it.
type LateBindQueryist func(ctx context.Context) (Queryist, *sql.Context, func(), error)

// CliContexct is used to pass top level command information down to subcommands.
type CliContext interface {
	// GlobalArgs returns the arguments passed before the subcommand.
	GlobalArgs() *argparser.ArgParseResults
	QueryEngine(ctx context.Context) (Queryist, *sql.Context, func(), error)
}

type LateBindCliContext struct {
	globalArgs *argparser.ArgParseResults
	queryist   Queryist
	sqlCtx     *sql.Context

	bind LateBindQueryist
}

// GlobalArgs returns the arguments passed before the subcommand.
func (lbc LateBindCliContext) GlobalArgs() *argparser.ArgParseResults {
	return lbc.globalArgs
}

// QueryEngine returns a Queryist, a sql.Context, a closer function, and an error. It ensures that only one call to the
// LateBindQueryist is made, and caches the result.
func (lbc LateBindCliContext) QueryEngine(ctx context.Context) (Queryist, *sql.Context, func(), error) {
	if lbc.queryist != nil {
		return lbc.queryist, lbc.sqlCtx, nil, nil
	}

	qryist, sqlCtx, closer, err := lbc.bind(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	lbc.queryist = qryist
	lbc.sqlCtx = sqlCtx

	return qryist, sqlCtx, closer, nil
}

var _ CliContext = LateBindCliContext{}

func BuildCliContext(args *argparser.ArgParseResults, latebind LateBindQueryist) (CliContext, errhand.VerboseError) {
	if args == nil || latebind == nil {
		return nil, errhand.VerboseErrorFromError(fmt.Errorf("Invariants violated.  args and latebind must be non nil."))
	}

	return LateBindCliContext{globalArgs: args, bind: latebind}, nil
}
