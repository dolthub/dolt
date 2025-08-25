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
	"errors"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

type LateBindQueryistResult struct {
	Queryist Queryist
	Context  *sql.Context
	IsRemote bool
	Closer   func()
}

type LateBindQueryistConfig struct {
	EnableAutoGC bool
}

type LateBindQueryistOption func(*LateBindQueryistConfig)

// LateBindQueryist is a function that will be called the first time Queryist is needed for use. Input is a context which
// is appropriate for the call to commence. Output is a LateBindQueryistResult, which includes a Queryist, a sql.Context, and
// a closer function. It can also result in an error.
//
// The Closer function should be called when the Queryist is no longer needed. If the result is cached and returned to
// multiple callers, it should be called after the cached result itself is no longer needed.
//
// A LateBindqueryistResult includes enough information for a caller to know if it is connecting to a remote Dolt instance
// or if it running the SqlEngine locally in-process. The CliContext uses this, in addition to its own local state, to
// let a caller know if they are connected to a remote and if this is the first QueryEngine fetch of the process lifecycle.
// This is reflected in |IsRemote| and, in the case of QueryEngineResult, |IsFirstUse|.
//
// This state is useful for determining whether a command making use of the CliContext is being run within the context of
// another command. This is particularly interesting when running a \checkout in a dolt sql session. It makes sense to do
// so in the context of `dolt sql`, but not in the context of `dolt checkout` when connected to a remote server.
type LateBindQueryist func(ctx context.Context, opts ...LateBindQueryistOption) (LateBindQueryistResult, error)

// CliContexct is used to pass top level command information down to subcommands.
type CliContext interface {
	// GlobalArgs returns the arguments passed before the subcommand.
	GlobalArgs() *argparser.ArgParseResults
	WorkingDir() filesys.Filesys
	Config() *env.DoltCliConfig
	QueryEngine(ctx context.Context, opts ...LateBindQueryistOption) (QueryEngineResult, error)
	// Release resources associated with the CliContext, including
	// any QueryEngines which were provisioned over the lifetime
	// of the CliContext.
	Close()
}

// NewCliContext creates a new CliContext instance. Arguments must not be nil.
func NewCliContext(args *argparser.ArgParseResults, config *env.DoltCliConfig, cwd filesys.Filesys, latebind LateBindQueryist) (CliContext, errhand.VerboseError) {
	if args == nil || config == nil || cwd == nil || latebind == nil {
		return nil, errhand.VerboseErrorFromError(errors.New("Invariant violated. args, config, cwd, and latebind must be non nil."))
	}

	return LateBindCliContext{
		globalArgs:    args,
		config:        config,
		cwd:           cwd,
		activeContext: &QueryistContext{},
		bind:          latebind,
	}, nil
}

type QueryistContext struct {
	sqlCtx   *sql.Context
	qryist   *Queryist
	isRemote bool
	close    func()
}

// LateBindCliContext is a struct that implements CliContext. Its primary purpose is to wrap the global arguments and
// provide an implementation of the QueryEngine function. This instance is stateful to ensure that the Queryist is only
// created once.
type LateBindCliContext struct {
	globalArgs    *argparser.ArgParseResults
	cwd           filesys.Filesys
	config        *env.DoltCliConfig
	activeContext *QueryistContext

	bind LateBindQueryist
}

type QueryEngineResult struct {
	Queryist Queryist
	Context  *sql.Context
	// |true| if this is the first time the CliContext is returning a QueryEngineResult.
	// Otherwise it will be |false|, which means this CliContext has already been used
	// to retrieve a Queryist, and the Queryist coming back is the cached result.
	IsFirstResult bool
	IsRemote      bool
}

// GlobalArgs returns the arguments passed before the subcommand.
func (lbc LateBindCliContext) GlobalArgs() *argparser.ArgParseResults {
	return lbc.globalArgs
}

// QueryEngine returns a Queryist, a sql.Context, a closer function, and an error. It ensures that only one call to the
// LateBindQueryist is made, and caches the result. Note that if this is called twice, the closer function returns will
// be nil, callers should check if is nil.
func (lbc LateBindCliContext) QueryEngine(ctx context.Context, opts ...LateBindQueryistOption) (res QueryEngineResult, err error) {
	if lbc.activeContext != nil && lbc.activeContext.qryist != nil && lbc.activeContext.sqlCtx != nil {
		res.Queryist = *lbc.activeContext.qryist
		res.Context = lbc.activeContext.sqlCtx
		res.IsRemote = lbc.activeContext.isRemote
		// Returning a cached result.
		res.IsFirstResult = false
		return res, nil
	}

	bindRes, err := lbc.bind(ctx, opts...)
	if err != nil {
		return res, err
	}

	lbc.activeContext.qryist = &bindRes.Queryist
	lbc.activeContext.sqlCtx = bindRes.Context
	lbc.activeContext.close = bindRes.Closer
	lbc.activeContext.isRemote = bindRes.IsRemote

	res.Queryist = bindRes.Queryist
	res.Context = bindRes.Context
	res.IsRemote = bindRes.IsRemote
	res.IsFirstResult = true
	return res, nil
}

func (lbc LateBindCliContext) Close() {
	if lbc.activeContext != nil && lbc.activeContext.close != nil {
		lbc.activeContext.close()
	}
}

func (lbc LateBindCliContext) WorkingDir() filesys.Filesys {
	return lbc.cwd
}

// Config returns the dolt config stored in CliContext
func (lbc LateBindCliContext) Config() *env.DoltCliConfig {
	return lbc.config
}

var _ CliContext = LateBindCliContext{}
