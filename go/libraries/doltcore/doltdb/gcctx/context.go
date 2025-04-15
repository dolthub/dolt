// Copyright 2025 Dolthub, Inc.
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

package gcctx

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/utils/valctx"
	"github.com/dolthub/dolt/go/store/hash"
)

type ctxKey int

var safepointControllerkey ctxKey

type ctxState struct {
	controller *GCSafepointController
}

// Creates a |Context| that registers GC safepoint lifecycle events
// with the given GCSafepointController.
//
// The lifecycle events themselves are done through the functions
// |SessionEnd|, |SessionCommandBegin| and |SessionCommandEnd| in this
// package.
//
// Sessions registered with the safepoint controller this way
// currently do not have a way to have their GC roots visited. As a
// consequence, they cannot hold database state in memory outside of
// lifecycle events. This is still useful for accessing doltdb.DoltDB
// data from things like background threads and interactings with the
// GC safepoint mechanism. All uses which occur from within a proper
// SQL context should instead of sql.Session{End,Command{Begin,End}}
// on the *DoltSession.
func WithGCSafepointController(ctx context.Context, controller *GCSafepointController) context.Context {
	state := &ctxState{
		controller: controller,
	}
	ret := context.WithValue(ctx, safepointControllerkey, state)
	ret = valctx.WithContextValidation(ret)
	valctx.SetContextValidation(ret, state.Validate)
	return ret
}

func SessionEnd(ctx context.Context) {
	state := ctx.Value(safepointControllerkey).(*ctxState)
	state.controller.SessionEnd(state)
}

func SessionCommandBegin(ctx context.Context, cancel context.CancelFunc) {
	state := ctx.Value(safepointControllerkey).(*ctxState)
	state.controller.SessionCommandBegin(state)
}

func SessionCommandEnd(ctx context.Context) {
	state := ctx.Value(safepointControllerkey).(*ctxState)
	state.controller.SessionCommandEnd(state)
}

func GetGCSafepointController(ctx context.Context) *GCSafepointController {
	if v := ctx.Value(safepointControllerkey); v != nil {
		return v.(*ctxState).controller
	}
	return nil
}

func GetValidate(ctx context.Context) func() {
	return ctx.Value(safepointControllerkey).(*ctxState).Validate
}

func (*ctxState) VisitGCRoots(context.Context, string, func(hash.Hash) bool) error {
	return nil
}

func (s *ctxState) Validate() {
	s.controller.Validate(s)
}
