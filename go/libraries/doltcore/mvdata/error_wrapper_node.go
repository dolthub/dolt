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

package mvdata

import (
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type errorHandlerFunc func(err error)

// ErrorHandler wraps a row iter and calls a handler function on each error.
type ErrorHandler struct {
	plan.UnaryNode
	errorHandlerFunc
}

var _ sql.Node = (*ErrorHandler)(nil)

// NewErrorHandlerNode returns a newly created ErrorHandler node.
func NewErrorHandlerNode(child sql.Node, errorHandler errorHandlerFunc) *ErrorHandler {
	return &ErrorHandler{plan.UnaryNode{Child: child}, errorHandler}
}

// String implements the sql.Node interface.
func (e *ErrorHandler) String() string {
	return fmt.Sprintf("ErrorHandler(%s)", e.Child.String())
}

// RowIter implements the sql.Node interface.
func (e *ErrorHandler) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	ri, err := e.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &errorHandlerIter{ri, e.errorHandlerFunc}, nil
}

// WithChildren implements the sql.Node interface.
func (e *ErrorHandler) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}

	return NewErrorHandlerNode(children[0], e.errorHandlerFunc), nil
}

// errorHandlerIter wraps a row iter and calls the handler on each subsequent Next call. Any error other an io.EOF
// is purposely dropped.
type errorHandlerIter struct {
	childIter sql.RowIter
	errorHandlerFunc
}

var _ sql.RowIter = (*errorHandlerIter)(nil)

// Next implements the sql.RowIter interface.
func (e errorHandlerIter) Next() (sql.Row, error) {
	row, err := e.childIter.Next()
	if err == io.EOF {
		return row, err
	}

	if err != nil {
		e.errorHandlerFunc(err)
	}

	// This is not typically a safe operation as if err != nil then row is typically equal to nil. This node
	// should only be used in the import path.
	return row, nil
}

// Close implements the sql.RowIter interface.
func (e errorHandlerIter) Close(context *sql.Context) error {
	return e.childIter.Close(context)
}
