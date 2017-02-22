package graphql

import (
	"errors"
	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/language/ast"
)

func NewLocatedError(err interface{}, nodes []ast.Node) *gqlerrors.Error {
	var origError error
	message := "An unknown error occurred."
	if err, ok := err.(error); ok {
		message = err.Error()
		origError = err
	}
	if err, ok := err.(string); ok {
		message = err
		origError = errors.New(err)
	}
	stack := message
	return gqlerrors.NewError(
		message,
		nodes,
		stack,
		nil,
		[]int{},
		origError,
	)
}

func FieldASTsToNodeASTs(fieldASTs []*ast.Field) []ast.Node {
	nodes := []ast.Node{}
	for _, fieldAST := range fieldASTs {
		nodes = append(nodes, fieldAST)
	}
	return nodes
}
