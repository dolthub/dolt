package gqlerrors

import (
	"fmt"

	"github.com/attic-labs/graphql/language/ast"
	"github.com/attic-labs/graphql/language/location"
	"github.com/attic-labs/graphql/language/source"
)

type Error struct {
	Message       string
	Stack         string
	Nodes         []ast.Node
	Source        *source.Source
	Positions     []int
	Locations     []location.SourceLocation
	OriginalError error
}

// implements Golang's built-in `error` interface
func (g Error) Error() string {
	return fmt.Sprintf("%v", g.Message)
}

func NewError(message string, nodes []ast.Node, stack string, source *source.Source, positions []int, origError error) *Error {
	if stack == "" && message != "" {
		stack = message
	}
	if source == nil {
		for _, node := range nodes {
			// get source from first node
			if node.GetLoc() != nil {
				source = node.GetLoc().Source
			}
			break
		}
	}
	if len(positions) == 0 && len(nodes) > 0 {
		for _, node := range nodes {
			if node.GetLoc() == nil {
				continue
			}
			positions = append(positions, node.GetLoc().Start)
		}
	}
	locations := []location.SourceLocation{}
	for _, pos := range positions {
		loc := location.GetLocation(source, pos)
		locations = append(locations, loc)
	}
	return &Error{
		Message:       message,
		Stack:         stack,
		Nodes:         nodes,
		Source:        source,
		Positions:     positions,
		Locations:     locations,
		OriginalError: origError,
	}
}
