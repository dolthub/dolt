package ast

import (
	"github.com/graphql-go/graphql/language/kinds"
)

// Argument implements Node
type Argument struct {
	Kind  string
	Loc   *Location
	Name  *Name
	Value Value
}

func NewArgument(arg *Argument) *Argument {
	if arg == nil {
		arg = &Argument{}
	}
	return &Argument{
		Kind:  kinds.Argument,
		Loc:   arg.Loc,
		Name:  arg.Name,
		Value: arg.Value,
	}
}

func (arg *Argument) GetKind() string {
	return arg.Kind
}

func (arg *Argument) GetLoc() *Location {
	return arg.Loc
}
