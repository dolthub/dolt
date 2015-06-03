package gen

import (
	"github.com/clipperhouse/typewriter"
)

func init() {
	templates = append(templates, &typewriter.Template{
		Name: "noms",
		Text: `
func (self {{.Name}}) Equals(other Value) bool {
	if other, ok := other.({{.Name}}); ok {
		return self == other
	} else {
		return false
	}
}
`,
		TypeConstraint: typewriter.Constraint{Comparable: true},
	})
}
