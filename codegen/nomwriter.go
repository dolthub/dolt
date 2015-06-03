package gen

import (
	"io"

	. "github.com/attic-labs/noms/dbg"
	"github.com/clipperhouse/typewriter"
)

var (
	templates = typewriter.TemplateSlice{}
)

func init() {
	Chk.NoError(typewriter.Register(&nomWriter{}))
}

type nomWriter struct{}

func (nw *nomWriter) Name() string {
	return "noms"
}

func (nw *nomWriter) Imports(t typewriter.Type) []typewriter.ImportSpec {
	return []typewriter.ImportSpec{}
}

func (nw *nomWriter) Write(w io.Writer, typ typewriter.Type) error {
	tag, found := typ.FindTag(nw)

	if !found {
		return nil
	}

	tmpl, err := templates.ByTag(typ, tag)
	if err != nil {
		return err
	}
	if err := tmpl.Execute(w, typ); err != nil {
		return err
	}
	return nil
}
