package main

import (
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"text/template"

	"github.com/attic-labs/noms/d"
)

var (
	headerTempl    = readTemplate("header.tmpl")
	primitiveTempl = readTemplate("primitive.tmpl")
)

func main() {
	types := []string{"Bool", "Int16", "Int32", "Int64", "UInt16", "UInt32", "UInt64", "Float32", "Float64"}

	f, err := os.OpenFile("primitives.go", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatal(err)
		return
	}

	headerTempl.Execute(f, nil)

	for _, t := range types {
		goType := strings.ToLower(t)
		primitiveTempl.Execute(f, struct {
			NomsType string
			GoType   string
		}{t, goType})
	}
}

func readTemplate(name string) *template.Template {
	_, thisfile, _, _ := runtime.Caller(1)
	f, err := os.Open(path.Join(path.Dir(thisfile), name))
	d.Chk.NoError(err)
	defer f.Close()
	b, err := ioutil.ReadAll(f)
	d.Chk.NoError(err)
	t, err := template.New(name).Parse(string(b))
	d.Chk.NoError(err)
	return t
}
