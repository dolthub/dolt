package main

import (
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"text/template"

	"sort"

	"github.com/attic-labs/noms/d"
)

var (
	headerTempl    = readTemplate("header.tmpl")
	primitiveTempl = readTemplate("primitive.tmpl")
)

func main() {
	types := map[string]bool{"Bool": false, "Int8": true, "Int16": true, "Int32": true, "Int64": true, "UInt8": true, "UInt16": true, "UInt32": true, "UInt64": true, "Float32": true, "Float64": true}
	typeNames := []string{}
	for k := range types {
		typeNames = append(typeNames, k)
	}
	sort.Strings(typeNames)

	f, err := os.OpenFile("primitives.go", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatal(err)
		return
	}

	headerTempl.Execute(f, nil)

	for _, t := range typeNames {
		ordered := types[t]
		goType := strings.ToLower(t)
		primitiveTempl.Execute(f, struct {
			NomsType  string
			GoType    string
			IsOrdered bool
		}{t, goType, ordered})
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
