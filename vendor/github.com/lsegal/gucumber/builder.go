package gucumber

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
)

const (
	importMarkerFile = "importmarker__.go"
	testFile         = "gucumbertest__.go"
)

func BuildAndRunDir(dir string, filters []string) error {
	defer buildCleanup(dir)

	info := buildInfo{
		Imports:      []string{},
		FeaturesPath: fmt.Sprintf("%q", dir),
		Filters:      filters,
	}

	goFiles, _ := filepath.Glob(filepath.Join(dir, "*.go"))
	goFiles2, _ := filepath.Glob(filepath.Join(dir, "**", "*.go"))
	goFiles = append(goFiles, goFiles2...)

	// write special constants to packages so they can be imported
	for _, file := range goFiles {
		ifile := filepath.Join(filepath.Dir(file), importMarkerFile)
		if _, err := os.Stat(ifile); err != nil {
			pkgName := filepath.Base(filepath.Dir(file))
			if pkgName == "_test" {
				continue
			}
			fullPkg := assembleImportPath(file)

			if fullPkg == "" {
				return fmt.Errorf("could not determine package path for %s", file)
			}

			info.Imports = append(info.Imports, fullPkg)

			src := fmt.Sprintf("package %s\nvar IMPORT_MARKER = true\n", pkgName)
			err = ioutil.WriteFile(ifile, []byte(src), 0664)
			if err != nil {
				return err
			}
		}
	}

	// write main test stub
	os.MkdirAll(filepath.Join(dir, "_test"), 0777)
	f, err := os.Create(filepath.Join(dir, "_test", testFile))
	if err != nil {
		return err
	}
	tplMain.Execute(f, info)
	f.Close()

	// now run the command
	tfile := "./" + filepath.ToSlash(dir) + "/_test/" + testFile
	cmd := exec.Command("go", "run", tfile)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		buildCleanup(dir)
		os.Exit(1)
	}

	return nil
}

// ToSlash is being used to coerce the different
// os PathSeparators into the forward slash
// as the forward slash is required by Go's import statement
func assembleImportPath(file string) string {
	a, _ := filepath.Abs(filepath.Dir(file))
	absPath, fullPkg := filepath.ToSlash(a), ""
	for _, p := range filepath.SplitList(os.Getenv("GOPATH")) {
		a, _ = filepath.Abs(p)
		p = filepath.ToSlash(a)
		if strings.HasPrefix(absPath, p) {
			prefixPath := filepath.ToSlash(filepath.Join(p, "src"))
			rpath, _ := filepath.Rel(prefixPath, absPath)
			fullPkg = filepath.ToSlash(rpath)
			break
		}
	}
	return fullPkg
}

type buildInfo struct {
	Imports      []string
	FeaturesPath string
	Filters      []string
}

func buildCleanup(dir string) {
	g, _ := filepath.Glob(filepath.Join(dir, importMarkerFile))
	g2, _ := filepath.Glob(filepath.Join(dir, "**", importMarkerFile))

	g = append(g, g2...)
	for _, d := range g {
		os.Remove(d)
	}

	p := filepath.Join(dir, "_test")
	if _, err := os.Stat(p); err == nil {
		os.RemoveAll(p)
	}
}

var tplMain = template.Must(template.New("main").Parse(`
package main

import (
	"github.com/lsegal/gucumber"
	{{range $n, $i := .Imports}}_i{{$n}} "{{$i}}"
	{{end}}
)

var (
	{{range $n, $i := .Imports}}_ci{{$n}} = _i{{$n}}.IMPORT_MARKER
	{{end}}
)

func main() {
	{{if .Filters}}
	gucumber.GlobalContext.Filters = []string{
	{{range $_, $f := .Filters}}"{{$f}}",
	{{end}}
	}
	{{end}}
	gucumber.GlobalContext.RunDir({{.FeaturesPath}})
}
`))
