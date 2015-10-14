package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/build"
	"go/parser"
	"go/token"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"

	"github.com/attic-labs/noms/Godeps/_workspace/src/golang.org/x/tools/imports"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/nomdl/codegen/code"
	"github.com/attic-labs/noms/nomdl/pkg"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var (
	depsDirFlag = flag.String("deps-dir", "", "Directory where code generated for dependencies will be written")
	inFlag      = flag.String("in", "", "The name of the noms file to read")
	outFlag     = flag.String("out", "", "The name of the go file to write")
	pkgDSFlag   = flag.String("package-ds", "", "The dataset to read/write packages from/to.")
	packageFlag = flag.String("package", "", "The name of the go package to write")
)

const ext = ".noms"

func main() {
	flags := datas.NewFlags()
	flag.Parse()

	ds, ok := flags.CreateDataStore()
	if !ok {
		ds = datas.NewDataStore(chunks.NewMemoryStore())
	}
	defer ds.Close()

	if *pkgDSFlag != "" {
		if !ok {
			log.Print("Package dataset provided, but DataStore could not be opened.")
			flag.Usage()
			return
		}
		if *depsDirFlag == "" {
			log.Print("Package dataset provided, but no output directory for generated dependency code.")
			flag.Usage()
			return
		}
	} else {
		log.Print("No package dataset provided; will be unable to process imports.")
	}
	pkgDS := dataset.NewDataset(ds, *pkgDSFlag)
	// Ensure that, if pkgDS has stuff in it, its head is a SetOfRefOfPackage.
	if h, ok := pkgDS.MaybeHead(); ok {
		// Will panic on failure. Can do better once generated collections implement types.Value.
		types.SetOfRefOfPackageFromVal(h.Value())
	}

	depsDir, err := filepath.Abs(*depsDirFlag)
	if err != nil {
		log.Fatalf("Could not canonicalize -deps-dir: %v", err)
	}
	packageName := getGoPackageName()
	if *inFlag != "" {
		out := *outFlag
		if out == "" {
			out = getOutFileName(*inFlag)
		}
		generate(packageName, *inFlag, out, depsDir, pkgDS)
		return
	}

	// Generate code from all .noms file in the current directory
	nomsFiles, err := filepath.Glob("*" + ext)
	d.Chk.NoError(err)
	for _, n := range nomsFiles {
		pkgDS = generate(packageName, n, getOutFileName(n), depsDir, pkgDS)
	}
}

func generate(packageName, in, out, depsDir string, pkgDS dataset.Dataset) dataset.Dataset {
	inFile, err := os.Open(in)
	d.Chk.NoError(err)
	defer inFile.Close()

	p := pkg.ParseNomDL(packageName, inFile, filepath.Dir(in), pkgDS.Store())

	// Generate code for all p's deps first.
	deps := generateDepCode(depsDir, p.New(), pkgDS.Store())

	generateAndEmit(getBareFileName(in), out, importPaths(depsDir, deps), deps, p)

	// Since we're just building up a set of refs to all the packages in pkgDS, simply retrying is the logical response to commit failure.
	for ok := false; !ok; pkgDS, ok = pkgDS.Commit(buildSetOfRefOfPackage(p, deps, pkgDS).NomsValue()) {
	}
	return pkgDS
}

type depsMap map[ref.Ref]types.Package

func generateDepCode(depsDir string, p types.Package, cs chunks.ChunkSource) depsMap {
	deps := depsMap{}
	p.Dependencies().IterAll(func(r types.RefOfPackage) {
		p := r.GetValue(cs)
		pDeps := generateDepCode(depsDir, p, cs)
		tag := code.ToTag(p.Ref().String())
		parsed := pkg.Parsed{PackageDef: p.Def(), Name: tag}
		generateAndEmit(tag, filepath.Join(depsDir, tag, tag+".go"), importPaths(depsDir, pDeps), pDeps, parsed)

		for depRef, dep := range pDeps {
			deps[depRef] = dep
		}
		deps[r.Ref()] = p
	})
	return deps
}

func generateAndEmit(tag, out string, importPaths []string, deps depsMap, p pkg.Parsed) {
	var buf bytes.Buffer
	gen := NewCodeGen(&buf, tag, importPaths, deps, p)
	gen.WritePackage()

	bs, err := imports.Process(out, buf.Bytes(), nil)
	if err != nil {
		fmt.Println(buf.String())
	}
	d.Chk.NoError(err)

	d.Chk.NoError(os.MkdirAll(filepath.Dir(out), 0700))

	outFile, err := os.OpenFile(out, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	d.Chk.NoError(err)
	defer outFile.Close()

	io.Copy(outFile, bytes.NewBuffer(bs))
}

func importPaths(depsDir string, deps depsMap) (paths []string) {
	for depRef := range deps {
		depDir := filepath.Join(depsDir, code.ToTag(depRef.String()))
		goPkg, err := build.Default.ImportDir(depDir, build.FindOnly)
		d.Chk.NoError(err)
		paths = append(paths, goPkg.ImportPath)
	}
	return
}

func buildSetOfRefOfPackage(pkg pkg.Parsed, deps depsMap, ds dataset.Dataset) types.SetOfRefOfPackage {
	// Can do better once generated collections implement types.Value.
	s := types.NewSetOfRefOfPackage()
	if h, ok := ds.MaybeHead(); ok {
		s = types.SetOfRefOfPackageFromVal(h.Value())
	}
	for _, dep := range deps {
		// Writing the deps into ds should be redundant at this point, but do it to be sure.
		// TODO: consider moving all dataset work over into nomdl/pkg BUG 409
		s = s.Insert(types.NewRefOfPackage(types.WriteValue(dep.NomsValue(), ds.Store())))
	}
	r := types.WriteValue(pkg.New().NomsValue(), ds.Store())
	return s.Insert(types.NewRefOfPackage(r))
}

func getOutFileName(in string) string {
	return in[:len(in)-len(ext)] + ".go"
}

func getBareFileName(in string) string {
	base := filepath.Base(in)
	return base[:len(base)-len(filepath.Ext(base))]
}

func getGoPackageName() string {
	if *packageFlag != "" {
		return *packageFlag
	}

	// It is illegal to have multiple go files in the same directory with different package names.
	// We can therefore just pick the first one and get the package name from there.
	goFiles, err := filepath.Glob("*.go")
	d.Chk.NoError(err)
	d.Chk.True(len(goFiles) > 0)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, goFiles[0], nil, parser.PackageClauseOnly)
	d.Chk.NoError(err)
	return f.Name.String()
}

type codeGen struct {
	w         io.Writer
	pkg       pkg.Parsed
	deps      depsMap
	fileid    string
	imports   []string
	written   map[string]bool
	generator *code.Generator
	templates *template.Template
}

type resolver struct {
}

func NewCodeGen(w io.Writer, fileID string, importPaths []string, deps depsMap, pkg pkg.Parsed) *codeGen {
	gen := &codeGen{w, pkg, deps, fileID, importPaths, map[string]bool{}, nil, nil}
	gen.generator = &code.Generator{R: gen}
	gen.templates = gen.readTemplates()
	return gen
}

func (gen *codeGen) readTemplates() *template.Template {
	_, thisfile, _, _ := runtime.Caller(1)
	glob := path.Join(path.Dir(thisfile), "*.tmpl")
	return template.Must(template.New("").Funcs(
		template.FuncMap{
			"defType":        gen.generator.DefType,
			"defToValue":     gen.generator.DefToValue,
			"valueToDef":     gen.generator.ValueToDef,
			"userType":       gen.generator.UserType,
			"userToValue":    gen.generator.UserToValue,
			"valueToUser":    gen.generator.ValueToUser,
			"userZero":       gen.generator.UserZero,
			"valueZero":      gen.generator.ValueZero,
			"title":          strings.Title,
			"toTypesTypeRef": gen.generator.ToTypeRef,
		}).ParseGlob(glob))
}

func (gen *codeGen) Resolve(t types.TypeRef) types.TypeRef {
	if !t.IsUnresolved() {
		return t
	}
	if !t.HasPackageRef() {
		return gen.pkg.GetNamedType(t.Name())
	}

	dep, ok := gen.deps[t.PackageRef()]
	d.Chk.True(ok, "Package %s is referenced in %+v, but is not a dependency.", t.PackageRef().String(), t)
	d.Chk.True(dep.HasNamedType(t.Name()), "Cannot import type %s from package %s.", t.Name(), t.PackageRef().String())
	return dep.GetNamedType(t.Name()).MakeImported(t.PackageRef())
}

func (gen *codeGen) WritePackage() {
	data := struct {
		HasImports bool
		HasTypes   bool
		FileID     string
		Imports    []string
		Name       string
		Types      []types.TypeRef
	}{
		len(gen.imports) > 0,
		len(gen.pkg.Types) > 0,
		gen.fileid,
		gen.imports,
		gen.pkg.Name,
		gen.pkg.Types,
	}
	err := gen.templates.ExecuteTemplate(gen.w, "header.tmpl", data)
	d.Exp.NoError(err)

	for _, t := range gen.pkg.UsingDeclarations {
		gen.write(t)
	}

	for _, t := range gen.pkg.Types {
		gen.write(t)
	}
}

func (gen *codeGen) write(t types.TypeRef) {
	t = gen.Resolve(t)
	// If t has a package reference, then it represents an imported type, so we shouldn't generate code for it.
	if gen.written[gen.generator.UserName(t)] || t.HasPackageRef() {
		return
	}
	k := t.Desc.Kind()
	switch k {
	case types.BlobKind, types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.StringKind, types.UInt16Kind, types.UInt32Kind, types.UInt64Kind, types.UInt8Kind, types.ValueKind, types.TypeRefKind:
		return
	case types.EnumKind:
		gen.writeEnum(t)
	case types.ListKind:
		gen.writeList(t)
	case types.MapKind:
		gen.writeMap(t)
	case types.RefKind:
		gen.writeRef(t)
	case types.SetKind:
		gen.writeSet(t)
	case types.StructKind:
		gen.writeStruct(t)
	default:
		panic("unreachable")
	}
}

func (gen *codeGen) writeTemplate(tmpl string, t types.TypeRef, data interface{}) {
	err := gen.templates.ExecuteTemplate(gen.w, tmpl, data)
	d.Exp.NoError(err)
	gen.written[gen.generator.UserName(t)] = true
}

func (gen *codeGen) writeStruct(t types.TypeRef) {
	desc := t.Desc.(types.StructDesc)
	data := struct {
		FileID        string
		PackageName   string
		Name          string
		Type          types.TypeRef
		Fields        []types.Field
		Choices       types.Choices
		HasUnion      bool
		UnionZeroType types.TypeRef
		CanUseDef     bool
	}{
		gen.fileid,
		gen.pkg.Name,
		gen.generator.UserName(t),
		t,
		desc.Fields,
		nil,
		len(desc.Union) != 0,
		types.MakePrimitiveTypeRef(types.UInt32Kind),
		gen.canUseDef(t),
	}
	if data.HasUnion {
		data.Choices = desc.Union
		data.UnionZeroType = data.Choices[0].T
	}
	gen.writeTemplate("struct.tmpl", t, data)
	for _, f := range desc.Fields {
		gen.write(f.T)
	}
	if data.HasUnion {
		for _, f := range desc.Union {
			gen.write(f.T)
		}
	}
}

func (gen *codeGen) writeList(t types.TypeRef) {
	elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
	data := struct {
		FileID      string
		PackageName string
		Name        string
		Type        types.TypeRef
		ElemType    types.TypeRef
		CanUseDef   bool
	}{
		gen.fileid,
		gen.pkg.Name,
		gen.generator.UserName(t),
		t,
		elemTypes[0],
		gen.canUseDef(t),
	}
	gen.writeTemplate("list.tmpl", t, data)
	gen.write(elemTypes[0])
}

func (gen *codeGen) writeMap(t types.TypeRef) {
	elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
	data := struct {
		FileID      string
		PackageName string
		Name        string
		Type        types.TypeRef
		KeyType     types.TypeRef
		ValueType   types.TypeRef
		CanUseDef   bool
	}{
		gen.fileid,
		gen.pkg.Name,
		gen.generator.UserName(t),
		t,
		elemTypes[0],
		elemTypes[1],
		gen.canUseDef(t),
	}
	gen.writeTemplate("map.tmpl", t, data)
	gen.write(elemTypes[0])
	gen.write(elemTypes[1])
}

func (gen *codeGen) writeRef(t types.TypeRef) {
	elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
	data := struct {
		FileID      string
		PackageName string
		Name        string
		Type        types.TypeRef
		ElemType    types.TypeRef
	}{
		gen.fileid,
		gen.pkg.Name,
		gen.generator.UserName(t),
		t,
		elemTypes[0],
	}
	gen.writeTemplate("ref.tmpl", t, data)
	gen.write(elemTypes[0])
}

func (gen *codeGen) writeSet(t types.TypeRef) {
	elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
	data := struct {
		FileID      string
		PackageName string
		Name        string
		Type        types.TypeRef
		ElemType    types.TypeRef
		CanUseDef   bool
	}{
		gen.fileid,
		gen.pkg.Name,
		gen.generator.UserName(t),
		t,
		elemTypes[0],
		gen.canUseDef(t),
	}
	gen.writeTemplate("set.tmpl", t, data)
	gen.write(elemTypes[0])
}

func (gen *codeGen) writeEnum(t types.TypeRef) {
	data := struct {
		FileID      string
		PackageName string
		Name        string
		Type        types.TypeRef
		Ids         []string
	}{
		gen.fileid,
		gen.pkg.Name,
		t.Name(),
		t,
		t.Desc.(types.EnumDesc).IDs,
	}
	gen.writeTemplate("enum.tmpl", t, data)
}

func (gen *codeGen) canUseDef(t types.TypeRef) bool {
	cache := map[string]bool{}

	var rec func(t types.TypeRef) bool
	rec = func(t types.TypeRef) bool {
		t = gen.Resolve(t)
		switch t.Desc.Kind() {
		case types.ListKind:
			return rec(t.Desc.(types.CompoundDesc).ElemTypes[0])
		case types.SetKind:
			elemType := t.Desc.(types.CompoundDesc).ElemTypes[0]
			return !gen.containsNonComparable(elemType) && rec(elemType)
		case types.MapKind:
			elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
			return !gen.containsNonComparable(elemTypes[0]) && rec(elemTypes[0]) && rec(elemTypes[1])
		case types.StructKind:
			userName := gen.generator.UserName(t)
			if b, ok := cache[userName]; ok {
				return b
			}
			cache[userName] = true
			for _, f := range t.Desc.(types.StructDesc).Fields {
				if f.T.Equals(t) || !rec(f.T) {
					cache[userName] = false
					return false
				}
			}
			return true
		default:
			return true
		}
	}

	return rec(t)
}

// We use a go map as the def for Set and Map. These cannot have a key that is a
// Set, Map or a List because slices and maps are not comparable in go.
func (gen *codeGen) containsNonComparable(t types.TypeRef) bool {
	cache := map[string]bool{}

	var rec func(t types.TypeRef) bool
	rec = func(t types.TypeRef) bool {
		t = gen.Resolve(t)
		switch t.Desc.Kind() {
		case types.ListKind, types.MapKind, types.SetKind:
			return true
		case types.StructKind:
			// Only structs can be recursive
			userName := gen.generator.UserName(t)
			if b, ok := cache[userName]; ok {
				return b
			}
			// If we get here in a recursive call we will mark it as not having a non comparable value. If it does then that will
			// get handled higher up in the call chain.
			cache[userName] = false
			for _, f := range t.Desc.(types.StructDesc).Fields {
				if rec(f.T) {
					cache[userName] = true
					return true
				}
			}
			return cache[userName]
		default:
			return false
		}
	}

	return rec(t)
}
