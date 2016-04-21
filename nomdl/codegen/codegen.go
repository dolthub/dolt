package main

import (
	"bytes"
	"flag"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"text/template"

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
	outDirFlag  = flag.String("out-dir", ".", "Directory where generated code will be written")
	inFlag      = flag.String("in", "", "The name of the noms file to read")
	pkgDSFlag   = flag.String("package-ds", "", "The dataset to read/write packages from/to.")
	packageFlag = flag.String("package", "", "The name of the go package to write")

	idRegexp    = regexp.MustCompile(`[_\pL][_\pL\pN]*`)
	illegalRune = regexp.MustCompile(`[^_\pL\pN]`)
)

const ext = ".noms"

type refSet map[ref.Ref]bool

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
	} else {
		log.Print("No package dataset provided; will be unable to process imports.")
		*pkgDSFlag = "default"
	}

	pkgDS := dataset.NewDataset(ds, *pkgDSFlag)
	// Ensure that, if pkgDS has stuff in it, its head is a SetOfRefOfPackage.
	if h, ok := pkgDS.MaybeHead(); ok {
		d.Chk.IsType(types.NewSetOfRefOfPackage(), h.Value())
	}

	localPkgs := refSet{}
	outDir, err := filepath.Abs(*outDirFlag)
	d.Chk.NoError(err, "Could not canonicalize -out-dir: %v", err)
	packageName := ""

	if *inFlag != "" {
		out := getOutFileName(filepath.Base(*inFlag))
		p := parsePackageFile(packageName, *inFlag, pkgDS)
		localPkgs[p.Ref()] = true
		generate(packageName, *inFlag, filepath.Join(outDir, out), outDir, map[string]bool{}, p, localPkgs, pkgDS)
		return
	}

	// Generate code from all .noms file in the current directory
	nomsFiles, err := filepath.Glob("*" + ext)
	d.Chk.NoError(err)

	written := map[string]bool{}
	packages := map[string]pkg.Parsed{}
	for _, inFile := range nomsFiles {
		p := parsePackageFile(packageName, inFile, pkgDS)
		localPkgs[p.Ref()] = true
		packages[inFile] = p
	}
	// Sort to have deterministic output.
	keys := make([]string, 0, len(packages))
	sort.Strings(keys)
	for inFile := range packages {
		keys = append(keys, inFile)
	}
	for _, inFile := range keys {
		p := packages[inFile]
		pkgDS = generate(packageName, inFile, filepath.Join(outDir, getOutFileName(inFile)), outDir, written, p, localPkgs, pkgDS)
	}
}

func parsePackageFile(packageName string, in string, pkgDS dataset.Dataset) pkg.Parsed {
	inFile, err := os.Open(in)
	d.Chk.NoError(err)
	defer inFile.Close()

	return pkg.ParseNomDL(packageName, inFile, filepath.Dir(in), pkgDS.Store())
}

func generate(packageName, in, out, outDir string, written map[string]bool, parsed pkg.Parsed, localPkgs refSet, pkgDS dataset.Dataset) dataset.Dataset {
	// Generate code for all p's deps first.
	deps := generateDepCode(packageName, outDir, written, parsed.Package, localPkgs, pkgDS.Store())
	generateAndEmit(getBareFileName(in), out, written, deps, parsed)

	// Since we're just building up a set of refs to all the packages in pkgDS, simply retrying is the logical response to commit failure.
	err := datas.ErrOptimisticLockFailed
	for ; err == datas.ErrOptimisticLockFailed; pkgDS, err = pkgDS.Commit(buildSetOfRefOfPackage(parsed, deps, pkgDS)) {
	}
	return pkgDS
}

type depsMap map[ref.Ref]types.Package

func generateDepCode(packageName, outDir string, written map[string]bool, p types.Package, localPkgs refSet, vr types.ValueReader) depsMap {
	deps := depsMap{}
	for _, r := range p.Dependencies() {
		p := vr.ReadValue(r).(types.Package)
		pDeps := generateDepCode(packageName, outDir, written, p, localPkgs, vr)
		tag := code.ToTag(p.Ref())
		parsed := pkg.Parsed{Package: p, Name: packageName}
		if !localPkgs[parsed.Ref()] {
			generateAndEmit(tag, filepath.Join(outDir, tag+".js"), written, pDeps, parsed)
			localPkgs[parsed.Ref()] = true
		}
		for depRef, dep := range pDeps {
			deps[depRef] = dep
		}
		deps[r] = p
	}
	return deps
}

func generateAndEmit(tag, out string, written map[string]bool, deps depsMap, p pkg.Parsed) {
	var buf bytes.Buffer
	gen := newCodeGen(&buf, tag, written, deps, p)
	gen.WritePackage()

	d.Chk.NoError(os.MkdirAll(filepath.Dir(out), 0700))

	outFile, err := os.OpenFile(out, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	d.Chk.NoError(err)
	defer outFile.Close()

	io.Copy(outFile, &buf)
}

func buildSetOfRefOfPackage(pkg pkg.Parsed, deps depsMap, ds dataset.Dataset) types.Set {
	// Can do better once generated collections implement types.Value.
	s := types.NewSetOfRefOfPackage()
	if h, ok := ds.MaybeHead(); ok {
		s = h.Value().(types.Set)
	}
	for _, dep := range deps {
		// Writing the deps into ds should be redundant at this point, but do it to be sure.
		// TODO: consider moving all dataset work over into nomdl/pkg BUG 409
		s = s.Insert(ds.Store().WriteValue(dep).(types.Ref))
	}
	r := ds.Store().WriteValue(pkg.Package).(types.Ref)
	return s.Insert(r)
}

func getOutFileName(in string) string {
	return in[:len(in)-len(ext)] + ".noms.js"
}

func getBareFileName(in string) string {
	base := filepath.Base(in)
	return base[:len(base)-len(filepath.Ext(base))]
}

type codeGen struct {
	w         io.Writer
	pkg       pkg.Parsed
	deps      depsMap
	written   map[string]bool
	toWrite   []types.Type
	generator *code.Generator
	templates *template.Template
}

func newCodeGen(w io.Writer, fileID string, written map[string]bool, deps depsMap, pkg pkg.Parsed) *codeGen {
	gen := &codeGen{w, pkg, deps, written, []types.Type{}, nil, nil}
	gen.generator = &code.Generator{
		R:          gen,
		AliasNames: pkg.AliasNames,
		Package:    &pkg.Package,
	}
	gen.templates = gen.readTemplates()
	return gen
}

func (gen *codeGen) readTemplates() *template.Template {
	_, thisfile, _, _ := runtime.Caller(1)
	glob := path.Join(path.Dir(thisfile), "js", "*.tmpl")
	return template.Must(template.New("").Funcs(
		template.FuncMap{
			"defToUser":            gen.generator.DefToUser,
			"defToValue":           gen.generator.DefToValue,
			"defType":              gen.generator.DefType,
			"importJS":             gen.generator.ImportJS,
			"importJsType":         gen.generator.ImportJSType,
			"isLast":               gen.generator.IsLast,
			"mayHaveChunks":        gen.generator.MayHaveChunks,
			"refToAliasName":       gen.generator.RefToAliasName,
			"refToJSIdentfierName": gen.generator.RefToJSIdentfierName,
			"title":                strings.Title,
			"toTypesType":          gen.generator.ToTypesType,
			"toTypeValueJS":        gen.generator.ToTypeValueJS,
			"userToDef":            gen.generator.UserToDef,
			"userToValue":          gen.generator.UserToValue,
			"userType":             gen.generator.UserType,
			"userTypeJS":           gen.generator.UserTypeJS,
			"userZero":             gen.generator.UserZero,
			"valueToDef":           gen.generator.ValueToDef,
			"valueToUser":          gen.generator.ValueToUser,
			"valueZero":            gen.generator.ValueZero,
		}).ParseGlob(glob))
}

func (gen *codeGen) Resolve(t types.Type, pkg *types.Package) types.Type {
	if !t.IsUnresolved() {
		return t
	}
	if !t.HasPackageRef() {
		return gen.pkg.Types()[t.Ordinal()]
	}

	if t.PackageRef() == pkg.Ref() {
		return pkg.Types()[t.Ordinal()]
	}

	dep, ok := gen.deps[t.PackageRef()]
	d.Chk.True(ok, "Package %s is referenced in %+v, but is not a dependency.", t.PackageRef().String(), t)
	return dep.Types()[t.Ordinal()]
}

func (gen *codeGen) WritePackage() {
	pkgTypes := gen.pkg.Types()
	data := struct {
		PackageRef   ref.Ref
		HasTypes     bool
		Dependencies []ref.Ref
		Name         string
		Types        []types.Type
	}{
		gen.pkg.Package.Ref(),
		len(pkgTypes) > 0,
		gen.pkg.Dependencies(),
		gen.pkg.Name,
		pkgTypes,
	}

	// In JS we want to write the imports at the top of the file but we do not know what we need to import until we have written everything. We therefore write to a buffer and when everything is done we can write the imports and write the buffer into the writer.
	var buf bytes.Buffer
	w := gen.w

	gen.w = &buf

	err := gen.templates.ExecuteTemplate(gen.w, "package.tmpl", data)
	d.Exp.NoError(err)

	for i, t := range pkgTypes {
		gen.writeTopLevel(t, i)
	}

	for _, t := range gen.pkg.UsingDeclarations {
		gen.write(t)
	}

	for len(gen.toWrite) > 0 {
		t := gen.toWrite[0]
		gen.toWrite = gen.toWrite[1:]
		gen.write(t)
	}

	gen.w = w
	gen.WriteHeader()
	io.Copy(w, &buf)
}

func (gen *codeGen) WriteHeader() {
	importedJS := make([]string, 0, len(gen.generator.ImportedJS))
	importedJSTypes := make([]string, 0, len(gen.generator.ImportedJSTypes))
	for name := range gen.generator.ImportedJS {
		importedJS = append(importedJS, name)
	}
	for name := range gen.generator.ImportedJSTypes {
		if _, ok := gen.generator.ImportedJS[name]; !ok {
			importedJSTypes = append(importedJSTypes, name)
		}
	}
	sort.Strings(importedJS)
	sort.Strings(importedJSTypes)

	pkgTypes := gen.pkg.Types()
	data := struct {
		PackageRef      ref.Ref
		HasTypes        bool
		Dependencies    []ref.Ref
		Name            string
		Types           []types.Type
		ImportedJS      []string
		ImportedJSTypes []string
		AliasNames      map[ref.Ref]string
	}{
		gen.pkg.Package.Ref(),
		len(pkgTypes) > 0,
		gen.pkg.Dependencies(),
		gen.pkg.Name,
		pkgTypes,
		importedJS,
		importedJSTypes,
		gen.pkg.AliasNames,
	}

	err := gen.templates.ExecuteTemplate(gen.w, "header.tmpl", data)
	d.Exp.NoError(err)
}

func (gen *codeGen) shouldBeWritten(t types.Type) bool {
	if t.IsUnresolved() {
		return false
	}
	if t.Kind() == types.EnumKind || t.Kind() == types.StructKind {
		name := gen.generator.UserName(t)
		d.Chk.False(gen.written[name], "Multiple definitions of type named %s", name)
		return true
	}
	return !gen.written[gen.generator.UserName(t)]
}

func (gen *codeGen) writeTopLevel(t types.Type, ordinal int) {
	switch t.Kind() {
	case types.EnumKind:
		gen.writeEnum(t, ordinal)
	case types.StructKind:
		gen.writeStruct(t, ordinal)
	default:
		gen.write(t)
	}
}

// write generates the code for the given type.
func (gen *codeGen) write(t types.Type) {
	if !gen.shouldBeWritten(t) {
		return
	}
	k := t.Kind()
	switch k {
	case types.BlobKind, types.BoolKind, types.Float32Kind, types.Float64Kind, types.Int16Kind, types.Int32Kind, types.Int64Kind, types.Int8Kind, types.PackageKind, types.StringKind, types.Uint16Kind, types.Uint32Kind, types.Uint64Kind, types.Uint8Kind, types.ValueKind, types.TypeKind:
		return
	case types.ListKind:
		gen.writeList(t)
	case types.MapKind:
		gen.writeMap(t)
	case types.RefKind:
		gen.writeRef(t)
	case types.SetKind:
		gen.writeSet(t)
	default:
		panic("unreachable")
	}
}

func (gen *codeGen) writeLater(t types.Type) {
	if !gen.shouldBeWritten(t) {
		return
	}
	gen.toWrite = append(gen.toWrite, t)
}

func (gen *codeGen) writeTemplate(tmpl string, t types.Type, data interface{}) {
	err := gen.templates.ExecuteTemplate(gen.w, tmpl, data)
	d.Exp.NoError(err)
	gen.written[gen.generator.UserName(t)] = true
}

func (gen *codeGen) writeStruct(t types.Type, ordinal int) {
	d.Chk.True(ordinal >= 0)
	desc := t.Desc.(types.StructDesc)
	data := struct {
		PackageRef    ref.Ref
		Name          string
		Type          types.Type
		Ordinal       int
		Fields        []types.Field
		Choices       []types.Field
		HasUnion      bool
		UnionZeroType types.Type
	}{
		gen.pkg.Package.Ref(),
		gen.generator.UserName(t),
		t,
		ordinal,
		desc.Fields,
		nil,
		len(desc.Union) != 0,
		types.Uint32Type,
	}

	if data.HasUnion {
		data.Choices = desc.Union
		data.UnionZeroType = data.Choices[0].T
	}
	gen.writeTemplate("struct.tmpl", t, data)
	for _, f := range desc.Fields {
		gen.writeLater(f.T)
	}
	if data.HasUnion {
		for _, f := range desc.Union {
			gen.writeLater(f.T)
		}
	}
}

func (gen *codeGen) writeList(t types.Type) {
	elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
	data := struct {
		PackageRef ref.Ref
		Name       string
		Type       types.Type
		ElemType   types.Type
	}{
		gen.pkg.Package.Ref(),
		gen.generator.UserName(t),
		t,
		elemTypes[0],
	}
	gen.writeTemplate("list.tmpl", t, data)
	gen.writeLater(elemTypes[0])
}

func (gen *codeGen) writeMap(t types.Type) {
	elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
	data := struct {
		PackageRef ref.Ref
		Name       string
		Type       types.Type
		KeyType    types.Type
		ValueType  types.Type
	}{
		gen.pkg.Package.Ref(),
		gen.generator.UserName(t),
		t,
		elemTypes[0],
		elemTypes[1],
	}
	gen.writeTemplate("map.tmpl", t, data)
	gen.writeLater(elemTypes[0])
	gen.writeLater(elemTypes[1])
}

func (gen *codeGen) writeRef(t types.Type) {
	elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
	data := struct {
		PackageRef ref.Ref
		Name       string
		Type       types.Type
		ElemType   types.Type
	}{
		gen.pkg.Package.Ref(),
		gen.generator.UserName(t),
		t,
		elemTypes[0],
	}
	gen.writeTemplate("ref.tmpl", t, data)
	gen.writeLater(elemTypes[0])
}

func (gen *codeGen) writeSet(t types.Type) {
	elemTypes := t.Desc.(types.CompoundDesc).ElemTypes
	data := struct {
		PackageRef ref.Ref
		Name       string
		Type       types.Type
		ElemType   types.Type
	}{
		gen.pkg.Package.Ref(),
		gen.generator.UserName(t),
		t,
		elemTypes[0],
	}
	gen.writeTemplate("set.tmpl", t, data)
	gen.writeLater(elemTypes[0])
}

func (gen *codeGen) writeEnum(t types.Type, ordinal int) {
	d.Chk.True(ordinal >= 0)
	data := struct {
		PackageRef ref.Ref
		Name       string
		Type       types.Type
		Ordinal    int
		Ids        []string
	}{
		gen.pkg.Package.Ref(),
		t.Name(),
		t,
		ordinal,
		t.Desc.(types.EnumDesc).IDs,
	}

	gen.writeTemplate("enum.tmpl", t, data)
}
