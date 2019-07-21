package main

import (
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
)

type Module struct {
	Path    string  `json:",omitempty"`
	Replace *Module `json:",omitempty"`
	Main    bool    `json:",omitempty"`
	Dir     string  `json:",omitempty"`
}

type Package struct {
	Root     string  `json:",omitempty"`
	Module   *Module `json:",omitempty"`
	Standard bool    `json:",omitempty"`
}

func main() {
	d := json.NewDecoder(os.Stdin)
	var root string
	var mods []string
	var modPkgs map[string][]Package = make(map[string][]Package)
	for {
		var dep Package
		if err := d.Decode(&dep); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		if dep.Standard {
			root = dep.Root
		} else if dep.Module != nil {
			if dep.Module.Main {
			} else if dep.Module.Replace != nil {
				mods = append(mods, dep.Module.Replace.Path)
				modPkgs[dep.Module.Replace.Path] = append(modPkgs[dep.Module.Replace.Path], dep)
			} else {
				mods = append(mods, dep.Module.Path)
				modPkgs[dep.Module.Path] = append(modPkgs[dep.Module.Path], dep)
			}
		} else {
			panic("Unexpected dep")
		}
	}

	PrintDoltLicense()
	PrintGoLicense(root)

	sort.Strings(mods)
	var l string
	for _, k := range mods {
		if k != l {
			m := modPkgs[k][0].Module
			dir := m.Dir
			if m.Replace != nil {
				dir = m.Replace.Dir
			}
			PrintPkgLicense(k, dir)
		}
		l = k
	}

}

func PrintDoltLicense() {
	fmt.Printf("================================================================================\n")
	fmt.Printf("= Dolt licensed under: =\n\n")
	PrintLicense("./LICENSE")
	fmt.Printf("================================================================================\n")
}

func PrintGoLicense(root string) {
	fmt.Printf("\n================================================================================\n")
	fmt.Printf("= Go standard library licensed under: =\n\n")
	PrintLicense(root + "/LICENSE")
	fmt.Printf("================================================================================\n")
}

func PrintPkgLicense(pkg string, dir string) {
	filepath := FindLicenseFile(dir)
	fmt.Printf("\n================================================================================\n")
	fmt.Printf("= %v licensed under: =\n\n", pkg)
	PrintLicense(filepath)
	fmt.Printf("================================================================================\n")
}

func FindLicenseFile(dir string) string {
	candidates := []string{
		"LICENSE",
		"LICENSE.txt",
		"LICENSE.md",
		"COPYING",
		"LICENSE-MIT",
		"README.org",
	}
	for _, c := range candidates {
		if _, err := os.Stat(dir + "/" + c); err == nil {
			return dir + "/" + c
		}
	}
	panic("License not found: " + dir)
}

func PrintLicense(filepath string) {
	f, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	contents, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	base := path.Base(filepath)
	// XXX: Hack for extracting LICENSE from xslx.
	// This can be removed when we upgrade the package.
	if base == "README.org" {
		start := bytes.Index(contents, []byte("#+BEGIN_EXAMPLE\n\n"))
		end := bytes.Index(contents, []byte("\n#+END_EXAMPLE\n"))
		contents = contents[start+17 : end]
	}
	_, err = os.Stdout.Write(contents)
	if err != nil {
		panic(err)
	}
	sum := sha512.Sum512_224(contents)
	fmt.Printf("\n= %v %v =\n", base, hex.EncodeToString(sum[:]))
}
