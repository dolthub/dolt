// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
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
	PrintLicense("../LICENSE")
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
	_, err = os.Stdout.Write(contents)
	if err != nil {
		panic(err)
	}
	base := path.Base(filepath)
	sum := sha512.Sum512_224(contents)
	fmt.Printf("\n= %v %v =\n", base, hex.EncodeToString(sum[:]))
}
