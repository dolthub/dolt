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
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
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
	var verifyFilename *string
	if len(os.Args) > 1 {
		if os.Args[1] == "-verify" {
			if len(os.Args) != 3 {
				fmt.Printf("Usage: 3pdeps [-verify EXISTING_LICENSES_FILENAME]\n")
				os.Exit(1)
			}
			verifyFilename = &os.Args[2]
		} else {
			fmt.Printf("Usage: 3pdeps [-verify EXISTING_LICENSES_FILENAME]\n")
			os.Exit(1)
		}
	}

	d := json.NewDecoder(os.Stdin)
	var root string
	var mods []string
	var modPkgs map[string][]Package = make(map[string][]Package)
	for {
		var dep Package
		if err := d.Decode(&dep); err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Error reading `go list` stdin input: %v\n", err)
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
			log.Fatalf("Unexpected dependency read from stdin; not gosdk, not main module, not a module.\n")
		}
	}

	var out io.Writer = os.Stdout
	if verifyFilename != nil {
		out = &bytes.Buffer{}
	}

	PrintDoltLicense(out)
	PrintGoLicense(out, root)

	sort.Strings(mods)
	var l string
	for _, k := range mods {
		if k != l {
			m := modPkgs[k][0].Module
			dir := m.Dir
			if m.Replace != nil {
				dir = m.Replace.Dir
			}
			PrintPkgLicense(out, k, dir)
		}
		l = k
	}

	if verifyFilename != nil {
		verifyFile, err := os.Open(*verifyFilename)
		if err != nil {
			log.Fatalf("Error opening -verify file %s: %v\n", *verifyFilename, err)
		}
		verifyContents, err := ioutil.ReadAll(verifyFile)
		if err != nil {
			log.Fatalf("Error reading -verify file %s: %v\n", *verifyFilename, err)
		}
		if !bytes.Equal(out.(*bytes.Buffer).Bytes(), verifyContents) {
			fmt.Printf("Difference found between current output and %s\n", *verifyFilename)
			fmt.Printf("Please run ./Godeps/update.sh and check in the results.\n")
			os.Exit(1)
		}
	}

}

func PrintDoltLicense(out io.Writer) {
	fmt.Fprintf(out, "================================================================================\n")
	fmt.Fprintf(out, "= Dolt licensed under: =\n\n")
	PrintLicense(out, "../LICENSE")
	fmt.Fprintf(out, "================================================================================\n")
}

func PrintGoLicense(out io.Writer, root string) {
	filepath := FindLicenseFile(root, []string{"LICENSE", "../LICENSE"})
	fmt.Fprintf(out, "\n================================================================================\n")
	fmt.Fprintf(out, "= Go standard library licensed under: =\n\n")
	PrintLicense(out, filepath)
	fmt.Fprintf(out, "================================================================================\n")
}

var StandardCandidates = []string{
	"LICENSE",
	"LICENSE.txt",
	"LICENSE.md",
	"COPYING",
	"LICENSE-MIT",
}

func PrintPkgLicense(out io.Writer, pkg string, dir string) {
	filepath := FindLicenseFile(dir, StandardCandidates)
	fmt.Fprintf(out, "\n================================================================================\n")
	fmt.Fprintf(out, "= %v licensed under: =\n\n", pkg)
	PrintLicense(out, filepath)
	fmt.Fprintf(out, "================================================================================\n")
}

func FindLicenseFile(dir string, candidates []string) string {
	for _, c := range candidates {
		if _, err := os.Stat(dir + "/" + c); err == nil {
			return dir + "/" + c
		}
	}
	log.Fatalf("Required license not found in directory %s", dir)
	// Unreachable
	return ""
}

func PrintLicense(out io.Writer, filepath string) {
	f, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("Error opening license file [%s] for copying: %v\n", filepath, err)
	}
	contents, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("Error reading license file [%s] for copying: %v\n", filepath, err)
	}
	_, err = out.Write(contents)
	if err != nil {
		log.Fatalf("Error writing license file contents to out: %v", err)
	}
	base := path.Base(filepath)
	sum := sha512.Sum512_224(contents)
	fmt.Fprintf(out, "\n= %v %v =\n", base, hex.EncodeToString(sum[:]))
}
