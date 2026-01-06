// Copyright 2024 Dolthub, Inc.
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

package minver

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/structwalk"
	"github.com/dolthub/dolt/go/libraries/utils/version"
)

type FieldInfo struct {
	Name    string
	TypeStr string
	MinVer  string
	YamlTag string
}

func FieldInfoFromLine(l string) (FieldInfo, error) {
	l = strings.TrimSpace(l)
	tokens := strings.Split(l, " ")

	if len(tokens) != 4 {
		return FieldInfo{}, fmt.Errorf("invalid line in minver_validation.txt: '%s'", l)
	}

	return FieldInfo{
		Name:    tokens[0],
		TypeStr: tokens[1],
		MinVer:  tokens[2],
		YamlTag: tokens[3],
	}, nil
}

func FieldInfoFromStructField(field reflect.StructField, depth int) FieldInfo {
	info := FieldInfo{
		Name:    strings.Repeat("-", depth) + field.Name,
		TypeStr: strings.Replace(field.Type.String(), " ", "", -1),
		MinVer:  field.Tag.Get("minver"),
		YamlTag: field.Tag.Get("yaml"),
	}

	if info.MinVer == "" {
		info.MinVer = "0.0.0"
	}

	return info
}

func (fi FieldInfo) Equals(other FieldInfo) bool {
	return fi.Name == other.Name && fi.TypeStr == other.TypeStr && fi.MinVer == other.MinVer && fi.YamlTag == other.YamlTag
}

func (fi FieldInfo) String() string {
	return fmt.Sprintf("%s %s %s %s", fi.Name, fi.TypeStr, fi.MinVer, fi.YamlTag)
}

type MinVerValidationReader struct {
	lines   []string
	current int
}

func OpenMinVerValidation(path string) (*MinVerValidationReader, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(data), "\n")

	return &MinVerValidationReader{
		lines:   lines,
		current: -1,
	}, nil
}

func (r *MinVerValidationReader) Advance() {
	for r.current < len(r.lines) {
		r.current++

		if r.current < len(r.lines) {
			l := r.lines[r.current]

			if !strings.HasPrefix(l, "#") {
				return
			}
		}
	}
}

func (r *MinVerValidationReader) Current() (FieldInfo, error) {
	if r.current < 0 {
		r.Advance()
	}

	if r.current < 0 || r.current < len(r.lines) {
		l := r.lines[r.current]
		return FieldInfoFromLine(l)
	}

	return FieldInfo{}, io.EOF
}

func ValidateMinVerFunc(field reflect.StructField, depth int) error {
	var hasMinVer bool
	var hasOmitEmpty bool

	minVerTag := field.Tag.Get("minver")
	if minVerTag != "" {
		if minVerTag != "TBD" {
			if _, err := version.Encode(minVerTag); err != nil {
				return fmt.Errorf("invalid minver tag on field %s '%s': %w", field.Name, minVerTag, err)
			}
		}
		hasMinVer = true
	}

	isNullable := field.Type.Kind() == reflect.Ptr || field.Type.Kind() == reflect.Slice || field.Type.Kind() == reflect.Map
	if hasMinVer && !isNullable {
		return fmt.Errorf("field '%s' has a version tag '%s' but is not nullable", field.Name, minVerTag)
	}

	yamlTag := field.Tag.Get("yaml")
	if yamlTag == "" {
		return fmt.Errorf("required tag 'yaml' missing on field '%s'", field.Name)
	} else {
		vals := strings.Split(yamlTag, ",")
		for _, val := range vals {
			if val == "omitempty" {
				hasOmitEmpty = true
				break
			}
		}
	}

	if hasMinVer && !hasOmitEmpty {
		return fmt.Errorf("field '%s' has a version tag '%s' but no yaml tag with omitempty", field.Name, minVerTag)
	}

	return nil
}

func ValidateAgainstFile(t *testing.T, path string, st any) {
	rd, err := OpenMinVerValidation(path)
	require.NoError(t, err)

	rd.Advance()

	// if the last field is TBD, we need to skip any nested TBD fields
	var lastIsTBD bool
	var tbdDepth int
	err = structwalk.Walk(st, func(field reflect.StructField, depth int) error {
		if lastIsTBD {
			if depth > tbdDepth {
				// skip this field as it is nested under a TBD field
				return nil
			}
		}

		lastIsTBD = false

		fi := FieldInfoFromStructField(field, depth)
		prevFI, err := rd.Current()
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		if prevFI.Equals(fi) {
			rd.Advance()
			return nil
		}

		if fi.MinVer == "TBD" {
			lastIsTBD = true
			tbdDepth = depth
			return nil
		}

		if errors.Is(err, io.EOF) {
			return fmt.Errorf("new field '%s' added", fi.String())
		} else {
			// You are seeing this error because a new config field was added that didn't meet the requirements.
			// See the comment in "TestMinVer" which covers the requirements of new fields.
			return fmt.Errorf("expected '%s' but got '%s'", prevFI.String(), fi.String())
		}
	})
	require.NoError(t, err)
}

func GenValidationFile(st any, outFile string) error {
	lines := []string{
		"# file automatically updated by the release process.",
		"# if you are getting an error with this file it's likely you",
		"# have added a new minver tag with a value other than TBD",
	}

	err := structwalk.Walk(st, func(field reflect.StructField, depth int) error {
		fi := FieldInfoFromStructField(field, depth)
		lines = append(lines, fi.String())
		return nil
	})

	if err != nil {
		return fmt.Errorf("error generating data for '%s': %w", outFile, err)
	}

	fileContents := strings.Join(lines, "\n")

	err = os.WriteFile(outFile, []byte(fileContents), 0644)
	if err != nil {
		return fmt.Errorf("error writing '%s': %w", outFile, err)
	}

	return nil
}
