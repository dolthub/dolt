// Copyright 2020 Liquidata, Inc.
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

package ref

import (
	"regexp"
	"strings"
)

// The following list of patterns are all forbidden in a tag name.
var InvalidTagNameRegex = regexp.MustCompile(strings.Join([]string{
	// Any appearance of a period, currently unsupported by noms layer
	//`[.*]`,
	// Any appearance of the following characters: :, ?, [, \, ^, ~, SPACE, TAB, *
	`:`, `\?`, `\[`, `\\`, `\^`, `~`, ` `, `\t`, `\*`,
	// Any ASCII control character.
	`[\x00-\x1f]`, `\x7f`,
	// Any component ending with ".lock"
	`\.lock\z`, `\.lock\/`,
	// An exact name of "", "HEAD" or "-"
	`\A\z`, `\AHEAD\z`, `\A-\z`,
	// A name that looks exactly like a commit id
	`\A[0-9a-v]{32}\z`,
	// Any appearance of ".." or "@{"
	`\.\.`, `@{`,
	// Any empty component; that is, starting or ending with "/" or any appearance of "//"
	`\/\/`, `\A\/`, `\/\z`,
}, "|"))

// IsValidTagName validates that tagName passes naming constraints.
func IsValidTagName(tagName string) bool {
	return !InvalidTagNameRegex.MatchString(tagName)
}

type TagRef struct {
	tag string
}

var _ DoltRef = TagRef{}

// NewTagRef creates a reference to a local tag from a tag name or a tag ref e.g. v1, or refs/tag/v1
func NewTagRef(tagName string) TagRef {
	if IsRef(tagName) {
		prefix := PrefixForType(TagRefType)
		if strings.HasPrefix(tagName, prefix) {
			tagName = tagName[len(prefix):]
		} else {
			panic(tagName + " is a ref that is not of type " + prefix)
		}
	}

	return TagRef{tagName}
}

// GetType will return TagRefType
func (br TagRef) GetType() RefType {
	return TagRefType
}

// GetPath returns the name of the tag
func (br TagRef) GetPath() string {
	return br.tag
}

// String returns the fully qualified reference name e.g. refs/heads/master
func (br TagRef) String() string {
	return String(br)
}

// MarshalJSON serializes a TagRef to JSON.
func (br TagRef) MarshalJSON() ([]byte, error) {
	return MarshalJSON(br)
}
