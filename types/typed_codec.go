// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"encoding/json"
	"io"
	"regexp"
	"strings"

	"github.com/attic-labs/noms/d"
)

var (
	typedTag = []byte("t ")
)

func appendCompactedJSON(dst io.Writer, v interface{}) {
	buff := &bytes.Buffer{}
	err := json.NewEncoder(buff).Encode(v)
	d.Exp.NoError(err)
	buff2 := &bytes.Buffer{}
	err = json.Compact(buff2, buff.Bytes())
	d.Chk.NoError(err)
	s := normalizeJSONStrings(buff2.String())
	_, err = io.Copy(dst, strings.NewReader(s))
	d.Chk.NoError(err)
}

var unescapeHTMLRegexp = regexp.MustCompile(`\\u[0-9a-z]{4}`)

// normalizeJSONStrings undoes the HTMLEscape that the Go JSON encoder does.
func normalizeJSONStrings(s string) string {
	return unescapeHTMLRegexp.ReplaceAllStringFunc(s, func(s string) string {
		switch s {
		case `\u003c`:
			return "<"
		case `\u003e`:
			return ">"
		case `\u0026`:
			return "&"
		case `\u2028`:
			return "\u2028"
		case `\u2029`:
			return "\u2029"
		}
		return s
	})
}

func typedEncode(dst io.Writer, v interface{}) {
	_, err := dst.Write(typedTag)
	d.Exp.NoError(err)
	appendCompactedJSON(dst, v)
	return
}

func typedDecode(reader io.Reader) []interface{} {
	prefix := make([]byte, len(typedTag))
	_, err := io.ReadFull(reader, prefix)
	d.Exp.NoError(err)

	// Since typedDecode is private, and Decode() should have checked this, it is invariant that the prefix will match.
	d.Chk.EqualValues(typedTag[:], prefix, "Cannot typedDecode - invalid prefix")

	var v []interface{}
	err = json.NewDecoder(reader).Decode(&v)
	d.Exp.NoError(err)

	return v
}
