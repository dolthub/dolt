// Copyright 2016 Charles Banning. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

// strict.go actually addresses setting xml.Decoder attribute
// values.  This'll let you parse non-standard XML.

package mxj

import (
	"encoding/xml"
)

// CustomDecoder can be used to specify xml.Decoder attribute
// values, e.g., Strict:false, to be used.  By default CustomDecoder
// is nil.  If CustomeDecoder != nil, then mxj.XmlCharsetReader variable is
// ignored and must be set as part of the CustomDecoder value, if needed.
//	Usage:
//		mxj.CustomDecoder = &xml.Decoder{Strict:false}
var CustomDecoder *xml.Decoder

// useCustomDecoder copy over public attributes from customDecoder
func useCustomDecoder(d *xml.Decoder) {
	d.Strict = CustomDecoder.Strict
	d.AutoClose = CustomDecoder.AutoClose
	d.Entity = CustomDecoder.Entity
	d.CharsetReader = CustomDecoder.CharsetReader
	d.DefaultSpace = CustomDecoder.DefaultSpace
}

