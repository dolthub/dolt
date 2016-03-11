// Copyright 2012-2016 Charles Banning. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

// xml.go - basically the core of X2j for map[string]interface{} values.
//          NewMapXml, NewMapXmlReader, mv.Xml, mv.XmlWriter
// see x2j and j2x for wrappers to provide end-to-end transformation of XML and JSON messages.

package mxj

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ------------------- NewMapXml & NewMapXmlReader ... -------------------------

// If XmlCharsetReader != nil, it will be used to decode the XML, if required.
//   import (
//	     charset "code.google.com/p/go-charset/charset"
//	     github.com/clbanning/mxj
//	 )
//   ...
//   mxj.XmlCharsetReader = charset.NewReader
//   m, merr := mxj.NewMapXml(xmlValue)
var XmlCharsetReader func(charset string, input io.Reader) (io.Reader, error)

// NewMapXml - convert a XML doc into a Map
// (This is analogous to unmarshalling a JSON string to map[string]interface{} using json.Unmarshal().)
//	If the optional argument 'cast' is 'true', then values will be converted to boolean or float64 if possible.
//
//	Converting XML to JSON is a simple as:
//		...
//		mapVal, merr := mxj.NewMapXml(xmlVal)
//		if merr != nil {
//			// handle error
//		}
//		jsonVal, jerr := mapVal.Json()
//		if jerr != nil {
//			// handle error
//		}
//
//	NOTES:
//	   1. The 'xmlVal' will be parsed looking for an xml.StartElement, so BOM and other
//	      extraneous xml.CharData will be ignored unless io.EOF is reached first.
//	   2. If CoerceKeysToLower() has been called, then all key values will be lower case.
func NewMapXml(xmlVal []byte, cast ...bool) (Map, error) {
	var r bool
	if len(cast) == 1 {
		r = cast[0]
	}
	return xmlToMap(xmlVal, r)
}

// Get next XML doc from an io.Reader as a Map value.  Returns Map value.
//	NOTES:
//	   1. The 'xmlReader' will be parsed looking for an xml.StartElement, so BOM and other
//	      extraneous xml.CharData will be ignored unless io.EOF is reached first.
//	   2. If CoerceKeysToLower() has been called, then all key values will be lower case.
func NewMapXmlReader(xmlReader io.Reader, cast ...bool) (Map, error) {
	var r bool
	if len(cast) == 1 {
		r = cast[0]
	}

	// build the node tree
	return xmlReaderToMap(xmlReader, r)
}

// XmlWriterBufSize - set the size of io.Writer for the TeeReader used by NewMapXmlReaderRaw()
// and HandleXmlReaderRaw().  This reduces repeated memory allocations and copy() calls in most cases.
//	NOTE: the 'xmlVal' will be parsed looking for an xml.StartElement, so BOM and other
//	      extraneous xml.CharData will be ignored unless io.EOF is reached first.
var XmlWriterBufSize int = 256

// Get next XML doc from an io.Reader as a Map value.  Returns Map value and slice with the raw XML.
//	NOTES:
//	   1. Due to the implementation of xml.Decoder, the raw XML off the reader is buffered to []byte
//	      using a ByteReader. If the io.Reader is an os.File, there may be significant performance impact.
//	      See the examples - getmetrics1.go through getmetrics4.go - for comparative use cases on a large
//	      data set. If the io.Reader is wrapping a []byte value in-memory, however, such as http.Request.Body
//	      you CAN use it to efficiently unmarshal a XML doc and retrieve the raw XML in a single call.
//	   2. The 'raw' return value may be larger than the XML text value.
//	   3. The 'xmlReader' will be parsed looking for an xml.StartElement, so BOM and other
//	      extraneous xml.CharData will be ignored unless io.EOF is reached first.
//	   4. If CoerceKeysToLower() has been called, then all key values will be lower case.
func NewMapXmlReaderRaw(xmlReader io.Reader, cast ...bool) (Map, []byte, error) {
	var r bool
	if len(cast) == 1 {
		r = cast[0]
	}
	// create TeeReader so we can retrieve raw XML
	buf := make([]byte, XmlWriterBufSize)
	wb := bytes.NewBuffer(buf)
	trdr := myTeeReader(xmlReader, wb) // see code at EOF

	// build the node tree
	m, err := xmlReaderToMap(trdr, r)

	// retrieve the raw XML that was decoded
	b := make([]byte, wb.Len())
	_, _ = wb.Read(b)

	if err != nil {
		return nil, b, err
	}

	return m, b, nil
}

// xmlReaderToMap() - parse a XML io.Reader to a map[string]interface{} value
func xmlReaderToMap(rdr io.Reader, r bool) (map[string]interface{}, error) {
	// parse the Reader
	p := xml.NewDecoder(rdr)
	p.CharsetReader = XmlCharsetReader
	return xmlToMapParser("", nil, p, r)
}

// xmlToMap - convert a XML doc into map[string]interface{} value
func xmlToMap(doc []byte, r bool) (map[string]interface{}, error) {
	b := bytes.NewReader(doc)
	p := xml.NewDecoder(b)
	p.CharsetReader = XmlCharsetReader
	return xmlToMapParser("", nil, p, r)
}

// ===================================== where the work happens =============================

// Allow people to drop hyphen when unmarshaling the XML doc.
var useHyphen bool = true

// PrependAttrWithHyphen. Prepend attribute tags with a hyphen.
// Default is 'true'.
//	Note:
//		If 'false', unmarshaling and marshaling is not symmetric. Attributes will be
//		marshal'd as <attr_tag>attr</attr_tag> and may be part of a list.
func PrependAttrWithHyphen(v bool) {
	useHyphen = v
}

// Include sequence id with inner tags. - per Sean Murphy, murphysean84@gmail.com.
var includeTagSeqNum bool

// IncludeTagSeqNum - include a "_seq":N key:value pair with each inner tag, denoting
// its position when parsed. This is of limited usefulness, since list values cannot
// be tagged with "_seq" without changing their depth in the Map.
// So THIS SHOULD BE USED WITH CAUTION - see the test cases. Here's a sample of what
// you get.
/*
		<Obj c="la" x="dee" h="da">
			<IntObj id="3"/>
			<IntObj1 id="1"/>
			<IntObj id="2"/>
			<StrObj>hello</StrObj>
		</Obj>

	parses as:

		{
		Obj:{
			"-c":"la",
			"-h":"da",
			"-x":"dee",
			"intObj":[
				{
					"-id"="3",
					"_seq":"0" // if mxj.Cast is passed, then: "_seq":0
				},
				{
					"-id"="2",
					"_seq":"2"
				}],
			"intObj1":{
				"-id":"1",
				"_seq":"1"
				},
			"StrObj":{
				"#text":"hello", // simple element value gets "#text" tag
				"_seq":"3"
				}
			}
		}
*/
func IncludeTagSeqNum(b bool) {
	includeTagSeqNum = b
}

// all keys will be "lower case"
var lowerCase bool

// Coerce all tag values to keys in lower case.  This is useful if you've got sources with variable
// tag capitalization, and you want to use m.ValuesForKeys(), etc., with the key or path spec
// in lower case.
//	CoerceKeysToLower() will toggle the coercion flag true|false - on|off
//	CoerceKeysToLower(true|false) will set the coercion flag on|off
//
//	NOTE: only recognized by NewMapXml, NewMapXmlReader, and NewMapXmlReaderRaw functions as well as
//	      the associated HandleXmlReader and HandleXmlReaderRaw.
func CoerceKeysToLower(b ...bool) {
	if len(b) == 1 {
		lowerCase = b[0]
		return
	}
	if !lowerCase {
		lowerCase = true
	} else {
		lowerCase = false
	}
}

// xmlToMapParser (2015.11.12) - load a 'clean' XML doc into a map[string]interface{} directly.
// A refactoring of xmlToTreeParser(), markDuplicate() and treeToMap() - here, all-in-one.
// We've removed the intermediate *node tree with the allocation and subsequent rescanning.
func xmlToMapParser(skey string, a []xml.Attr, p *xml.Decoder, r bool) (map[string]interface{}, error) {
	if lowerCase {
		skey = strings.ToLower(skey)
	}

	// NOTE: all attributes and sub-elements parsed into 'na', 'na' is returned as value for 'skey'
	// Unless 'skey' is a simple element w/o attributes, in which case the xml.CharData value is the value.
	var n, na map[string]interface{}
	var seq int // for includeTagSeqNum

	// Allocate maps and load attributes, if any.
	if skey != "" {
		n = make(map[string]interface{})  // old n
		na = make(map[string]interface{}) // old n.nodes
		if len(a) > 0 {
			for _, v := range a {
				var key string
				if useHyphen {
					key = `-` + v.Name.Local
				} else {
					key = v.Name.Local
				}
				if lowerCase {
					key = strings.ToLower(key)
				}
				na[key] = cast(v.Value, r)
			}
		}
	}
	for {
		t, err := p.Token()
		if err != nil {
			if err != io.EOF {
				return nil, errors.New("xml.Decoder.Token() - " + err.Error())
			}
			return nil, err
		}
		switch t.(type) {
		case xml.StartElement:
			tt := t.(xml.StartElement)

			// First call to xmlToMapParser() doesn't pass xml.StartElement - the map key.
			// So when the loop is first entered, the first token is the root tag along
			// with any attributes, which we process here.
			//
			// Subsequent calls to xmlToMapParser() will pass in tag+attributes for
			// processing before getting the next token which is the element value,
			// which is done above.
			if skey == "" {
				return xmlToMapParser(tt.Name.Local, tt.Attr, p, r)
			}

			// If not initializing the map, parse the element.
			// len(nn) == 1, necessarily - it is just an 'n'.
			nn, err := xmlToMapParser(tt.Name.Local, tt.Attr, p, r)
			if err != nil {
				return nil, err
			}

			// The nn map[string]interface{} value is a na[nn_key] value.
			// We need to see if nn_key already exists - means we're parsing a list.
			// This may require converting na[nn_key] value into []interface{} type.
			// First, extract the key:val for the map - it's a singleton.
			// Note: if CoerceKeysToLower() called, then key will be lower case.
			var key string
			var val interface{}
			for key, val = range nn {
				break
			}

			// IncludeTagSeqNum requests that the element be augmented with a "_seq" sub-element.
			// In theory, we don't need this if len(na) == 1. But, we don't know what might
			// come next - we're only parsing forward.  So if you ask for 'includeTagSeqNum' you
			// get it on every element. (Personally, I never liked this, but I added it on request
			// and did get a $50 Amazon gift card in return - now we support it for backwards compatibility!)
			if includeTagSeqNum {
				switch val.(type) {
				case []interface{}:
					// noop - There's no clean way to handle this w/o changing message structure.
				case map[string]interface{}:
					val.(map[string]interface{})["_seq"] = seq // will overwrite an "_seq" XML tag
					seq++
				case interface{}: // a non-nil simple element: string, float64, bool
					v := map[string]interface{}{"#text": val}
					v["_seq"] = seq
					seq++
					val = v
				}
			}

			// 'na' holding sub-elements of n.
			// See if 'key' already exists.
			// If 'key' exists, then this is a list, if not just add key:val to na.
			if v, ok := na[key]; ok {
				var a []interface{}
				switch v.(type) {
				case []interface{}:
					a = v.([]interface{})
				default: // anything else - note: v.(type) != nil
					a = []interface{}{v}
				}
				a = append(a, val)
				na[key] = a
			} else {
				na[key] = val // save it as a singleton
			}
		case xml.EndElement:
			// len(n) > 0 if this is a simple element w/o xml.Attrs - see xml.CharData case.
			if len(n) == 0 {
				// If len(na)==0 we have an empty element == "";
				// it has no xml.Attr nor xml.CharData.
				// Note: in original node-tree parser, val defaulted to "";
				// so we always had the default if len(node.nodes) == 0.
				if len(na) > 0 {
					n[skey] = na
				} else {
					n[skey] = "" // empty element
				}
			}
			return n, nil
		case xml.CharData:
			// clean up possible noise
			tt := strings.Trim(string(t.(xml.CharData)), "\t\r\b\n ")
			if len(tt) > 0 {
				if len(na) > 0 {
					na["#text"] = cast(tt, r)
				} else if skey != "" {
					n[skey] = cast(tt, r)
				} else {
					// per Adrian (http://www.adrianlungu.com/) catch stray text
					// in decoder stream -
					// https://github.com/clbanning/mxj/pull/14#issuecomment-182816374
					// NOTE: CharSetReader must be set to non-UTF-8 CharSet or you'll get
					// a p.Token() decoding error when the BOM is UTF-16 or UTF-32.
					continue
				}
			}
		default:
			// noop
		}
	}
}

var castNanInf bool

// Cast "Nan", "Inf", "-Inf" XML values to 'float64'.
// By default, these values will be decoded as 'string'.
func CastNanInf(b bool) {
	castNanInf = b
}

// cast - try to cast string values to bool or float64
func cast(s string, r bool) interface{} {
	if r {
		// handle nan and inf
		if !castNanInf {
			switch strings.ToLower(s) {
			case "nan", "inf", "-inf":
				return interface{}(s)
			}
		}

		// handle numeric strings ahead of boolean
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return interface{}(f)
		}
		// ParseBool treats "1"==true & "0"==false
		// but be more strick - only allow TRUE, True, true, FALSE, False, false
		if s != "t" && s != "T" && s != "f" && s != "F" {
			if b, err := strconv.ParseBool(s); err == nil {
				return interface{}(b)
			}
		}
	}
	return interface{}(s)
}

// ------------------ END: NewMapXml & NewMapXmlReader -------------------------

// ------------------ mv.Xml & mv.XmlWriter - from j2x ------------------------

const (
	DefaultRootTag = "doc"
)

var useGoXmlEmptyElemSyntax bool

// XmlGoEmptyElemSyntax() - <tag ...></tag> rather than <tag .../>.
//	Go's encoding/xml package marshals empty XML elements as <tag ...></tag>.  By default this package
//	encodes empty elements as <tag .../>.  If you're marshaling Map values that include structures
//	(which are passed to xml.Marshal for encoding), this will let you conform to the standard package.
func XmlGoEmptyElemSyntax() {
	useGoXmlEmptyElemSyntax = true
}

// XmlDefaultEmptyElemSyntax() - <tag .../> rather than <tag ...></tag>.
// Return XML encoding for empty elements to the default package setting.
// Reverses effect of XmlGoEmptyElemSyntax().
func XmlDefaultEmptyElemSyntax() {
	useGoXmlEmptyElemSyntax = false
}

// Encode a Map as XML.  The companion of NewMapXml().
// The following rules apply.
//    - The key label "#text" is treated as the value for a simple element with attributes.
//    - Map keys that begin with a hyphen, '-', are interpreted as attributes.
//      It is an error if the attribute doesn't have a []byte, string, number, or boolean value.
//    - Map value type encoding:
//          > string, bool, float64, int, int32, int64, float32: per "%v" formating
//          > []bool, []uint8: by casting to string
//          > structures, etc.: handed to xml.Marshal() - if there is an error, the element
//            value is "UNKNOWN"
//    - Elements with only attribute values or are null are terminated using "/>".
//    - If len(mv) == 1 and no rootTag is provided, then the map key is used as the root tag, possible.
//      Thus, `{ "key":"value" }` encodes as "<key>value</key>".
//    - To encode empty elements in a syntax consistent with encoding/xml call UseGoXmlEmptyElementSyntax().
// The attributes tag=value pairs are alphabetized by "tag".  Also, when encoding map[string]interface{} values -
// complex elements, etc. - the key:value pairs are alphabetized by key so the resulting tags will appear sorted.
func (mv Map) Xml(rootTag ...string) ([]byte, error) {
	m := map[string]interface{}(mv)
	var err error
	s := new(string)
	p := new(pretty) // just a stub

	if len(m) == 1 && len(rootTag) == 0 {
		for key, value := range m {
			// if it an array, see if all values are map[string]interface{}
			// we force a new root tag if we'll end up with no key:value in the list
			// so: key:[string_val, bool:true] --> <doc><key>string_val</key><bool>true</bool></doc>
			switch value.(type) {
			case []interface{}:
				for _, v := range value.([]interface{}) {
					switch v.(type) {
					case map[string]interface{}: // noop
					default: // anything else
						err = mapToXmlIndent(false, s, DefaultRootTag, m, p)
						goto done
					}
				}
			}
			err = mapToXmlIndent(false, s, key, value, p)
		}
	} else if len(rootTag) == 1 {
		err = mapToXmlIndent(false, s, rootTag[0], m, p)
	} else {
		err = mapToXmlIndent(false, s, DefaultRootTag, m, p)
	}
done:
	return []byte(*s), err
}

// The following implementation is provided only for symmetry with NewMapXmlReader[Raw]
// The names will also provide a key for the number of return arguments.

// Writes the Map as  XML on the Writer.
// See Xml() for encoding rules.
func (mv Map) XmlWriter(xmlWriter io.Writer, rootTag ...string) error {
	x, err := mv.Xml(rootTag...)
	if err != nil {
		return err
	}

	_, err = xmlWriter.Write(x)
	return err
}

// Writes the Map as  XML on the Writer. []byte is the raw XML that was written.
// See Xml() for encoding rules.
func (mv Map) XmlWriterRaw(xmlWriter io.Writer, rootTag ...string) ([]byte, error) {
	x, err := mv.Xml(rootTag...)
	if err != nil {
		return x, err
	}

	_, err = xmlWriter.Write(x)
	return x, err
}

// Writes the Map as pretty XML on the Writer.
// See Xml() for encoding rules.
func (mv Map) XmlIndentWriter(xmlWriter io.Writer, prefix, indent string, rootTag ...string) error {
	x, err := mv.XmlIndent(prefix, indent, rootTag...)
	if err != nil {
		return err
	}

	_, err = xmlWriter.Write(x)
	return err
}

// Writes the Map as pretty XML on the Writer. []byte is the raw XML that was written.
// See Xml() for encoding rules.
func (mv Map) XmlIndentWriterRaw(xmlWriter io.Writer, prefix, indent string, rootTag ...string) ([]byte, error) {
	x, err := mv.XmlIndent(prefix, indent, rootTag...)
	if err != nil {
		return x, err
	}

	_, err = xmlWriter.Write(x)
	return x, err
}

// -------------------- END: mv.Xml & mv.XmlWriter -------------------------------

// --------------  Handle XML stream by processing Map value --------------------

// Default poll delay to keep Handler from spinning on an open stream
// like sitting on os.Stdin waiting for imput.
var xhandlerPollInterval = time.Millisecond

// Bulk process XML using handlers that process a Map value.
//	'rdr' is an io.Reader for XML (stream)
//	'mapHandler' is the Map processor. Return of 'false' stops io.Reader processing.
//	'errHandler' is the error processor. Return of 'false' stops io.Reader processing and returns the error.
//	Note: mapHandler() and errHandler() calls are blocking, so reading and processing of messages is serialized.
//	      This means that you can stop reading the file on error or after processing a particular message.
//	      To have reading and handling run concurrently, pass argument to a go routine in handler and return 'true'.
func HandleXmlReader(xmlReader io.Reader, mapHandler func(Map) bool, errHandler func(error) bool) error {
	var n int
	for {
		m, merr := NewMapXmlReader(xmlReader)
		n++

		// handle error condition with errhandler
		if merr != nil && merr != io.EOF {
			merr = fmt.Errorf("[xmlReader: %d] %s", n, merr.Error())
			if ok := errHandler(merr); !ok {
				// caused reader termination
				return merr
			}
			continue
		}

		// pass to maphandler
		if len(m) != 0 {
			if ok := mapHandler(m); !ok {
				break
			}
		} else if merr != io.EOF {
			time.Sleep(xhandlerPollInterval)
		}

		if merr == io.EOF {
			break
		}
	}
	return nil
}

// Bulk process XML using handlers that process a Map value and the raw XML.
//	'rdr' is an io.Reader for XML (stream)
//	'mapHandler' is the Map and raw XML - []byte - processor. Return of 'false' stops io.Reader processing.
//	'errHandler' is the error and raw XML processor. Return of 'false' stops io.Reader processing and returns the error.
//	Note: mapHandler() and errHandler() calls are blocking, so reading and processing of messages is serialized.
//	      This means that you can stop reading the file on error or after processing a particular message.
//	      To have reading and handling run concurrently, pass argument(s) to a go routine in handler and return 'true'.
//	See NewMapXmlReaderRaw for comment on performance associated with retrieving raw XML from a Reader.
func HandleXmlReaderRaw(xmlReader io.Reader, mapHandler func(Map, []byte) bool, errHandler func(error, []byte) bool) error {
	var n int
	for {
		m, raw, merr := NewMapXmlReaderRaw(xmlReader)
		n++

		// handle error condition with errhandler
		if merr != nil && merr != io.EOF {
			merr = fmt.Errorf("[xmlReader: %d] %s", n, merr.Error())
			if ok := errHandler(merr, raw); !ok {
				// caused reader termination
				return merr
			}
			continue
		}

		// pass to maphandler
		if len(m) != 0 {
			if ok := mapHandler(m, raw); !ok {
				break
			}
		} else if merr != io.EOF {
			time.Sleep(xhandlerPollInterval)
		}

		if merr == io.EOF {
			break
		}
	}
	return nil
}

// ----------------- END: Handle XML stream by processing Map value --------------

// --------  a hack of io.TeeReader ... need one that's an io.ByteReader for xml.NewDecoder() ----------

// This is a clone of io.TeeReader with the additional method t.ReadByte().
// Thus, this TeeReader is also an io.ByteReader.
// This is necessary because xml.NewDecoder uses a ByteReader not a Reader. It appears to have been written
// with bufio.Reader or bytes.Reader in mind ... not a generic io.Reader, which doesn't have to have ReadByte()..
// If NewDecoder is passed a Reader that does not satisfy ByteReader() it wraps the Reader with
// bufio.NewReader and uses ReadByte rather than Read that runs the TeeReader pipe logic.

type teeReader struct {
	r io.Reader
	w io.Writer
	b []byte
}

func myTeeReader(r io.Reader, w io.Writer) io.Reader {
	b := make([]byte, 1)
	return &teeReader{r, w, b}
}

// need for io.Reader - but we don't use it ...
func (t *teeReader) Read(p []byte) (n int, err error) {
	return 0, nil
}

func (t *teeReader) ReadByte() (c byte, err error) {
	n, err := t.r.Read(t.b)
	if n > 0 {
		if _, err := t.w.Write(t.b[:1]); err != nil {
			return t.b[0], err
		}
	}
	return t.b[0], err
}

// ----------------------- END: io.TeeReader hack -----------------------------------

// ---------------------- XmlIndent - from j2x package ----------------------------

// Encode a map[string]interface{} as a pretty XML string.
// See Xml for encoding rules.
func (mv Map) XmlIndent(prefix, indent string, rootTag ...string) ([]byte, error) {
	m := map[string]interface{}(mv)

	var err error
	s := new(string)
	p := new(pretty)
	p.indent = indent
	p.padding = prefix

	if len(m) == 1 && len(rootTag) == 0 {
		// this can extract the key for the single map element
		// use it if it isn't a key for a list
		for key, value := range m {
			if _, ok := value.([]interface{}); ok {
				err = mapToXmlIndent(true, s, DefaultRootTag, m, p)
			} else {
				err = mapToXmlIndent(true, s, key, value, p)
			}
		}
	} else if len(rootTag) == 1 {
		err = mapToXmlIndent(true, s, rootTag[0], m, p)
	} else {
		err = mapToXmlIndent(true, s, DefaultRootTag, m, p)
	}
	return []byte(*s), err
}

type pretty struct {
	indent   string
	cnt      int
	padding  string
	mapDepth int
	start    int
}

func (p *pretty) Indent() {
	p.padding += p.indent
	p.cnt++
}

func (p *pretty) Outdent() {
	if p.cnt > 0 {
		p.padding = p.padding[:len(p.padding)-len(p.indent)]
		p.cnt--
	}
}

// where the work actually happens
// returns an error if an attribute is not atomic
func mapToXmlIndent(doIndent bool, s *string, key string, value interface{}, pp *pretty) error {
	var endTag bool
	var isSimple bool
	var elen int
	p := &pretty{pp.indent, pp.cnt, pp.padding, pp.mapDepth, pp.start}

	switch value.(type) {
	case map[string]interface{}, []byte, string, float64, bool, int, int32, int64, float32:
		if doIndent {
			*s += p.padding
		}
		*s += `<` + key
	}
	switch value.(type) {
	case map[string]interface{}:
		vv := value.(map[string]interface{})
		lenvv := len(vv)
		// scan out attributes - keys have prepended hyphen, '-'
		var cntAttr int
		attrlist := make([][2]string, len(vv))
		var n int
		for k, v := range vv {
			if k[:1] == "-" {
				cntAttr++
				switch v.(type) {
				case string, float64, bool, int, int32, int64, float32:
					attrlist[n][0] = k[1:]
					attrlist[n][1] = fmt.Sprintf("%v", v)
					n++
				case []byte:
					attrlist[n][0] = k[1:]
					attrlist[n][1] = fmt.Sprintf("%v", string(v.([]byte)))
				default:
					return fmt.Errorf("invalid attribute value for: %s", k)
				}
			}
		}
		if cntAttr > 0 {
			attrlist = attrlist[:n]
			sort.Sort(attrList(attrlist))
			for _, v := range attrlist {
				*s += ` ` + v[0] + `="` + v[1] + `"`
			}
		}

		// only attributes?
		if cntAttr == lenvv {
			break
		}
		// simple element? Note: '#text" is an invalid XML tag.
		if v, ok := vv["#text"]; ok && cntAttr+1 == lenvv {
			*s += ">" + fmt.Sprintf("%v", v)
			endTag = true
			elen = 1
			isSimple = true
			break
		}
		// close tag with possible attributes
		*s += ">"
		if doIndent {
			*s += "\n"
		}
		// something more complex
		p.mapDepth++
		// extract the map k:v pairs and sort on key
		elemlist := make([][2]interface{}, len(vv))
		n = 0
		for k, v := range vv {
			if k[:1] == "-" {
				continue
			}
			elemlist[n][0] = k
			elemlist[n][1] = v
			n++
		}
		elemlist = elemlist[:n]
		sort.Sort(elemList(elemlist))
		var i int
		for _, v := range elemlist {
			switch v[1].(type) {
			case []interface{}:
			default:
				if i == 0 && doIndent {
					p.Indent()
				}
			}
			i++
			mapToXmlIndent(doIndent, s, v[0].(string), v[1], p)
			switch v[1].(type) {
			case []interface{}: // handled in []interface{} case
			default:
				if doIndent {
					p.Outdent()
				}
			}
			i--
		}
		p.mapDepth--
		endTag = true
		elen = 1 // we do have some content ...
	case []interface{}:
		for _, v := range value.([]interface{}) {
			if doIndent {
				p.Indent()
			}
			mapToXmlIndent(doIndent, s, key, v, p)
			if doIndent {
				p.Outdent()
			}
		}
		return nil
	case nil:
		// terminate the tag
		*s += "<" + key
		break
	default: // handle anything - even goofy stuff
		elen = 0
		switch value.(type) {
		case string, float64, bool, int, int32, int64, float32:
			v := fmt.Sprintf("%v", value)
			elen = len(v)
			if elen > 0 {
				*s += ">" + v
			}
		case []byte: // NOTE: byte is just an alias for uint8
			// similar to how xml.Marshal handles []byte structure members
			v := string(value.([]byte))
			elen = len(v)
			if elen > 0 {
				*s += ">" + v
			}
		default:
			var v []byte
			var err error
			if doIndent {
				v, err = xml.MarshalIndent(value, p.padding, p.indent)
			} else {
				v, err = xml.Marshal(value)
			}
			if err != nil {
				*s += ">UNKNOWN"
			} else {
				elen = len(v)
				if elen > 0 {
					*s += string(v)
				}
			}
		}
		isSimple = true
		endTag = true
	}
	if endTag {
		if doIndent {
			if !isSimple {
				*s += p.padding
			}
		}
		switch value.(type) {
		case map[string]interface{}, []byte, string, float64, bool, int, int32, int64, float32:
			if elen > 0 || useGoXmlEmptyElemSyntax {
				if elen == 0 {
					*s += ">"
				}
				*s += `</` + key + ">"
			} else {
				*s += `/>`
			}
		}
	} else if useGoXmlEmptyElemSyntax {
		*s += "></" + key + ">"
	} else {
		*s += "/>"
	}
	if doIndent {
		if p.cnt > p.start {
			*s += "\n"
		}
		p.Outdent()
	}

	return nil
}

// ============================ sort interface implementation =================

type attrList [][2]string

func (a attrList) Len() int {
	return len(a)
}

func (a attrList) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a attrList) Less(i, j int) bool {
	if a[i][0] > a[j][0] {
		return false
	}
	return true
}

type elemList [][2]interface{}

func (e elemList) Len() int {
	return len(e)
}

func (e elemList) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func (e elemList) Less(i, j int) bool {
	if e[i][0].(string) > e[j][0].(string) {
		return false
	}
	return true
}
