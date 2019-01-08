// mxj - A collection of map[string]interface{} and associated XML and JSON utilities.
// Copyright 2012-2015 Charles Banning. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

/*
Marshal/Unmarshal XML to/from map[string]interface{} values (and JSON); extract/modify values from maps by key or key-path, including wildcards.

mxj supplants the legacy x2j and j2x packages. The subpackage x2j-wrapper is provided to facilitate migrating from the x2j package.  The x2j and j2x subpackages provide similar functionality of the old packages but are not function-name compatible with them.

Note: this library was designed for processing ad hoc anonymous messages.  Bulk processing large data sets may be much more efficiently performed using the encoding/xml or encoding/json packages from Go's standard library directly.

Related Packages:
	checkxml: github.com/clbanning/checkxml provides functions for validating XML data.

Notes:
	2018.04.18: mv.Xml/mv.XmlIndent encodes non-map[string]interface{} map values - map[string]string, map[int]uint, etc.
	2018.03.29: mv.Gob/NewMapGob support gob encoding/decoding of Maps.
	2018.03.26: Added mxj/x2j-wrapper sub-package for migrating from legacy x2j package.
	2017.02.22: LeafNode paths can use ".N" syntax rather than "[N]" for list member indexing.
	2017.02.21: github.com/clbanning/checkxml provides functions for validating XML data.
	2017.02.10: SetFieldSeparator changes field separator for args in UpdateValuesForPath, ValuesFor... methods.
	2017.02.06: Support XMPP stream processing - HandleXMPPStreamTag().
	2016.11.07: Preserve name space prefix syntax in XmlSeq parser - NewMapXmlSeq(), etc.
	2016.06.25: Support overriding default XML attribute prefix, "-", in Map keys - SetAttrPrefix().
	2016.05.26: Support customization of xml.Decoder by exposing CustomDecoder variable.
	2016.03.19: Escape invalid chars when encoding XML attribute and element values - XMLEscapeChars().
	2016.03.02: By default decoding XML with float64 and bool value casting will not cast "NaN", "Inf", and "-Inf".
	            To cast them to float64, first set flag with CastNanInf(true).
	2016.02.22: New mv.Root(), mv.Elements(), mv.Attributes methods let you examine XML document structure.
	2016.02.16: Add CoerceKeysToLower() option to handle tags with mixed capitalization.
	2016.02.12: Seek for first xml.StartElement token; only return error if io.EOF is reached first (handles BOM).
	2015-12-02: NewMapXmlSeq() with mv.XmlSeq() & co. will try to preserve structure of XML doc when re-encoding.
	2014-08-02: AnyXml() and AnyXmlIndent() will try to marshal arbitrary values to XML.

SUMMARY

   type Map map[string]interface{}

   Create a Map value, 'mv', from any map[string]interface{} value, 'v':
      mv := Map(v)

   Unmarshal / marshal XML as a Map value, 'mv':
      mv, err := NewMapXml(xmlValue) // unmarshal
      xmlValue, err := mv.Xml()      // marshal

   Unmarshal XML from an io.Reader as a Map value, 'mv':
      mv, err := NewMapXmlReader(xmlReader)         // repeated calls, as with an os.File Reader, will process stream
      mv, raw, err := NewMapXmlReaderRaw(xmlReader) // 'raw' is the raw XML that was decoded

   Marshal Map value, 'mv', to an XML Writer (io.Writer):
      err := mv.XmlWriter(xmlWriter)
      raw, err := mv.XmlWriterRaw(xmlWriter) // 'raw' is the raw XML that was written on xmlWriter

   Also, for prettified output:
      xmlValue, err := mv.XmlIndent(prefix, indent, ...)
      err := mv.XmlIndentWriter(xmlWriter, prefix, indent, ...)
      raw, err := mv.XmlIndentWriterRaw(xmlWriter, prefix, indent, ...)

   Bulk process XML with error handling (note: handlers must return a boolean value):
      err := HandleXmlReader(xmlReader, mapHandler(Map), errHandler(error))
      err := HandleXmlReaderRaw(xmlReader, mapHandler(Map, []byte), errHandler(error, []byte))

   Converting XML to JSON: see Examples for NewMapXml and HandleXmlReader.

   There are comparable functions and methods for JSON processing.

   Arbitrary structure values can be decoded to / encoded from Map values:
      mv, err := NewMapStruct(structVal)
      err := mv.Struct(structPointer)

   To work with XML tag values, JSON or Map key values or structure field values, decode the XML, JSON
   or structure to a Map value, 'mv', or cast a map[string]interface{} value to a Map value, 'mv', then:
      paths := mv.PathsForKey(key)
      path := mv.PathForKeyShortest(key)
      values, err := mv.ValuesForKey(key, subkeys)
      values, err := mv.ValuesForPath(path, subkeys) // 'path' can be dot-notation with wildcards and indexed arrays.
      count, err := mv.UpdateValuesForPath(newVal, path, subkeys)

   Get everything at once, irrespective of path depth:
      leafnodes := mv.LeafNodes()
      leafvalues := mv.LeafValues()

   A new Map with whatever keys are desired can be created from the current Map and then encoded in XML
   or JSON. (Note: keys can use dot-notation. 'oldKey' can also use wildcards and indexed arrays.)
      newMap, err := mv.NewMap("oldKey_1:newKey_1", "oldKey_2:newKey_2", ..., "oldKey_N:newKey_N")
      newXml, err := newMap.Xml()   // for example
      newJson, err := newMap.Json() // ditto

XML PARSING CONVENTIONS

   Using NewMapXml()

   - Attributes are parsed to `map[string]interface{}` values by prefixing a hyphen, `-`,
     to the attribute label. (Unless overridden by `PrependAttrWithHyphen(false)` or
     `SetAttrPrefix()`.)
   - If the element is a simple element and has attributes, the element value
     is given the key `#text` for its `map[string]interface{}` representation.  (See
     the 'atomFeedString.xml' test data, below.)
   - XML comments, directives, and process instructions are ignored.
   - If CoerceKeysToLower() has been called, then the resultant keys will be lower case.

   Using NewMapXmlSeq()

   - Attributes are parsed to `map["#attr"]map[<attr_label>]map[string]interface{}`values
     where the `<attr_label>` value has "#text" and "#seq" keys - the "#text" key holds the 
     value for `<attr_label>`.
   - All elements, except for the root, have a "#seq" key.
   - Comments, directives, and process instructions are unmarshalled into the Map using the
     keys "#comment", "#directive", and "#procinst", respectively. (See documentation for more
     specifics.)
   - Name space syntax is preserved: 
      - <ns:key>something</ns.key> parses to map["ns:key"]interface{}{"something"}
      - xmlns:ns="http://myns.com/ns" parses to map["xmlns:ns"]interface{}{"http://myns.com/ns"}

   Both

   - By default, "Nan", "Inf", and "-Inf" values are not cast to float64.  If you want them
     to be cast, set a flag to cast them  using CastNanInf(true).

XML ENCODING CONVENTIONS
   
   - 'nil' Map values, which may represent 'null' JSON values, are encoded as "<tag/>".
     NOTE: the operation is not symmetric as "<tag/>" elements are decoded as 'tag:""' Map values,
           which, then, encode in JSON as '"tag":""' values..
   - ALSO: there is no guarantee that the encoded XML doc will be the same as the decoded one.  (Go
           randomizes the walk through map[string]interface{} values.) If you plan to re-encode the
           Map value to XML and want the same sequencing of elements look at NewMapXmlSeq() and
           mv.XmlSeq() - these try to preserve the element sequencing but with added complexity when
           working with the Map representation.

*/
package mxj
