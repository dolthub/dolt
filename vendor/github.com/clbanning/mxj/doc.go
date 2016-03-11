// mxj - A collection of map[string]interface{} and associated XML and JSON utilities.
// Copyright 2012-2015 Charles Banning. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

/*
Marshal/Unmarshal XML to/from JSON and map[string]interface{} values, and extract/modify values from maps by key or key-path, including wildcards.

mxj supplants the legacy x2j and j2x packages. If you want the old syntax, use mxj/x2j or mxj/j2x packages.

Note: this library was designed for processing ad hoc anonymous messages.  Bulk processing large data sets may be much more efficiently performed using the encoding/xml or encoding/json packages from Go's standard library directly.

Note:
	2015-12-02: NewMapXmlSeq() with mv.XmlSeq() & co. will try to preserve structure of XML doc when re-encoding.
	2014-08-02: AnyXml() and AnyXmlIndent() will try to marshal arbitrary values to XML.

SUMMARY

   type Map map[string]interface{}

   Create a Map value, 'm', from any map[string]interface{} value, 'v':
      m := Map(v)

   Unmarshal / marshal XML as a Map value, 'm':
      m, err := NewMapXml(xmlValue) // unmarshal
      xmlValue, err := m.Xml()      // marshal

   Unmarshal XML from an io.Reader as a Map value, 'm':
      m, err := NewMapReader(xmlReader)         // repeated calls, as with an os.File Reader, will process stream
      m, raw, err := NewMapReaderRaw(xmlReader) // 'raw' is the raw XML that was decoded

   Marshal Map value, 'm', to an XML Writer (io.Writer):
      err := m.XmlWriter(xmlWriter)
      raw, err := m.XmlWriterRaw(xmlWriter) // 'raw' is the raw XML that was written on xmlWriter

   Also, for prettified output:
      xmlValue, err := m.XmlIndent(prefix, indent, ...)
      err := m.XmlIndentWriter(xmlWriter, prefix, indent, ...)
      raw, err := m.XmlIndentWriterRaw(xmlWriter, prefix, indent, ...)

   Bulk process XML with error handling (note: handlers must return a boolean value):
      err := HandleXmlReader(xmlReader, mapHandler(Map), errHandler(error))
      err := HandleXmlReaderRaw(xmlReader, mapHandler(Map, []byte), errHandler(error, []byte))

   Converting XML to JSON: see Examples for NewMapXml and HandleXmlReader.

   There are comparable functions and methods for JSON processing.

   Arbitrary structure values can be decoded to / encoded from Map values:
      m, err := NewMapStruct(structVal)
      err := m.Struct(structPointer)

   To work with XML tag values, JSON or Map key values or structure field values, decode the XML, JSON
   or structure to a Map value, 'm', or cast a map[string]interface{} value to a Map value, 'm', then:
      paths := m.PathsForKey(key)
      path := m.PathForKeyShortest(key)
      values, err := m.ValuesForKey(key, subkeys)
      values, err := m.ValuesForPath(path, subkeys) // 'path' can be dot-notation with wildcards and indexed arrays.
      count, err := m.UpdateValuesForPath(newVal, path, subkeys)

   Get everything at once, irrespective of path depth:
      leafnodes := m.LeafNodes()
      leafvalues := m.LeafValues()

   A new Map with whatever keys are desired can be created from the current Map and then encoded in XML
   or JSON. (Note: keys can use dot-notation. 'oldKey' can also use wildcards and indexed arrays.)
      newMap, err := m.NewMap("oldKey_1:newKey_1", "oldKey_2:newKey_2", ..., "oldKey_N:newKey_N")
      newXml, err := newMap.Xml()   // for example
      newJson, err := newMap.Json() // ditto

XML PARSING CONVENTIONS

   Using NewXml()

   - Attributes are parsed to `map[string]interface{}` values by prefixing a hyphen, `-`,
     to the attribute label. (Unless overridden by `PrependAttrWithHyphen(false)`.)
   - If the element is a simple element and has attributes, the element value
     is given the key `#text` for its `map[string]interface{}` representation.  (See
     the 'atomFeedString.xml' test data, below.)
   - XML comments, directives, and process instructions are ignored.
   - If CoerceKeysToLower() has been called, then the resultant keys will be lower case.

   Using NewXmlSeq()

   - Attributes are parsed to `map["#attr"]map[<attr_label>]map[string]interface{}`values
     where the `<attr_label>` value has "#text" and "#seq" keys - the "#text" key holds the 
     value for `<attr_label>`.
   - All elements, except for the root, have a "#seq" key.
   - Comments, directives, and process instructions are unmarshalled into the Map using the
     keys "#comment", "#directive", and "#procinst", respectively. (See documentation for more
     specifics.)

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
           m.XmlSeq() - these try to preserve the element sequencing but with added complexity when
           working with the Map representation.

*/
package mxj
