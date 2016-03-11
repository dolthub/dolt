<h2>mxj - to/from maps, XML and JSON</h2>
Decode/encode XML to/from map[string]interface{} (or JSON) values, and extract/modify values from maps by key or key-path, including wildcards.

mxj supplants the legacy x2j and j2x packages. If you want the old syntax, use mxj/x2j and mxj/j2x packages.

<h4>Refactor Decoder - 2015.11.15</h4>
For over a year I've wanted to refactor the XML-to-map[string]interface{} decoder to make it more performant.  I recently took the time to do that, since we were using github.com/clbanning/mxj in a production system that could be deployed on a Raspberry Pi.  Now the decoder is comparable to the stdlib JSON-to-map[string]interface{} decoder in terms of its additional processing overhead relative to decoding to a structure value.  As shown by:

	BenchmarkNewMapXml-4         	  100000	     18043 ns/op
	BenchmarkNewStructXml-4      	  100000	     14892 ns/op
	BenchmarkNewMapJson-4        	  300000	      4633 ns/op
	BenchmarkNewStructJson-4     	  300000	      3427 ns/op
	BenchmarkNewMapXmlBooks-4    	   20000	     82850 ns/op
	BenchmarkNewStructXmlBooks-4 	   20000	     67822 ns/op
	BenchmarkNewMapJsonBooks-4   	  100000	     17222 ns/op
	BenchmarkNewStructJsonBooks-4	  100000	     15309 ns/op

<h4>Notices</h4>
	2016.03.02: By default decoding XML with float64 and bool value casting will not cast "NaN", "Inf", and "-Inf".
	            To cast them to float64, first set flag with CastNanInf(true).
	2016.02.22: New m.Root(), m.Elements(), m.Attributes methods let you examine XML document structure.
	2016.02.16: Add CoerceKeysToLower() option to handle tags with mixed capitalization.
	2016.02.12: Seek for first xml.StartElement token; only return error if io.EOF is reached first (handles BOM).
	2015.12.02: EXPERIMENTAL XML decoding/encoding that preserves original structure of document. See
	            NewMapXmlSeq() and mv.XmlSeq() / mv.XmlSeqIndent().
	2015-05-20: New: mv.StringIndentNoTypeInfo().
	            Also, alphabetically sort map[string]interface{} values by key to prettify output for mv.Xml(),
	            mv.XmlIndent(), mv.StringIndent(), mv.StringIndentNoTypeInfo().
	2014-11-09: IncludeTagSeqNum() adds "_seq" key with XML doc positional information.
	            (NOTE: PreserveXmlList() is similar and will be here soon.)
	2014-09-18: inspired by NYTimes fork, added PrependAttrWithHyphen() to allow stripping hyphen from attribute tag.
	2014-08-02: AnyXml() and AnyXmlIndent() will try to marshal arbitrary values to XML.
	2014-04-28: ValuesForPath() and NewMap() now accept path with indexed array references.

<h4>Basic Unmarshal XML to map[string]interface{}</h4>
<pre>type Map map[string]interface{}</pre>

Create a `Map` value, 'm', from any `map[string]interface{}` value, 'v':
<pre>m := Map(v)</pre>

Unmarshal / marshal XML as a `Map` value, 'm':
<pre>m, err := NewMapXml(xmlValue) // unmarshal
xmlValue, err := m.Xml()      // marshal</pre>

Unmarshal XML from an `io.Reader` as a `Map` value, 'm':
<pre>m, err := NewMapReader(xmlReader)         // repeated calls, as with an os.File Reader, will process stream
m, raw, err := NewMapReaderRaw(xmlReader) // 'raw' is the raw XML that was decoded</pre>

Marshal `Map` value, 'm', to an XML Writer (`io.Writer`):
<pre>err := m.XmlWriter(xmlWriter)
raw, err := m.XmlWriterRaw(xmlWriter) // 'raw' is the raw XML that was written on xmlWriter</pre>
   
Also, for prettified output:
<pre>xmlValue, err := m.XmlIndent(prefix, indent, ...)
err := m.XmlIndentWriter(xmlWriter, prefix, indent, ...)
raw, err := m.XmlIndentWriterRaw(xmlWriter, prefix, indent, ...)</pre>

Bulk process XML with error handling (note: handlers must return a boolean value):
<pre>err := HandleXmlReader(xmlReader, mapHandler(Map), errHandler(error))
err := HandleXmlReaderRaw(xmlReader, mapHandler(Map, []byte), errHandler(error, []byte))</pre>

Converting XML to JSON: see Examples for `NewMapXml` and `HandleXmlReader`.

There are comparable functions and methods for JSON processing.

Arbitrary structure values can be decoded to / encoded from `Map` values:
<pre>m, err := NewMapStruct(structVal)
err := m.Struct(structPointer)</pre>

<h4>Extract / modify Map values</h4>
To work with XML tag values, JSON or Map key values or structure field values, decode the XML, JSON
or structure to a `Map` value, 'm', or cast a `map[string]interface{}` value to a `Map` value, 'm', then:
<pre>paths := m.PathsForKey(key)
path := m.PathForKeyShortest(key)
values, err := m.ValuesForKey(key, subkeys)
values, err := m.ValuesForPath(path, subkeys)
count, err := m.UpdateValuesForPath(newVal, path, subkeys)</pre>

Get everything at once, irrespective of path depth:
<pre>leafnodes := m.LeafNodes()
leafvalues := m.LeafValues()</pre>

A new `Map` with whatever keys are desired can be created from the current `Map` and then encoded in XML
or JSON. (Note: keys can use dot-notation.)
<pre>newMap, err := m.NewMap("oldKey_1:newKey_1", "oldKey_2:newKey_2", ..., "oldKey_N:newKey_N")
newXml, err := newMap.Xml()   // for example
newJson, err := newMap.Json() // ditto</pre>

<h4>Usage</h4>

The package is fairly well self-documented with examples. (http://godoc.org/github.com/clbanning/mxj)

Also, the subdirectory "examples" contains a wide range of examples, several taken from golang-nuts discussions.

<h4>XML parsing conventions</h4>

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

<h4>XML encoding conventions</h4>

   - 'nil' `Map` values, which may represent 'null' JSON values, are encoded as `<tag/>`.
     NOTE: the operation is not symmetric as `<tag/>` elements are decoded as `tag:""` `Map` values,
           which, then, encode in JSON as `"tag":""` values.
   - ALSO: there is no guarantee that the encoded XML doc will be the same as the decoded one.  (Go
           randomizes the walk through map[string]interface{} values.) If you plan to re-encode the
           Map value to XML and want the same sequencing of elements look at NewMapXmlSeq() and
           m.XmlSeq() - these try to preserve the element sequencing but with added complexity when
           working with the Map representation.

<h4>Running "go test"</h4>

Because there are no guarantees on the sequence map elements are retrieved, the tests have been 
written for visual verification in most cases.  One advantage is that you can easily use the 
output from running "go test" as examples of calling the various functions and methods.

<h4>Motivation</h4>

I make extensive use of JSON for messaging and typically unmarshal the messages into
`map[string]interface{}` values.  This is easily done using `json.Unmarshal` from the
standard Go libraries.  Unfortunately, many legacy solutions use structured
XML messages; in those environments the applications would have to be refactored to
interoperate with my components.

The better solution is to just provide an alternative HTTP handler that receives
XML messages and parses it into a `map[string]interface{}` value and then reuse
all the JSON-based code.  The Go `xml.Unmarshal()` function does not provide the same
option of unmarshaling XML messages into `map[string]interface{}` values. So I wrote
a couple of small functions to fill this gap and released them as the x2j package.

Over the next year and a half additional features were added, and the companion j2x
package was released to address XML encoding of arbitrary JSON and `map[string]interface{}`
values.  As part of a refactoring of our production system and looking at how we had been
using the x2j and j2x packages we found that we rarely performed direct XML-to-JSON or
JSON-to_XML conversion and that working with the XML or JSON as `map[string]interface{}`
values was the primary value.  Thus, everything was refactored into the mxj package.

