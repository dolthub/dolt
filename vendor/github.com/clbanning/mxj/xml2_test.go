package mxj

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestXml2Header(t *testing.T) {
	fmt.Println("\n----------------  xml2_test.go ...\n")
}

func TestNewMapXml4(t *testing.T) {
	x := []byte(`<doc> 
   <books>
      <book seq="1">
         <author>William T. Gaddis</author>
         <title>The Recognitions</title>
         <review>One of the great seminal American novels of the 20th century.</review>
      </book>
      <book seq="2">
         <author>Austin Tappan Wright</author>
         <title>Islandia</title>
         <review>An example of earlier 20th century American utopian fiction.</review>
      </book>
      <book seq="3">
         <author>John Hawkes</author>
         <title>The Beetle Leg</title>
         <review>A lyrical novel about the construction of Ft. Peck Dam in Montana.</review>
      </book>
      <book seq="4"> 
         <author>
            <first_name>T.E.</first_name>
            <last_name>Porter</last_name>
         </author>
         <title>King's Day</title>
         <review>A magical novella.</review>
      </book>
   </books>
</doc>`)

	m, err := NewMapXml(x)
	if err != nil && err != io.EOF {
		t.Fatal("err:", err.Error())
	}
	fmt.Println("NewMapXml4, x:\n", string(x))
	fmt.Println("NewMapXml4, m:\n", m)
	fmt.Println("NewMapXml4, s:\n", m.StringIndent())
	b, err := m.XmlIndent("", "  ")
	if err != nil {
		t.Fatal("err:", err)
	}
	fmt.Println("NewMapXml4, b:\n", string(b))
}

func TestNewMapXml5(t *testing.T) {
	fh, err := os.Open("songtext.xml")
	if err != nil {
		t.Fatal("err:", err.Error())
	}
	defer fh.Close()

	m, raw, err := NewMapXmlReaderRaw(fh)
	if err != nil && err != io.EOF {
		t.Fatal("err:", err.Error())
	}
	fmt.Println("NewMapXml5, raw:\n", string(raw))
	fmt.Println("NewMapXml5, m:\n", m)
	fmt.Println("NewMapXml5, s:\n", m.StringIndent())
	b, err := m.Xml()
	if err != nil {
		t.Fatal("err:", err)
	}
	fmt.Println("NewMapXml5, b:\n", string(b))
	b, err = m.XmlIndent("", "  ")
	if err != nil {
		t.Fatal("err:", err)
	}
	fmt.Println("NewMapXml5, b:\n", string(b))
}

func TestNewMapXml6(t *testing.T) {
	fh, err := os.Open("atomFeedString.xml")
	if err != nil {
		t.Fatal("err:", err.Error())
	}
	defer fh.Close()

	m, raw, err := NewMapXmlReaderRaw(fh)
	if err != nil && err != io.EOF {
		t.Fatal("err:", err.Error())
	}
	fmt.Println("NewMapXml6, raw:\n", string(raw))
	fmt.Println("NewMapXml6, m:\n", m)
	fmt.Println("NewMapXml6, s:\n", m.StringIndent())
	b, err := m.Xml()
	if err != nil {
		t.Fatal("err:", err)
	}
	fmt.Println("NewMapXml6, b:\n", string(b))
	b, err = m.XmlIndent("", "  ")
	if err != nil {
		t.Fatal("err:", err)
	}
	fmt.Println("NewMapXml6, b:\n", string(b))
}

// ===================================== benchmarks ============================

var smallxml = []byte(`
	<doc>
		<words>
			<word1>this</word1>
			<word2>is</word2>
			<word3>the</word3>
			<word4>end</word4>
		</words>
	</doc>
`)

var smalljson = []byte(`{"doc":{"words":{"word1":"this","word2":"is","word3":"the","word4":"end"}}}`)

type words struct {
	Word1 string `xml:"word1"`
	Word2 string `xml:"word2"`
	Word3 string `xml:"word3"`
	Word4 string `xml:"word4"`
}

type xmldoc struct {
	Words words `xml:"words"`
}

type jsondoc struct {
	Doc xmldoc
}

func BenchmarkNewMapXml(b *testing.B) {
	// var m Map
	var err error
	for i := 0; i < b.N; i++ {
		if _, err = NewMapXml(smallxml); err != nil {
			b.Fatal("err:", err)
		}
	}
	// fmt.Println("m Map:", m)
}

func BenchmarkNewStructXml(b *testing.B) {
	var s *xmldoc
	var err error
	for i := 0; i < b.N; i++ {
		s = new(xmldoc)
		if err = xml.Unmarshal(smallxml, s); err != nil {
			b.Fatal("err:", err)
		}
	}
	// fmt.Println("s xmldoc:", *s)
}

func BenchmarkNewMapJson(b *testing.B) {
	var m map[string]interface{}
	var err error
	for i := 0; i < b.N; i++ {
		m = make(map[string]interface{})
		if err = json.Unmarshal(smalljson, &m); err != nil {
			b.Fatal("err:", err)
		}
	}
	// fmt.Println("m map:", m)
}

func BenchmarkNewStructJson(b *testing.B) {
	var s *jsondoc
	var err error
	for i := 0; i < b.N; i++ {
		s = new(jsondoc)
		if err = json.Unmarshal(smalljson, s); err != nil {
			b.Fatal("err:", err)
		}
	}
	// fmt.Println("s jsondoc:", *s)
}

// ================== something with a little more content ... ===================

var xmlbooks = []byte(`
<doc> 
   <books>
      <book seq="1">
         <author>William T. Gaddis</author>
         <title>The Recognitions</title>
         <review>One of the great seminal American novels of the 20th century.</review>
      </book>
      <book seq="2">
         <author>Austin Tappan Wright</author>
         <title>Islandia</title>
         <review>An example of earlier 20th century American utopian fiction.</review>
      </book>
      <book seq="3">
         <author>John Hawkes</author>
         <title>The Beetle Leg</title>
         <review>A lyrical novel set during the construction of Ft. Peck Dam in Montana.</review>
      </book>
      <book seq="4"> 
         <author>
            <first_name>T.E.</first_name>
            <last_name>Porter</last_name>
         </author>
         <title>King's Day</title>
         <review>A magical novella.</review>
      </book>
   </books>
</doc>
`)

var jsonbooks = []byte(`
{"doc":
	{"books":
		{"book":[
			{	"author":"William T. Gaddis",
				"title":"The Recognitions",
				"review":"One of the great seminal American novels of the 20th century."
			},
			{	"author":"Austin Tappan Wright",
				"title":"Islandia",
				"review":"An example of earlier 20th century American utopian fiction."
			},
			{	"author":"John Hawkes",
				"title":"The Beetle Leg",
				"review":"A lyrical novel set during the construction of Ft. Peck Dam in Montana."
			},
			{	"author":{"first_name":"T.E.", "last_name":"Porter"},
				"title":"King's Day",
				"review":"A magical novella."
			}]
		}
	}
}
`)

type book struct {
	Author string `xml:"author"`
	Title  string `xml:"title"`
	Review string `xml:"review"`
}

type books struct {
	Book []book `xml:"book"`
}

type doc struct {
	Books books `xml:"books"`
}

type jsonbook struct {
	Author json.RawMessage
	Title  string
	Review string
}

type jsonbooks2 struct {
	Book []jsonbook
}

type jsondoc1 struct {
	Books jsonbooks2
}

type jsondoc2 struct {
	Doc jsondoc1
}

func BenchmarkNewMapXmlBooks(b *testing.B) {
	// var m Map
	var err error
	for i := 0; i < b.N; i++ {
		if _, err = NewMapXml(xmlbooks); err != nil {
			b.Fatal("err:", err)
		}
	}
	// fmt.Println("m Map:", m)
}

func BenchmarkNewStructXmlBooks(b *testing.B) {
	var s *doc
	var err error
	for i := 0; i < b.N; i++ {
		s = new(doc)
		if err = xml.Unmarshal(xmlbooks, s); err != nil {
			b.Fatal("err:", err)
		}
	}
	// fmt.Println("s doc:", *s)
}

func BenchmarkNewMapJsonBooks(b *testing.B) {
	var m map[string]interface{}
	var err error
	for i := 0; i < b.N; i++ {
		m = make(map[string]interface{})
		if err = json.Unmarshal(jsonbooks, &m); err != nil {
			b.Fatal("err:", err)
		}
	}
	// fmt.Println("m map:", m)
}

func BenchmarkNewStructJsonBooks(b *testing.B) {
	var s *jsondoc2
	var err error
	for i := 0; i < b.N; i++ {
		s = new(jsondoc2)
		if err = json.Unmarshal(jsonbooks, s); err != nil {
			b.Fatal("err:", err)
		}
	}
	// fmt.Println("s jsondoc2:", *s)
}
