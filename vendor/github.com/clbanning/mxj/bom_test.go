// bomxml.go - test handling Byte-Order-Mark headers

package mxj

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

// Check for Byte-Order-Mark header.
var boms = [][]byte{
	{'\xef', '\xbb', '\xbf'},
	{'\xfe', '\xff'},
	{'\xff', '\xfe'},
	{'\x00', '\x00', '\xfe', '\xff'},
	{'\xff', '\xfe', '\x00', '\x00'},
}

func TestBom(t *testing.T) {
	fmt.Println("\n--------------- bom_test.go \n")
	fmt.Println("TestBom ...")

	// use just UTF-8 BOM ... no alternative CharSetReader
	if _, err := NewMapXml(boms[0]); err != io.EOF {
		t.Fatalf("NewMapXml err; %v\n", err)
	}

	if _, err := NewMapXmlSeq(boms[0]); err != io.EOF {
		t.Fatalf("NewMapXmlSeq err: %v\n", err)
	}
}

var bomdata = append(boms[0], []byte(`<Allitems>
	<Item>
	</Item>
   <Item>
        <link>http://www.something.com</link>
        <description>Some description goes here.</description>
   </Item>
</Allitems>`)...)

func TestBomData(t *testing.T) {
	fmt.Println("TestBomData ...")
	m, err := NewMapXml(bomdata)
	if err != nil {
		t.Fatalf("err: didn't find xml.StartElement")
	}
	fmt.Printf("m: %v\n", m)
	j, _ := m.Xml()
	fmt.Println("m:", string(j))
}

func TestBomDataSeq(t *testing.T) {
	fmt.Println("TestBomDataSeq ...")
	m, err := NewMapXmlSeq(bomdata)
	if err != nil {
		t.Fatalf("err: didn't find xml.StartElement")
	}
	fmt.Printf("m: %v\n", m)
	j, _ := m.XmlSeq()
	fmt.Println("m:", string(j))
}

func TestBomDataReader(t *testing.T) {
	fmt.Println("TestBomDataReader ...")
	r := bytes.NewReader(bomdata)
	m, err := NewMapXmlReader(r)
	if err != nil {
		t.Fatalf("err: didn't find xml.StartElement")
	}
	fmt.Printf("m: %v\n", m)
	j, _ := m.Xml()
	fmt.Println("m:", string(j))
}

func TestBomDataSeqReader(t *testing.T) {
	fmt.Println("TestBomDataSeqReader ...")
	r := bytes.NewReader(bomdata)
	m, err := NewMapXmlSeqReader(r)
	if err != nil {
		t.Fatalf("err: didn't find xml.StartElement")
	}
	fmt.Printf("m: %v\n", m)
	j, _ := m.XmlSeq()
	fmt.Println("m:", string(j))
}
