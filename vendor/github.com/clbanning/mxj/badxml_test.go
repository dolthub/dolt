// trying to recreate a panic

package mxj

import (
	"bytes"
	"fmt"
	"testing"
)

var baddata = []byte(`
	something strange
<Allitems>
	<Item>
	</Item>
   <Item>
        <link>http://www.something.com</link>
        <description>Some description goes here.</description>
   </Item>
</Allitems>
`)

func TestBadXml(t *testing.T) {
	fmt.Println("\n---------------- badxml_test.go\n")
	fmt.Println("TestBadXml ...")
	m, err := NewMapXml(baddata)
	if err != nil {
		t.Fatalf("err: didn't find xml.StartElement")
	}
	fmt.Printf("m: %v\n", m)
	j, _ := m.Xml()
	fmt.Println("m:", string(j))
}

func TestBadXmlSeq(t *testing.T) {
	fmt.Println("TestBadXmlSeq ...")
	m, err := NewMapXmlSeq(baddata)
	if err != nil {
		t.Fatalf("err: didn't find xmlStartElement")
	}
	fmt.Printf("m: %v\n", m)
	j, _ := m.XmlSeq()
	fmt.Println("m:", string(j))
}

func TestBadXmlReader(t *testing.T) {
	fmt.Println("TestBadXmlReader ...")
	r := bytes.NewReader(baddata)
	m, err := NewMapXmlReader(r)
	if err != nil {
		t.Fatalf("err: didn't find xml.StartElement")
	}
	fmt.Printf("m: %v\n", m)
	j, _ := m.Xml()
	fmt.Println("m:", string(j))
}

func TestBadXmlSeqReader(t *testing.T) {
	fmt.Println("TestBadXmlSeqReader ...")
	r := bytes.NewReader(baddata)
	m, err := NewMapXmlSeqReader(r)
	if err != nil {
		t.Fatalf("err: didn't find xmlStartElement")
	}
	fmt.Printf("m: %v\n", m)
	j, _ := m.XmlSeq()
	fmt.Println("m:", string(j))
}
