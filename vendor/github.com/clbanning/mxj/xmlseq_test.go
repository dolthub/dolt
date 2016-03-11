
package mxj

import (
	"fmt"
	"io"
	"testing"
)

func TestXmlSeqHeader(t *testing.T) {
	fmt.Println("\n----------------  xmlseq_test.go ...\n")
}

func TestNewMapXmlSeq(t *testing.T) {
	x := []byte(`<doc> 
   <books>
      <book seq="1">
         <author>William T. Gaddis</author>
         <review>Gaddis is one of the most influential but little know authors in America.</review>
         <title>The Recognitions</title>
         <!-- here's the rest of the review -->
         <review>One of the great seminal American novels of the 20th century.</review>
         <review>Without it Thomas Pynchon probably wouldn't have written Gravity's Rainbow.</review>
      </book>
      <book seq="2">
         <author>Austin Tappan Wright</author>
         <title>Islandia</title>
         <review>An example of earlier 20th century American utopian fiction.</review>
      </book>
      <book>
         <author>John Hawkes</author>
         <title>The Beetle Leg</title>
         <!throw in a directive here>
         <review>A lyrical novel about the construction of Ft. Peck Dam in Montana.</review>
      </book>
      <book> 
         <author>
            <?cat first_name last_name?>
            <first_name>T.E.</first_name>
            <last_name>Porter</last_name>
         </author>
         <title>King's Day</title>
         <review>A magical novella.</review>
      </book>
   </books>
</doc>`)

	m, err := NewMapXmlSeq(x)
	if err != nil && err != io.EOF {
		t.Fatal("err:", err.Error())
	}
	fmt.Println("NewMapXmlSeq, x:\n", string(x))
	// fmt.Println("NewMapXmlSeq, m:\n", m)
	fmt.Println("NewMapXmlSeq, s:\n", m.StringIndent())

	b, err := m.XmlIndent("", "  ")
	if err != nil {
		t.Fatal("err:", err)
	}
	fmt.Println("NewMapXmlSeq, mv.XmlIndent():\n", string(b))

	b, err = m.XmlSeqIndent("", "  ")
	if err != nil {
		t.Fatal("err:", err)
	}
	fmt.Println("NewMapXmlSeq, mv.XmlSeqIndent():\n", string(b))
}
