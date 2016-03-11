// https://groups.google.com/forum/?fromgroups#!topic/golang-nuts/2A6_YRYXCjA

package main

import (
	"fmt"
	"github.com/clbanning/mxj"
)

var data = []byte(`<root>
    <!-- first child1 element -->
    <child1>
        <option1 />
    </child1>
    <!-- follows first child2 element -->
    <child2>
         <option2 />
    </child2>
    <!-- there can be multiple child1 elements -->
    <child1>
          <option1 />
    </child1>
    <child1>
          <option1 />
    </child1>
    <!-- followed by child2 element -->
    <child2>
          <option2 />
    </child2>
    <!-- followed by child1 element -->
    <child1>
          <option1 />
    </child1>
    <!-- there can be multiple child2 elements -->
    <child2>
          <option2 />
    </child2>
    <child2>
          <option2 />
    </child2>
 </root>`)

func main() {
	m, err := mxj.NewMapXml(data)
	if err != nil {
		fmt.Println("err:", err)
	}
	fmt.Println(m.StringIndentNoTypeInfo())

	doc, err := m.XmlIndent("", "  ")
	if err != nil {
		fmt.Println("err:", err)
	}
	fmt.Println(string(doc))

	val, err := m.ValuesForKey("child1")
	if err != nil {
		fmt.Println("err:", err)
	}
	fmt.Println("val:", val)

	mxj.XmlGoEmptyElemSyntax()
	doc, err = mxj.AnyXmlIndent(val, "", "  ", "child1")
	if err != nil {
		fmt.Println("err:", err)
	}
	fmt.Println(string(doc))
}
