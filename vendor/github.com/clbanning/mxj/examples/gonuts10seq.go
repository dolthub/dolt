/* gonuts10.go - https://groups.google.com/forum/?fromgroups#!topic/golang-nuts/tf4aDQ1Hn_c
change:
<author>
    <first-name effect_range="1999-2011">Sam</first-name>
    <first-name effect_range="2012-">Kevin</first-name>
    <last-name>Smith</last-name>
   <full-name></full-name>
</author>

to:
<author>
     <first-name effect_range="1999-2011">Sam</first-name>
    <first-name effect_range="2012-">Kevin</first-name>
    <last-name>Smith</last-name>
   <full-name>Kevin Smith</full-name>
</author>

NOTE: use NewMapXmlSeq() and mv.XmlSeqIndent() to preserve structure.

Here we build the "full-name" element value from other values in the doc by selecting the
"first-name" value with the latest dates.
*/

package main

import (
	"fmt"
	"github.com/clbanning/mxj"
	"strings"
)

var data = []byte(`
<author>
    <first-name effect_range="1999-2011">Sam</first-name>
    <first-name effect_range="2012-">Kevin</first-name>
    <last-name>Smith</last-name>
   <full-name></full-name>
</author>
`)

func main() {
	fmt.Println(string(data))
	m, err := mxj.NewMapXmlSeq(data)
	if err != nil {
		fmt.Println("NewMapXml err:", err)
		return
	}
	vals, err := m.ValuesForPath("author.first-name") // full-path option
	if err != nil {
		fmt.Println("ValuesForPath err:", err)
		return
	} else if len(vals) == 0 {
		fmt.Println("no first-name vals")
		return
	}
	var fname, date string
	var index int
	for _, v := range vals {
		vm, ok := v.(map[string]interface{})
		if !ok {
			fmt.Println("assertion failed")
			return
		}
		fn, ok := vm["#text"].(string)
		if !ok {
			fmt.Println("no #text tag")
			return
		}
		// extract the associated date
		dt, _ := mxj.Map(vm).ValueForPathString("#attr.effect_range.#text")
		if dt == "" {
			fmt.Println("no effect_range attr")
			return
		}
		dts := strings.Split(dt, "-")
		if len(dts) > 1 && dts[len(dts)-1] == "" {
			index = len(dts) - 2
		} else if len(dts) > 0 {
			index = len(dts) - 1
		}
		if len(dts) > 0 && dts[index] > date {
			date = dts[index]
			fname = fn
		}
	}

	vals, err = m.ValuesForPath("author.last-name.#text") // full-path option
	if err != nil {
		fmt.Println("ValuesForPath err:", err)
		return
	} else if len(vals) == 0 {
		fmt.Println("no last-name vals")
		return
	}
	lname := vals[0].(string)
	if err = m.SetValueForPath(fname+" "+lname, "author.full-name.#text"); err != nil {
		fmt.Println("SetValueForPath err:", err)
		return
	}
	b, err := m.XmlSeqIndent("", "  ")
	if err != nil {
		fmt.Println("XmlIndent err:", err)
		return
	}
	fmt.Println(string(b))
}
