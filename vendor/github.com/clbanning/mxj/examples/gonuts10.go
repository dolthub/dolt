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

NOTE: sequence of elements NOT guaranteed due to use of map[string]interface{}.

Here we build the "full-name" element value from other values in the doc by selecting the
"first-name" value with the latest dates.
*/

package main

import (
	"fmt"
	"github.com/clbanning/mxj"
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
	m, err := mxj.NewMapXml(data)
	if err != nil {
		fmt.Println("NewMapXml err:", err)
		return
	}
	// vals, err := m.ValuesForPath("author.first-name") // full-path option
	vals, err := m.ValuesForKey("first-name") // key-only alternatively
	if err != nil {
		fmt.Println("ValuesForPath err:", err)
		return
	} else if len(vals) == 0 {
		fmt.Println("no first-name vals")
		return
	}
	var fname, date string
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
		dt, ok := vm["-effect_range"].(string)
		if !ok {
			fmt.Println("no -effect_range attr")
			return
		}
		if dt > date {
			date = dt
			fname = fn
		}
	}
	/*
		// alternatively:
		//(however, this requires knowing what latest "effect_range" attribute value is)
		vals, err := m.ValuesForKey("first-name", "-effect_range:2012-")
		if len(vals) == 0 {
			fmt.Println("no #text vals")
			return
		}
		fname := vals[0].(map[string]interface{})["#text"].(string)
	*/

	// vals, err = m.ValuesForKey("last-name") // key-only option
	vals, err = m.ValuesForPath("author.last-name") // full-path option
	if err != nil {
		fmt.Println("ValuesForPath err:", err)
		return
	} else if len(vals) == 0 {
		fmt.Println("no last-name vals")
		return
	}
	lname := vals[0].(string)
	if err = m.SetValueForPath(fname+" "+lname, "author.full-name"); err != nil {
		fmt.Println("SetValueForPath err:", err)
		return
	}
	b, err := m.XmlIndent("", "  ")
	if err != nil {
		fmt.Println("XmlIndent err:", err)
		return
	}
	fmt.Println(string(b))
}
