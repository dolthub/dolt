// nan_test.go

package mxj

import (
	"fmt"
	"testing"
)

func TestNan(t *testing.T) {
	fmt.Println("\n------------ TestNan\n")
	data := []byte("<foo><bar>NAN</bar></foo>")

	m, err := NewMapXml(data, true)
	if err != nil {
		t.Fatal("err:", err)
	}
	v, err := m.ValueForPath("foo.bar")
	if err != nil {
		t.Fatal("err:", err)
	}
	if _, ok := v.(string); !ok {
		t.Fatal("v not string")
	}
	fmt.Println("foo.bar:", v)
}

func TestInf(t *testing.T) {
	data := []byte("<foo><bar>INF</bar></foo>")

	m, err := NewMapXml(data, true)
	if err != nil {
		t.Fatal("err:", err)
	}
	v, err := m.ValueForPath("foo.bar")
	if err != nil {
		t.Fatal("err:", err)
	}
	if _, ok := v.(string); !ok {
		t.Fatal("v not string")
	}
	fmt.Println("foo.bar:", v)
}

func TestMinusInf(t *testing.T) {
	data := []byte("<foo><bar>-INF</bar></foo>")

	m, err := NewMapXml(data, true)
	if err != nil {
		t.Fatal("err:", err)
	}
	v, err := m.ValueForPath("foo.bar")
	if err != nil {
		t.Fatal("err:", err)
	}
	if _, ok := v.(string); !ok {
		t.Fatal("v not string")
	}
	fmt.Println("foo.bar:", v)
}

func TestCastNanInf(t *testing.T) {
	data := []byte("<foo><bar>NAN</bar></foo>")

	CastNanInf(true)

	m, err := NewMapXml(data, true)
	if err != nil {
		t.Fatal("err:", err)
	}
	v, err := m.ValueForPath("foo.bar")
	if err != nil {
		t.Fatal("err:", err)
	}
	if _, ok := v.(float64); !ok {
		fmt.Printf("%#v\n", v)
		t.Fatal("v not float64")
	}
	fmt.Println("foo.bar:", v)
}

