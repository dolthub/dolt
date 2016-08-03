package splice

import (
	"io/ioutil"
	"os"
	"testing"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func TestCopyFile(t *testing.T) {
	src, _ := ioutil.TempFile("", "termite")
	err := ioutil.WriteFile(src.Name(), []byte("hello"), 0644)
	if err != nil {
		t.Error(err)
	}
	dst, _ := ioutil.TempFile("", "termite")
	err = CopyFile(dst.Name(), src.Name(), 0755)
	if err != nil {
		t.Error(err)
	}

	c, err := ioutil.ReadFile(dst.Name())
	if err != nil {
		t.Error(err)
	}
	if string(c) != "hello" {
		t.Error("mismatch", string(c))
	}
}

func TestSpliceCopy(t *testing.T) {
	src, err := ioutil.TempFile("", "termite")
	check(err)
	bs := make([]byte, 2*1024*1024)
	for i := range bs {
		bs[i] = byte(i % 256)
	}
	_, err = src.Write(bs)
	check(err)
	err = src.Close()
	check(err)
	src, err = os.Open(src.Name())
	check(err)
	dst, err := ioutil.TempFile("", "termite")
	check(err)

	if maxPipeSize%4096 != 0 || maxPipeSize < 4096 {
		t.Error("pipe size should be page size multiple", maxPipeSize)
	}
	pool := newSplicePairPool()
	p, err := pool.get()
	if p != nil {
		p.MaxGrow()
		t.Logf("Splice size %d", p.size)
		SpliceCopy(dst, src, p)
		dst.Close()
		src.Close()
		p.Close()
	} else {
		t.Error("Could not open splice: ", err)
	}
}
