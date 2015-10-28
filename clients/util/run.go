package util

import (
	"io/ioutil"
	"os"

	"github.com/attic-labs/noms/d"
)

func Run(f func(), args []string) string {
	origArgs := os.Args
	origOut := os.Stdout
	out, err := ioutil.TempFile(os.TempDir(), "")

	defer func() {
		os.Args = origArgs
		os.Stdout = origOut
		out.Close()
		d.Chk.NoError(os.Remove(out.Name()))
	}()

	d.Chk.NoError(err)

	os.Args = args
	os.Stdout = out
	f()

	_, err = os.Stdout.Seek(0, 0)
	d.Chk.NoError(err)
	b, err := ioutil.ReadAll(os.Stdout)
	d.Chk.NoError(err)
	return string(b)
}
