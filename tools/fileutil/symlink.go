package fileutil

import (
	"os"

	"github.com/attic-labs/noms/d"
)

func ForceSymlink(oldname, newname string) {
	info, err := os.Lstat(newname)
	if os.IsNotExist(err) {
		d.Exp.NoError(os.Symlink(oldname, newname))
		return
	}
	d.Exp.NoError(err)
	d.Exp.False(info.IsDir())
	if info.Mode()&os.ModeSymlink != os.ModeSymlink {
		if err := os.Remove(newname); !os.IsNotExist(err) {
			d.Exp.NoError(err)
		}
		d.Exp.NoError(os.Symlink(oldname, newname))
	}
}
