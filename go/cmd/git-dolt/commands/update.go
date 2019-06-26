package commands

import (
	"fmt"

	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/config"
	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/utils"
)

// Update updates the git-dolt pointer file at the given filename
// to point to the specified revision.
func Update(ptrFname string, revision string) error {
	ptrFname = utils.EnsureSuffix(ptrFname, ".git-dolt")
	c, err := config.Load(ptrFname)
	if err != nil {
		return err
	}

	c.Revision = revision

	if err := config.Write(ptrFname, c.String()); err != nil {
		return err
	}
	fmt.Printf("Updated pointer file %s to revision %s\n", ptrFname, revision)
	return nil
}
