package commands

import (
	"fmt"

	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/config"
	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/doltops"
	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/utils"
)

// Fetch takes the filename of a git-dolt pointer file and clones
// the specified dolt repository to the specified revision.
func Fetch(ptrFname string) error {
	config, err := config.Load(ptrFname)
	if err != nil {
		return err
	}

	if err := doltops.CloneToRevision(config.Remote, config.Revision); err != nil {
		return err
	}

	fmt.Printf("Dolt repository cloned from remote %s to directory %s at revision %s\n", config.Remote, utils.LastSegment(config.Remote), config.Revision)
	return nil
}
