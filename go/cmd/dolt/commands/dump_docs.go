package commands

import (
	"context"
	"fmt"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"io"
	"path/filepath"
	"strings"
)

const (
	dirParamName = "dir"
)

type DumpDocsCmd struct {
	DoltCommand cli.SubCommandHandler
}

func (cmd *DumpDocsCmd) Name() string {
	return "dump-docs"
}

func (cmd *DumpDocsCmd) Description() string {
	return "dumps all documentation in md format to a directory"
}

func (cmd *DumpDocsCmd) Hidden() bool {
	return true
}

func (cmd *DumpDocsCmd) RequiresRepo() bool {
	return false
}

func (cmd *DumpDocsCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	return nil
}

func (cmd *DumpDocsCmd) Exec(_ context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsString(dirParamName, "", "dir", "The directory where the md files should be dumped")
	help, usage := cli.HelpAndUsagePrinters(commandStr, initShortDesc, initLongDesc, initSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	dirStr := apr.GetValueOrDefault(dirParamName, ".")

	exists, isDir := dEnv.FS.Exists(dirStr)

	if !exists {
		cli.PrintErrln(dirStr + " does not exist.")
		usage()
		return 1
	} else if !isDir {
		cli.PrintErrln(dirStr + " is a file, not a directory.")
		usage()
		return 1
	}

	indexPath := filepath.Join(dirStr, "command_line.md")
	idxWr, err := dEnv.FS.OpenForWrite(indexPath)

	if err != nil {
		verr := errhand.BuildDError("error writing to command_line.md").AddCause(err).Build()
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	defer idxWr.Close()
	err = iohelp.WriteAll(idxWr, []byte("# Dolt Commands\n"))

	if err != nil {
		verr := errhand.BuildDError("error writing to command_line.md").AddCause(err).Build()
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	err = cmd.dumpDocs(idxWr, dEnv, dirStr, cmd.DoltCommand.Name(), cmd.DoltCommand.Subcommands)

	if err != nil {
		verr := errhand.BuildDError("error: Failed to dump docs.").AddCause(err).Build()
		cli.PrintErrln(verr.Verbose())

		return 1
	}

	return 0
}

func (cmd *DumpDocsCmd) dumpDocs(idxWr io.Writer, dEnv *env.DoltEnv, dirStr, cmdStr string, subCommands []cli.Command) error {
	for _, curr := range subCommands {
		var hidden bool
		if hidCmd, ok := curr.(cli.HiddenCommand); ok {
			hidden = hidCmd.Hidden()
		}

		if !hidden {
			if subCmdHandler, ok := curr.(cli.SubCommandHandler); ok {
				err := cmd.dumpDocs(idxWr, dEnv, dirStr, cmdStr+" "+subCmdHandler.Name(), subCmdHandler.Subcommands)

				if err != nil {
					return err
				}
			} else {
				currCmdStr := cmdStr + " " + curr.Name()
				filename := strings.Replace(currCmdStr, " ", "_", -1)
				filename = strings.Replace(filename, "-", "_", -1)
				absPath := filepath.Join(dirStr, filename+".md")

				indexLine := fmt.Sprintf("* [%s](%s)\n", currCmdStr, filename)
				err := iohelp.WriteAll(idxWr, []byte(indexLine))

				if err != nil {
					return err
				}

				err = curr.CreateMarkdown(dEnv.FS, absPath, currCmdStr)

				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
