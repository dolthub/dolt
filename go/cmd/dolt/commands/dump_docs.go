// Copyright 2020 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
)

const (
	dirParamName = "dir"
)

type DumpDocsCmd struct {
	DoltCommand cli.SubCommandHandler
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd *DumpDocsCmd) Name() string {
	return "dump-docs"
}

// Description returns a description of the command
func (cmd *DumpDocsCmd) Description() string {
	return "dumps all documentation in md format to a directory"
}

// Hidden should return true if this command should be hidden from the help text
func (cmd *DumpDocsCmd) Hidden() bool {
	return true
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd *DumpDocsCmd) RequiresRepo() bool {
	return false
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd *DumpDocsCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	return nil
}

// Exec executes the command
func (cmd *DumpDocsCmd) Exec(_ context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsString(dirParamName, "", "dir", "The directory where the md files should be dumped")
	help, usage := cli.HelpAndUsagePrinters(commandStr, initDocumentation, ap)
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

	indexPath := filepath.Join(dirStr, "command_line_index.md")
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
				filename := strings.ReplaceAll(currCmdStr, " ", "_")
				filename = strings.ReplaceAll(filename, "-", "_")
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


func CreateMarkdown(fs filesys.Filesys, path, commandStr string, cmdDoc cli.CommandDocumentation, parser *argparser.ArgParser) error {
	longDesc, longDescErr := cmdDoc.GetLongDesc(cli.MarkdownFormat)
	if longDescErr != nil {
		return longDescErr
	}
	synopsis, synopsisErr := cmdDoc.GetSynopsis(cli.MarkdownFormat)
	if synopsisErr != nil {
		return synopsisErr
	}

	wr, err := fs.OpenForWrite(path)

	if err != nil {
		return err
	}

	defer wr.Close()

	// Insert the header that is used by Gatsby
	err = iohelp.WriteIfNoErr(wr, []byte(getHeader(commandStr)), nil)

	formattedShortDesc := fmt.Sprintf("`%s` - %s\n\n", commandStr, cmdDoc.GetShortDesc())
	err = iohelp.WriteIfNoErr(wr, titleHelper("Command"), err)
	err = iohelp.WriteIfNoErr(wr, []byte(formattedShortDesc), err)

	if len(synopsis) > 0 {
		err = iohelp.WriteIfNoErr(wr, titleHelper("Synopsis"), err)
		err = iohelp.WriteIfNoErr(wr, []byte(getSynopsis(commandStr, synopsis)), err)
	}

	err = iohelp.WriteIfNoErr(wr, titleHelper("Description"), err)
	err = iohelp.WriteIfNoErr(wr, []byte(fmt.Sprintf("%s\n\n", longDesc)), err)

	if len(parser.Supported) > 0 || len(parser.ArgListHelp) > 0 {
		err = iohelp.WriteIfNoErr(wr, titleHelper("Options"), err)

		// Iterate across arguments and template them
		for _, kvTuple := range parser.ArgListHelp {
			arg, desc := kvTuple[0], kvTuple[1]
			argStruct := Agument{arg, desc}
			outputStr, err := TemplateArgument(argStruct)
			err = iohelp.WriteIfNoErr(wr, []byte(outputStr), err)
		}

		// Iterate accross supported options, templating each one of them
		for _, supOpt := range parser.Supported {
			argStruct := Supported{supOpt.Abbrev, supOpt.Name, supOpt.ValDesc}
			outputStr, err := TemplateSupported(argStruct)
			err = iohelp.WriteIfNoErr(wr, []byte(outputStr), err)
		}
	}

	return err
}

func getHeader(commandStr string) string {
	header := `---
title: %s
---

`
	return fmt.Sprintf(header, commandStr)
}

// Apply appropriate markdown to title and add a few newlines
func titleHelper(title string) []byte {
	return []byte(fmt.Sprintf("## %s\n\n", title))
}

// Create a synopsis properly contained within HTML tags required for markdown generation
// TODO we could probably enhance this using html/template
func getSynopsis(commandStr string, synopsis [] string) string {
	synopsisStr := fmt.Sprintf("%s %s<br />\n", commandStr, synopsis[0])
	if len(synopsis) > 1 {
		temp := make([]string, len(synopsis)-1)
		for i, el := range(synopsis[1:]) {
			temp[i] = fmt.Sprintf("\t\t\t%s %s<br />\n", commandStr, el)
		}
		synopsisStr += strings.Join(temp, "")
	}

	html := `
<div class="gatsby-highlight" data-language="text">
	<pre class="language-text">
		<code class="language-text">
			%s
  		</code>
	</pre>
</div>

`

	return fmt.Sprintf(html, synopsisStr)
}

type Agument struct {
	Name string
	Description string
}


func TemplateArgument(supportedArg Agument) (string, error) {
	var formatString string
	if supportedArg.Description == "" {
		formatString = "`<{{.Name}}>`\n\n"
	} else {
		formatString = "`<{{.Name}}>`:\n\n{{.Description}}\n\n"
	}

	templ, err := template.New("argString").Parse(formatString)
	if err != nil {
		cli.Println(err)
		return "", err
	}
	var templBuffer bytes.Buffer
	if err := templ.Execute(&templBuffer, supportedArg); err != nil {
		cli.Println(err)
		return "", err
	}
	ret := templBuffer.String()
	cli.Printf("%s", ret)
	return ret, nil
}

type Supported struct {
	Abbreviation string
	Name string
	Description string
}

func TemplateSupported(supported Supported) (string, error) {
	var formatString string
	if supported.Abbreviation == "" && supported.Description == "" {
		formatString = "`--{{.Name}}`\n\n"
	} else if supported.Abbreviation == "" && supported.Description != ""  {
		formatString = "`--{{.Name}}`:\n\n\t{{.Description}}\n\n"

	} else if supported.Abbreviation != "" && supported.Description == "" {
		formatString = "`-{{.Abbreviation}}`, `--{{.Name}}`\n\n"
	} else {
		formatString = "`-{{.Abbreviation}}`, `--{{.Name}}`:\n\n\t{{.Description}}\n\n"
	}

	templ, err := template.New("argString").Parse(formatString)
	if err != nil {
		cli.Println(err)
		return "", err
	}
	var templBuffer bytes.Buffer
	if err := templ.Execute(&templBuffer, supported); err != nil {
		cli.Println(err)
		return "", err
	}
	ret := templBuffer.String()
	cli.Printf("%s", ret)
	return ret, nil
}



