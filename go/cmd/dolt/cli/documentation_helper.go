// Copyright 2020 Dolthub, Inc.
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

package cli

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

type commandDocumentForMarkdown struct {
	Command     string
	ShortDesc   string
	Synopsis    string
	Description string
	Options     string
}

var cmdMdDocTempl = "## `{{.Command}}`\n\n" +
	"{{.ShortDesc}}\n\n" +
	"### Synopsis\n\n" +
	"{{.Synopsis}}\n\n" +
	"### Description\n\n" +
	"{{.Description}}\n\n" +
	"### Arguments and options\n\n" +
	"{{.Options}}\n\n"

func (cmdDoc CommandDocumentation) CmdDocToMd() (string, error) {
	// Accumulate the options and args in a string
	options := ""
	if len(cmdDoc.ArgParser.Supported) > 0 || len(cmdDoc.ArgParser.ArgListHelp) > 0 {
		// Iterate across arguments and template them
		for _, kvTuple := range cmdDoc.ArgParser.ArgListHelp {
			arg, desc := kvTuple[0], kvTuple[1]
			templatedDesc, err := templateDocStringHelper(desc, MarkdownFormat)
			if err != nil {
				return "", err
			}
			argStruct := argument{arg, templatedDesc}
			outputStr, err := templateArgument(argStruct)
			if err != nil {
				return "", err
			}
			options += outputStr
		}

		// Iterate across supported options, templating each one of them
		for _, supOpt := range cmdDoc.ArgParser.Supported {
			templatedDesc, err := templateDocStringHelper(supOpt.Desc, MarkdownFormat)
			if err != nil {
				return "", err
			}
			argStruct := supported{supOpt.Abbrev, supOpt.Name, templatedDesc}
			outputStr, err := templateSupported(argStruct)
			if err != nil {
				return "", err
			}
			options += outputStr
		}
	} else {
		options = `No options for this command.`
	}

	cmdMdDoc, cmdMdDocErr := cmdDoc.cmdDocToCmdDocMd(options)
	if cmdMdDocErr != nil {
		return "", cmdMdDocErr
	}
	templ, templErr := template.New("shortDesc").Parse(cmdMdDocTempl)
	if templErr != nil {

		return "", templErr
	}
	var templBuffer bytes.Buffer
	if err := templ.Execute(&templBuffer, cmdMdDoc); err != nil {

		return "", err
	}
	ret := strings.Replace(templBuffer.String(), "HEAD~", "HEAD\\~", -1)
	return ret, nil
}

// A struct that represents all the data structures required to create the documentation for a command.
type CommandDocumentation struct {
	// The command/sub-command string passed to a command by the caller
	CommandStr string
	// The short description of the command
	ShortDesc string
	// The long description of the command
	LongDesc string
	// The synopsis, an array of strings showing how to use the command
	Synopsis []string
	// A structure that
	ArgParser *argparser.ArgParser
}

func (cmdDoc CommandDocumentation) cmdDocToCmdDocMd(options string) (commandDocumentForMarkdown, error) {
	longDesc, longDescErr := cmdDoc.GetLongDesc(MarkdownFormat)
	if longDescErr != nil {
		return commandDocumentForMarkdown{}, longDescErr
	}
	synopsis, synopsisErr := cmdDoc.GetSynopsis(SynopsisMarkdownFormat)
	if synopsisErr != nil {
		return commandDocumentForMarkdown{}, synopsisErr
	}

	return commandDocumentForMarkdown{
		Command:     cmdDoc.CommandStr,
		ShortDesc:   cmdDoc.GetShortDesc(),
		Synopsis:    transformSynopsisToMarkdown(cmdDoc.CommandStr, synopsis),
		Description: longDesc,
		Options:     options,
	}, nil
}

// Creates a CommandDocumentation given command string, arg parser, and a CommandDocumentationContent
func GetCommandDocumentation(commandStr string, cmdDoc CommandDocumentationContent, argParser *argparser.ArgParser) CommandDocumentation {
	return CommandDocumentation{
		CommandStr: commandStr,
		ShortDesc:  cmdDoc.ShortDesc,
		LongDesc:   cmdDoc.LongDesc,
		Synopsis:   cmdDoc.Synopsis,
		ArgParser:  argParser,
	}
}

// Returns the ShortDesc field of the receiver CommandDocumentation with the passed DocFormat injected into the template
func (cmdDoc CommandDocumentation) GetShortDesc() string {
	return cmdDoc.ShortDesc
}

// Returns the LongDesc field of the receiver CommandDocumentation with the passed DocFormat injected into the template
func (cmdDoc CommandDocumentation) GetLongDesc(format docFormat) (string, error) {
	return templateDocStringHelper(cmdDoc.LongDesc, format)
}

func templateDocStringHelper(docString string, docFormat docFormat) (string, error) {
	templ, err := template.New("description").Parse(docString)
	if err != nil {
		return "", err
	}
	var templBuffer bytes.Buffer
	if err := templ.Execute(&templBuffer, docFormat); err != nil {
		return "", err
	}
	return templBuffer.String(), nil
}

// Returns the synopsis iterating over each element and injecting the supplied DocFormat
func (cmdDoc CommandDocumentation) GetSynopsis(format docFormat) ([]string, error) {
	lines := cmdDoc.Synopsis
	for i, line := range lines {
		formatted, err := templateDocStringHelper(line, format)
		if err != nil {
			return []string{}, err
		}
		lines[i] = formatted
	}

	return lines, nil
}

type docFormat struct {
	LessThan      string
	GreaterThan   string
	EmphasisLeft  string
	EmphasisRight string
}

// mdx format
var MarkdownFormat = docFormat{"`<", ">`", "`", "`"}

// Shell help output format
var CliFormat = docFormat{"<", ">", "<b>", "</b>"}

// Synopsis is an mdx format, but already inside a code block
var SynopsisMarkdownFormat = docFormat{"<", ">", "`", "`"}

func transformSynopsisToMarkdown(commandStr string, synopsis []string) string {
	if len(synopsis) == 0 {
		return ""
	}
	synopsisStr := fmt.Sprintf("%s %s\n", commandStr, synopsis[0])
	if len(synopsis) > 1 {
		temp := make([]string, len(synopsis)-1)
		for i, el := range synopsis[1:] {
			temp[i] = fmt.Sprintf("%s %s\n", commandStr, el)
		}
		synopsisStr += strings.Join(temp, "")
	}

	markdown := "```bash\n%s```"
	return fmt.Sprintf(markdown, synopsisStr)
}

type argument struct {
	Name        string
	Description string
}

func templateArgument(supportedArg argument) (string, error) {
	var formatString string
	if supportedArg.Description == "" {
		formatString = "`<{{.Name}}>`\n\n"
	} else {
		formatString = "`<{{.Name}}>`: {{.Description}}\n\n"
	}

	templ, err := template.New("argString").Parse(formatString)
	if err != nil {
		return "", err
	}
	var templBuffer bytes.Buffer
	if err := templ.Execute(&templBuffer, supportedArg); err != nil {
		return "", err
	}
	ret := templBuffer.String()
	return ret, nil
}

type supported struct {
	Abbreviation string
	Name         string
	Description  string
}

func templateSupported(supported supported) (string, error) {
	var formatString string
	if supported.Abbreviation == "" && supported.Description == "" {
		formatString = "`--{{.Name}}`\n\n"
	} else if supported.Abbreviation == "" && supported.Description != "" {
		formatString = "`--{{.Name}}`:\n{{.Description}}\n\n"
	} else if supported.Abbreviation != "" && supported.Description == "" {
		formatString = "`-{{.Abbreviation}}`, `--{{.Name}}`\n\n"
	} else {
		formatString = "`-{{.Abbreviation}}`, `--{{.Name}}`:\n{{.Description}}\n\n"
	}

	templ, err := template.New("argString").Parse(formatString)
	if err != nil {
		return "", err
	}
	var templBuffer bytes.Buffer
	if err := templ.Execute(&templBuffer, supported); err != nil {
		return "", err
	}
	ret := templBuffer.String()
	return ret, nil
}
