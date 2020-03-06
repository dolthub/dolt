package cli

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

type CmdMdDoc struct {
	Command             string
	CommandAndShortDesc string
	Synopsis            string
	Description         string
	Options             string
}

var cmdMdDocTempl = `---
title: {{.Command}}
---

## Command
{{.CommandAndShortDesc}}

## Synopsis
{{.Synopsis}}

## Description
{{.Description}}

## Options
{{.Options}}

`

func (cmdDoc CommandDocumentation) CmdDocToMd() (string, error) {

	// Accumulate the options and args in a string
	options := ""
	if len(cmdDoc.ArgParser.Supported) > 0 || len(cmdDoc.ArgParser.ArgListHelp) > 0 {
		// no need to write
		//err = iohelp.WriteIfNoErr(wr, titleHelper("Options"), err)

		// Iterate across arguments and template them
		for _, kvTuple := range cmdDoc.ArgParser.ArgListHelp {
			arg, desc := kvTuple[0], kvTuple[1]
			templatedDesc, err := templateDocStringHelper(desc, MarkdownFormat)
			if err != nil {
				return "", err
			}
			argStruct := Agument{arg, templatedDesc}
			outputStr, err := templateArgument(argStruct)
			if err != nil {
				return "", err
			}
			options += outputStr
		}

		// Iterate accross supported options, templating each one of them
		for _, supOpt := range cmdDoc.ArgParser.Supported {
			templatedDesc, err := templateDocStringHelper(supOpt.Desc, MarkdownFormat)
			if err != nil {
				return "", err
			}
			argStruct := Supported{supOpt.Abbrev, supOpt.Name, templatedDesc}
			outputStr, err := templateSupported(argStruct)
			if err != nil {
				return "", err
			}
			options += outputStr
		}
	}

	longDesc, longDescErr := cmdDoc.GetLongDesc(MarkdownFormat)
	if longDescErr != nil {
		return "", longDescErr
	}
	synopsis, synopsisErr := cmdDoc.GetSynopsis(SynopsisMarkdownFormat)
	if synopsisErr != nil {
		return "", synopsisErr
	}

	cmdMdDoc := CmdMdDoc{
		Command:             cmdDoc.CommandStr,
		CommandAndShortDesc: fmt.Sprintf("`%s` - %s\n\n", cmdDoc.CommandStr, cmdDoc.GetShortDesc()),
		Synopsis:            transformSynopsisToHtml(cmdDoc.CommandStr, synopsis),
		Description:         longDesc,
		Options:             options,
	}

	templ, err := template.New("shortDesc").Parse(cmdMdDocTempl)
	if err != nil {
		return "", err
	}
	var templBuffer bytes.Buffer
	if err := templ.Execute(&templBuffer, cmdMdDoc); err != nil {
		return "", err
	}
	return templBuffer.String(), nil
}

// This handles creating
type CommandDocumentation struct {
	CommandStr string
	ShortDesc  string
	LongDesc   string
	Synopsis   []string
	ArgParser  *argparser.ArgParser
}

func GetCommandDocumentation(commandStr string, cmdDoc CommandDocumentationContent, argParser *argparser.ArgParser) CommandDocumentation {
	return  CommandDocumentation{
		CommandStr: commandStr,
		ShortDesc:  cmdDoc.ShortDesc,
		LongDesc:   cmdDoc.LongDesc,
		Synopsis:   cmdDoc.Synopsis,
		ArgParser:  argParser,
	}
}

func (cmdDoc CommandDocumentation) GetShortDesc() string {
	return cmdDoc.ShortDesc
}

func (cmdDoc CommandDocumentation) GetLongDesc(format DocFormat) (string, error) {
	return templateDocStringHelper(cmdDoc.LongDesc, format)
}

func templateDocStringHelper(docString string, docFormat DocFormat) (string, error) {
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

func (cmdDoc CommandDocumentation) GetSynopsis(format DocFormat) ([]string, error) {
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

type DocFormat struct {
	LessThan      string
	GreaterThan   string
	EmphasisLeft  string
	EmphasisRight string
}

var MarkdownFormat = DocFormat{"`<", ">`", "`", "`"}
var CliFormat = DocFormat{"<", ">", "<b>", "</b>"}
var SynopsisMarkdownFormat = DocFormat{"&lt;", "&gt;", "`", "`"}

func transformSynopsisToHtml(commandStr string, synopsis []string) string {
	if len(synopsis) == 0 {
		return ""
	}
	synopsisStr := fmt.Sprintf("%s %s<br />\n", commandStr, synopsis[0])
	if len(synopsis) > 1 {
		temp := make([]string, len(synopsis)-1)
		for i, el := range synopsis[1:] {
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
	Name        string
	Description string
}

func templateArgument(supportedArg Agument) (string, error) {
	var formatString string
	if supportedArg.Description == "" {
		formatString = "`<{{.Name}}>`\n\n"
	} else {
		formatString = "`<{{.Name}}>`:\n\n{{.Description}}\n\n"
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

type Supported struct {
	Abbreviation string
	Name         string
	Description  string
}

func templateSupported(supported Supported) (string, error) {
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
