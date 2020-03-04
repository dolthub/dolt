package cli

import (
	"bytes"
	"text/template"
	//"context"
	//"fmt"
	//"io"
	//"path/filepath"
	//"strings"
	//"text/template"
	//
	//"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	//"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	//"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	//"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	//"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	//"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
)

type CommandDocumentation struct {
	ShortDesc string
	LongDesc string
	Synopsis []string
}

func (cmdDoc CommandDocumentation) GetShortDesc() string {
	return cmdDoc.ShortDesc
}

func (cmdDoc CommandDocumentation) GetLongDesc(format DocFormat) (string, error) {
	return templateDocString(cmdDoc.LongDesc, format)
}

func (cmdDoc CommandDocumentation) GetSynopsis(format DocFormat) ([]string, error) {
	return templateSynopsis(cmdDoc.Synopsis, format)
}

type DocFormat struct {
	LessThan string
	GreaterThan string
	EmphasisLeft string
	EmphasisRight string
}

var MarkdownFormat = DocFormat{"`<", ">`", "`", "`"}
var CliFormat = DocFormat{"<", ">", "<b>", "</b>"}
var SynopsisMarkdownFormat = DocFormat{"&lt;", "&gt;", "`", "`"}

func templateDocString(docString string, docFormat DocFormat) (string, error) {
	templ, err := template.New("shortDesc").Parse(docString)
	if err != nil {
		return "", err
	}
	var templBuffer bytes.Buffer
	if err := templ.Execute(&templBuffer, docFormat); err != nil {
		return "", err
	}
	return templBuffer.String(), nil
}

func templateSynopsis(lines []string, docFormat DocFormat) ([]string, error) {
	for i, line := range lines {
		formatted, err := templateDocString(line, docFormat)
		if err != nil {
			return []string{}, err
		}
		lines[i] = formatted
	}

	return lines, nil
}


