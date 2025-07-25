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

package commands

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
)

type tagInfo struct {
	Name      string
	Hash      string
	Tagger    string
	Email     string
	Timestamp uint64
	Message   string
}

var tagDocs = cli.CommandDocumentationContent{
	ShortDesc: `Create, list, delete tags.`,
	LongDesc: `If there are no non-option arguments, existing tags are listed.

The command's second form creates a new tag named {{.LessThan}}tagname{{.GreaterThan}} which points to the current {{.EmphasisLeft}}HEAD{{.EmphasisRight}}, or {{.LessThan}}ref{{.GreaterThan}} if given. Optionally, a tag message can be passed using the {{.EmphasisLeft}}-m{{.EmphasisRight}} option. 

With a {{.EmphasisLeft}}-d{{.EmphasisRight}}, {{.LessThan}}tagname{{.GreaterThan}} will be deleted.`,
	Synopsis: []string{
		`[-v]`,
		`[-m {{.LessThan}}message{{.GreaterThan}}] {{.LessThan}}tagname{{.GreaterThan}} [{{.LessThan}}ref{{.GreaterThan}}]`,
		`-d {{.LessThan}}tagname{{.GreaterThan}}`,
	},
}

type TagCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd TagCmd) Name() string {
	return "tag"
}

// Description returns a description of the command
func (cmd TagCmd) Description() string {
	return "Create, list, delete tags."
}

func (cmd TagCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateTagArgParser()
	return cli.NewCommandDocumentation(tagDocs, ap)
}

func (cmd TagCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateTagArgParser()
}

// EventType returns the type of the event to log
func (cmd TagCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TAG
}

// Exec executes the command
func (cmd TagCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cli.CreateTagArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, tagDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return handleStatusVErr(err)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	// list tags
	if len(apr.Args) == 0 {
		err = listTags(queryist, sqlCtx, apr)
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	// delete tag
	if apr.Contains(cli.DeleteFlag) {
		err = deleteTags(queryist, sqlCtx, apr)
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	// create tag
	err = createTag(queryist, sqlCtx, apr)
	return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
}

func createTag(queryist cli.Queryist, sqlCtx *sql.Context, apr *argparser.ArgParseResults) error {
	if apr.Contains(cli.VerboseFlag) {
		return errors.New("verbose flag can only be used with tag listing")
	} else if len(apr.Args) > 2 {
		return errors.New("create tag takes at most two args")
	}

	tagName := apr.Arg(0)
	startPoint := "head"
	if len(apr.Args) > 1 {
		startPoint = apr.Arg(1)
	}
	message, _ := apr.GetValue(cli.MessageArg)
	author, _ := apr.GetValue(cli.AuthorParam)

	var query string
	var params []interface{}
	if len(message) == 0 {
		if len(author) == 0 {
			query = "call dolt_tag(?, ?)"
			params = []interface{}{tagName, startPoint}
		} else {
			query = "call dolt_tag(?, ?, '--author', ?)"
			params = []interface{}{tagName, startPoint, author}
		}
	} else {
		if len(author) == 0 {
			query = "call dolt_tag('-m', ?, ?, ?)"
			params = []interface{}{message, tagName, startPoint}
		} else {
			query = "call dolt_tag('-m', ?, ?, ?, '--author', ?)"
			params = []interface{}{message, tagName, startPoint, author}
		}
	}

	_, err := InterpolateAndRunQuery(queryist, sqlCtx, query, params...)
	if err != nil {
		return fmt.Errorf("error: failed to create tag %s: %w", tagName, err)
	}
	return nil
}

func deleteTags(queryist cli.Queryist, sqlCtx *sql.Context, apr *argparser.ArgParseResults) error {
	if apr.Contains(cli.MessageArg) {
		return errors.New("delete and tag message options are incompatible")
	} else if apr.Contains(cli.VerboseFlag) {
		return errors.New("delete and verbose options are incompatible")
	} else {
		for _, tagName := range apr.Args {
			_, err := InterpolateAndRunQuery(queryist, sqlCtx, "call dolt_tag('-d', ?)", tagName)
			if err != nil {
				return fmt.Errorf("error: failed to delete tag %s: %w", tagName, err)
			}
		}
	}
	return nil
}

func listTags(queryist cli.Queryist, sqlCtx *sql.Context, apr *argparser.ArgParseResults) error {
	if apr.Contains(cli.DeleteFlag) {
		return errors.New("must specify a tag name to delete")
	} else if apr.Contains(cli.MessageArg) {
		return errors.New("must specify a tag name to create")
	}

	tagInfos, err := getTagInfos(queryist, sqlCtx)
	if err != nil {
		return fmt.Errorf("error: failed to list tags: %w", err)
	}

	for _, tag := range tagInfos {
		if apr.Contains(cli.VerboseFlag) {
			verboseTagPrint(tag)
		} else {
			cli.Println(fmt.Sprintf("\t%s", tag.Name))
		}
	}

	return nil
}

func verboseTagPrint(tag tagInfo) {
	cli.Println(color.YellowString("%s\t%s", tag.Name, tag.Hash))

	cli.Printf("Tagger: %s <%s>\n", tag.Tagger, tag.Email)

	timeStr := time.UnixMilli(int64(tag.Timestamp)).In(datas.CommitLoc).Format(time.RubyDate)
	cli.Println("Date:  ", timeStr)

	if tag.Message != "" {
		formattedDesc := "\n\t" + strings.Replace(tag.Message, "\n", "\n\t", -1)
		cli.Println(formattedDesc)
	}
	cli.Println("")
}

func getTagInfos(queryist cli.Queryist, sqlCtx *sql.Context) ([]tagInfo, error) {
	rows, err := GetRowsForSql(queryist, sqlCtx, "SELECT * FROM dolt_tags")
	if err != nil {
		return nil, err
	}

	tags := []tagInfo{}
	for _, row := range rows {
		timestamp, err := getTimestampColAsUint64(row[4])
		if err != nil {
			return nil, fmt.Errorf("failed to parse tag timestamp: %w", err)
		}
		tag := tagInfo{
			Name:      row[0].(string),
			Hash:      row[1].(string),
			Tagger:    row[2].(string),
			Email:     row[3].(string),
			Timestamp: timestamp,
			Message:   row[5].(string),
		}
		tags = append(tags, tag)
	}

	return tags, nil
}
