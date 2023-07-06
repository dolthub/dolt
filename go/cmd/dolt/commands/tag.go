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
	"fmt"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
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
		var verr errhand.VerboseError
		if apr.Contains(cli.DeleteFlag) {
			verr = errhand.BuildDError("must specify a tag name to delete").Build()
		} else if apr.Contains(cli.MessageArg) {
			verr = errhand.BuildDError("must specify a tag name to create").Build()
		} else {
			verr = listTags(queryist, sqlCtx, apr)
		}
		return HandleVErrAndExitCode(verr, usage)
	}

	//// delete tag
	//if apr.Contains(cli.DeleteFlag) {
	//	var verr errhand.VerboseError
	//	if apr.Contains(cli.MessageArg) {
	//		verr = errhand.BuildDError("delete and tag message options are incompatible").Build()
	//	} else if apr.Contains(cli.VerboseFlag) {
	//		verr = errhand.BuildDError("delete and verbose options are incompatible").Build()
	//	} else {
	//		err := actions.DeleteTags(ctx, dEnv, apr.Args...)
	//		if err != nil {
	//			verr = errhand.BuildDError("failed to delete tags").AddCause(err).Build()
	//		}
	//	}
	//	return HandleVErrAndExitCode(verr, usage)
	//}
	//
	//// create tag
	//var verr errhand.VerboseError
	//if apr.Contains(cli.VerboseFlag) {
	//	verr = errhand.BuildDError("verbose flag can only be used with tag listing").Build()
	//} else if len(apr.Args) > 2 {
	//	verr = errhand.BuildDError("create tag takes at most two args").Build()
	//} else {
	//	props, err := getTagProps(dEnv, apr)
	//	if err != nil {
	//		verr = errhand.BuildDError("failed to get tag props").AddCause(err).Build()
	//		return HandleVErrAndExitCode(verr, usage)
	//	}
	//	tagName := apr.Arg(0)
	//	startPoint := "head"
	//	if len(apr.Args) > 1 {
	//		startPoint = apr.Arg(1)
	//	}
	//	err = actions.CreateTag(ctx, dEnv, tagName, startPoint, props)
	//	if err != nil {
	//		verr = errhand.BuildDError("failed to create tag").AddCause(err).Build()
	//	}
	//}

	//return HandleVErrAndExitCode(verr, usage)

	return 0
}

func getTagProps(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (props actions.TagProps, err error) {
	var name, email string
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
	} else {
		name, email, err = env.GetNameAndEmail(dEnv.Config)
	}
	if err != nil {
		return props, err
	}

	msg, _ := apr.GetValue(cli.MessageArg)

	props = actions.TagProps{
		TaggerName:  name,
		TaggerEmail: email,
		Description: msg,
	}

	return props, nil
}

func listTags(queryist cli.Queryist, sqlCtx *sql.Context, apr *argparser.ArgParseResults) errhand.VerboseError {
	tagInfos, err := getTagInfos(queryist, sqlCtx)
	if err != nil {
		return errhand.BuildDError("error: failed to get tags").AddCause(err).Build()
	}

	for _, tag := range tagInfos {
		if apr.Contains(cli.VerboseFlag) {
			verboseTagPrint(tag)
		} else {
			cli.Println(fmt.Sprintf("\t%s", tag.Name))
		}
	}

	if err != nil {
		return errhand.BuildDError("error listing tags").AddCause(err).Build()
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
