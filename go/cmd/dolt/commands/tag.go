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
	"io"
	"strings"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

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

const (
	tagMessageArg = "message"
)

type TagCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd TagCmd) Name() string {
	return "tag"
}

// Description returns a description of the command
func (cmd TagCmd) Description() string {
	return "Create, list, delete tags."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd TagCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, tagDocs, ap))
}

func (cmd TagCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	// todo: docs
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"ref", "A commit ref that the tag should point at."})
	ap.SupportsString(tagMessageArg, "m", "msg", "Use the given {{.LessThan}}msg{{.GreaterThan}} as the tag message.")
	ap.SupportsFlag(verboseFlag, "v", "list tags along with their metadata.")
	ap.SupportsFlag(deleteFlag, "d", "Delete a tag.")
	return ap
}

// EventType returns the type of the event to log
func (cmd TagCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TAG
}

// Exec executes the command
func (cmd TagCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, tagDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	// list tags
	if len(apr.Args) == 0 {
		var verr errhand.VerboseError
		if apr.Contains(deleteFlag) {
			verr = errhand.BuildDError("must specify a tag name to delete").Build()
		} else if apr.Contains(messageFlag) {
			verr = errhand.BuildDError("must specify a tag name to create").Build()
		} else {
			verr = listTags(ctx, dEnv, apr)
		}
		return HandleVErrAndExitCode(verr, usage)
	}

	// delete tag
	if apr.Contains(deleteFlag) {
		var verr errhand.VerboseError
		if apr.Contains(messageFlag) {
			verr = errhand.BuildDError("delete and tag message options are incompatible").Build()
		} else if apr.Contains(verboseFlag) {
			verr = errhand.BuildDError("delete and verbose options are incompatible").Build()
		} else {
			err := actions.DeleteTags(ctx, dEnv, apr.Args...)
			if err != nil {
				verr = errhand.BuildDError("failed to delete tags").AddCause(err).Build()
			}
		}
		return HandleVErrAndExitCode(verr, usage)
	}

	// create tag
	var verr errhand.VerboseError
	if apr.Contains(verboseFlag) {
		verr = errhand.BuildDError("verbose flag can only be used with tag listing").Build()
	} else if len(apr.Args) > 2 {
		verr = errhand.BuildDError("create tag takes at most two args").Build()
	} else {
		props, err := getTagProps(dEnv, apr)
		if err != nil {
			verr = errhand.BuildDError("failed to get tag props").AddCause(err).Build()
			return HandleVErrAndExitCode(verr, usage)
		}
		tagName := apr.Arg(0)
		startPoint := "head"
		if len(apr.Args) > 1 {
			startPoint = apr.Arg(1)
		}
		err = actions.CreateTag(ctx, dEnv, tagName, startPoint, props)
		if err != nil {
			verr = errhand.BuildDError("failed to create tag").AddCause(err).Build()
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func getTagProps(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (props actions.TagProps, err error) {
	name, email, err := env.GetNameAndEmail(dEnv.Config)
	if err != nil {
		return props, err
	}

	msg, _ := apr.GetValue(tagMessageArg)

	props = actions.TagProps{
		TaggerName:  name,
		TaggerEmail: email,
		Description: msg,
	}

	return props, nil
}

func listTags(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	var err error
	if apr.Contains(verboseFlag) {
		err = actions.IterResolvedTags(ctx, dEnv.DoltDB, func(tag *doltdb.Tag) (bool, error) {
			verboseTagPrint(tag)
			return false, nil
		})
	} else {
		err = actions.IterResolvedTags(ctx, dEnv.DoltDB, func(tag *doltdb.Tag) (bool, error) {
			cli.Println(fmt.Sprintf("\t%s", tag.Name))
			return false, nil
		})
	}

	if err != nil {
		return errhand.BuildDError("error listing tags").AddCause(err).Build()
	}

	return nil
}

func verboseTagPrint(tag *doltdb.Tag) {
	h, _ := tag.Commit.HashOf()

	cli.Println(color.YellowString("%s\t%s", tag.Name, h.String()))

	cli.Printf("Tagger: %s <%s>\n", tag.Meta.Name, tag.Meta.Email)

	timeStr := tag.Meta.FormatTS()
	cli.Println("Date:  ", timeStr)

	if tag.Meta.Description != "" {
		formattedDesc := "\n\t" + strings.Replace(tag.Meta.Description, "\n", "\n\t", -1)
		cli.Println(formattedDesc)
	}
	cli.Println("")
}
