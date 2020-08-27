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
	"context"
	"fmt"
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

var tagDocs = cli.CommandDocumentationContent{
	ShortDesc: `Create, list, delete tags.`,
	LongDesc:  ``,
	Synopsis:  []string{},
}

const (
	tagMessageArg = "message"
)

type TagCmd struct{}

// TaggerName returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd TagCmd) Name() string {
	return "tag"
}

// Description returns a description of the command
func (cmd TagCmd) Description() string {
	return "Create, list, delete tags."
}

// Hidden should return true if this command should be hidden from the help text
func (cmd *TagCmd) Hidden() bool {
	return true
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd TagCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, branchDocs, ap))
}

func (cmd TagCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	// todo: docs
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"start-point", "A commit that a new branch should point at."})
	ap.SupportsString(tagMessageArg, "m", "msg", "Use the given {{.LessThan}}msg{{.GreaterThan}} as the tag message.")
	ap.SupportsFlag(verboseFlag, "v", "")
	ap.SupportsFlag(deleteFlag, "d", "Delete a tag.")
	return ap
}

// EventType returns the type of the event to log
func (cmd TagCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_BRANCH
}

// Exec executes the command
func (cmd TagCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, tagDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	// list tags
	if len(apr.Args()) == 0 {
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
			err := actions.DeleteTags(ctx, dEnv, apr.Args()...)
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
	} else if len(apr.Args()) > 2 {
		verr = errhand.BuildDError("create tag takes at most two args").Build()
	} else {
		props, err := getTagProps(dEnv, apr)
		if err != nil {
			verr = errhand.BuildDError("failed to get tag props").AddCause(err).Build()
			return HandleVErrAndExitCode(verr, usage)
		}
		tagName := apr.Arg(0)
		startPoint := "head"
		if len(apr.Args()) > 1 {
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
	name, email, err := actions.GetNameAndEmail(dEnv.Config)
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
		err = actions.IterResolvedTags(ctx, dEnv, func(tag ref.DoltRef, c *doltdb.Commit, meta *doltdb.TagMeta) (bool, error) {
			h, err := c.HashOf()
			if err != nil {
				return false, nil
			}

			verboseTagPrint(tag, h, meta)
			return false, nil
		})
	} else {
		err = actions.IterResolvedTags(ctx, dEnv, func(tag ref.DoltRef, _ *doltdb.Commit, _ *doltdb.TagMeta) (bool, error) {
			cli.Println(fmt.Sprintf("\t%s", tag.GetPath()))
			return false, nil
		})
	}

	if err != nil {
		return errhand.BuildDError("error listing tags").AddCause(err).Build()
	}

	return nil
}

func verboseTagPrint(tag ref.DoltRef, h hash.Hash, meta *doltdb.TagMeta) {
	cli.Println(color.YellowString("%s\t%s", tag.GetPath(), h.String()))

	cli.Printf("Tagger: %s <%s>\n", meta.Name, meta.Email)

	timeStr := meta.FormatTS()
	cli.Println("Date:  ", timeStr)

	if meta.Description != "" {
		formattedDesc := "\n\t" + strings.Replace(meta.Description, "\n", "\n\t", -1)
		cli.Println(formattedDesc)
	}
	cli.Println("")
}
