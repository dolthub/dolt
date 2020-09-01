// Copyright 2019 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/events"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/earl"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

const (
	ForceFetchFlag = "force"
)

var fetchDocs = cli.CommandDocumentationContent{
	ShortDesc: "Download objects and refs from another repository",
	LongDesc: `Fetch refs, along with the objects necessary to complete their histories and update remote-tracking branches.

By default dolt will attempt to fetch from a remote named {{.EmphasisLeft}}origin{{.EmphasisRight}}.  The {{.LessThan}}remote{{.GreaterThan}} parameter allows you to specify the name of a different remote you wish to pull from by the remote's name.

When no refspec(s) are specified on the command line, the fetch_specs for the default remote are used.
`,

	Synopsis: []string{
		"[{{.LessThan}}remote{{.GreaterThan}}] [{{.LessThan}}refspec{{.GreaterThan}} ...]",
	},
}

type FetchCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd FetchCmd) Name() string {
	return "fetch"
}

// Description returns a description of the command
func (cmd FetchCmd) Description() string {
	return "Update the database from a remote data repository."
}

// EventType returns the type of the event to log
func (cmd FetchCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_FETCH
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd FetchCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, fetchDocs, ap))
}

func (cmd FetchCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(ForceFetchFlag, "f", "Update refs to remote branches with the current state of the remote, overwriting any conflicting history.")
	return ap
}

// Exec executes the command
func (cmd FetchCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, fetchDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	remotes, _ := dEnv.GetRemotes()
	r, refSpecs, verr := getRefSpecs(apr.Args(), dEnv, remotes)

	updateMode := ref.RefUpdateMode{Force: apr.Contains(ForceFetchFlag)}

	if verr == nil {
		verr = fetchRefSpecs(ctx, updateMode, dEnv, r, refSpecs)
	}

	return HandleVErrAndExitCode(verr, usage)
}

func getRefSpecs(args []string, dEnv *env.DoltEnv, remotes map[string]env.Remote) (env.Remote, []ref.RemoteRefSpec, errhand.VerboseError) {
	if len(remotes) == 0 {
		return env.NoRemote, nil, errhand.BuildDError("error: no remotes set").AddDetails("to add a remote run: dolt remote add <remote> <url>").Build()
	}

	remName := "origin"
	remote, remoteOK := remotes[remName]

	if len(args) != 0 {
		if val, ok := remotes[args[0]]; ok {
			remName = args[0]
			remote = val
			remoteOK = ok
			args = args[1:]
		}
	}

	if !remoteOK {
		return env.NoRemote, nil, errhand.BuildDError("error: unknown remote").SetPrintUsage().Build()
	}

	var rs []ref.RemoteRefSpec
	var verr errhand.VerboseError
	if len(args) != 0 {
		rs, verr = parseRSFromArgs(remName, args)
	} else {
		rs, verr = dEnv.GetRefSpecs(remName)
	}

	if verr != nil {
		return env.NoRemote, nil, verr
	}

	return remote, rs, verr
}

func parseRSFromArgs(remName string, args []string) ([]ref.RemoteRefSpec, errhand.VerboseError) {
	var refSpecs []ref.RemoteRefSpec
	for i := 0; i < len(args); i++ {
		rsStr := args[i]
		rs, err := ref.ParseRefSpec(rsStr)

		if err != nil {
			return nil, errhand.BuildDError("error: '%s' is not a valid refspec.", rsStr).SetPrintUsage().Build()
		}

		if _, ok := rs.(ref.BranchToBranchRefSpec); ok {
			local := "refs/heads/" + rsStr
			remTracking := "remotes/" + remName + "/" + rsStr
			rs2, err := ref.ParseRefSpec(local + ":" + remTracking)

			if err == nil {
				rs = rs2
			}
		}

		if rrs, ok := rs.(ref.RemoteRefSpec); !ok {
			return nil, errhand.BuildDError("error: '%s' is not a valid refspec referring to a remote tracking branch", rsStr).Build()
		} else {
			refSpecs = append(refSpecs, rrs)
		}
	}

	return refSpecs, nil
}

func mapRefspecsToRemotes(refSpecs []ref.RemoteRefSpec, dEnv *env.DoltEnv) (map[ref.RemoteRefSpec]env.Remote, errhand.VerboseError) {
	nameToRemote := dEnv.RepoState.Remotes

	rsToRem := make(map[ref.RemoteRefSpec]env.Remote)
	for _, rrs := range refSpecs {
		remName := rrs.GetRemote()

		if rem, ok := nameToRemote[remName]; !ok {
			return nil, errhand.BuildDError("error: unknown remote '%s'", remName).Build()
		} else {
			rsToRem[rrs] = rem
		}
	}

	return rsToRem, nil
}

func fetchRefSpecs(ctx context.Context, mode ref.RefUpdateMode, dEnv *env.DoltEnv, rem env.Remote, refSpecs []ref.RemoteRefSpec) errhand.VerboseError {
	srcDB, err := rem.GetRemoteDB(ctx, dEnv.DoltDB.ValueReadWriter().Format())

	if err != nil {
		return errhand.BuildDError("error: failed to get remote db").AddCause(err).Build()
	}

	for _, rs := range refSpecs {

		branchRefs, err := srcDB.GetRefs(ctx)

		if err != nil {
			return errhand.BuildDError("error: failed to read from ").AddCause(err).Build()
		}

		for _, branchRef := range branchRefs {
			remoteTrackRef := rs.DestRef(branchRef)

			if remoteTrackRef != nil {
				srcDBCommit, verr := fetchRemoteBranch(ctx, dEnv, rem, srcDB, dEnv.DoltDB, branchRef, remoteTrackRef)

				if verr != nil {
					return verr
				}

				switch mode {
				case ref.ForceUpdate:
					err = dEnv.DoltDB.SetHeadToCommit(ctx, remoteTrackRef, srcDBCommit)
				case ref.FastForwardOnly:
					ok, err := dEnv.DoltDB.CanFastForward(ctx, remoteTrackRef, srcDBCommit)
					if !ok {
						return errhand.BuildDError("error: fetch failed, can't fast forward remote tracking ref").Build()
					}
					if err == nil {
						err = dEnv.DoltDB.FastForward(ctx, remoteTrackRef, srcDBCommit)
					}
				}

				if err != nil {
					return errhand.BuildDError("error: fetch failed").AddCause(err).Build()
				}
			}
		}
	}

	verr := fetchFollowTags(ctx, dEnv, srcDB, dEnv.DoltDB)

	if verr != nil {
		return verr
	}

	return nil
}

func fetchRemoteBranch(ctx context.Context, dEnv *env.DoltEnv, rem env.Remote, srcDB, destDB *doltdb.DoltDB, srcRef, destRef ref.DoltRef) (*doltdb.Commit, errhand.VerboseError) {
	evt := events.GetEventFromContext(ctx)

	u, err := earl.Parse(rem.Url)

	if err == nil {
		if u.Scheme != "" {
			evt.SetAttribute(eventsapi.AttributeID_REMOTE_URL_SCHEME, u.Scheme)
		}
	}

	cs, _ := doltdb.NewCommitSpec(srcRef.String())
	srcDBCommit, err := srcDB.Resolve(ctx, cs, nil)

	if err != nil {
		return nil, errhand.BuildDError("error: unable to find '%s' on '%s'", srcRef.GetPath(), rem.Name).Build()
	} else {
		wg, progChan, pullerEventCh := runProgFuncs()
		err = actions.FetchCommit(ctx, dEnv, srcDB, destDB, srcDBCommit, progChan, pullerEventCh)
		stopProgFuncs(wg, progChan, pullerEventCh)

		if err != nil {
			return nil, errhand.BuildDError("error: fetch failed").AddCause(err).Build()
		}
	}

	return srcDBCommit, nil
}

// fetchFollowTags fetches all tags from the source DB whose commits have already
// been fetched into the destination DB.
// todo: potentially too expensive to iterate over all srcDB tags
func fetchFollowTags(ctx context.Context, dEnv *env.DoltEnv, srcDB, destDB *doltdb.DoltDB) errhand.VerboseError {
	err := actions.IterResolvedTags(ctx, srcDB, func(tag *doltdb.Tag) (stop bool, err error) {
		stRef, err := tag.GetStRef()
		if err != nil {
			return true, err
		}

		tagHash := stRef.TargetHash()

		tv, err := destDB.ValueReadWriter().ReadValue(ctx, tagHash)
		if err != nil {
			return true, err
		}
		if tv != nil {
			// tag is already fetched
			return false, nil
		}

		cmHash, err := tag.Commit.HashOf()
		if err != nil {
			return true, err
		}

		cv, err := destDB.ValueReadWriter().ReadValue(ctx, cmHash)
		if err != nil {
			return true, err
		}
		if cv == nil {
			// neither tag nor commit has been fetched
			return false, nil
		}

		wg, progChan, pullerEventCh := runProgFuncs()
		err = actions.FetchTag(ctx, dEnv, srcDB, destDB, tag, progChan, pullerEventCh)
		stopProgFuncs(wg, progChan, pullerEventCh)

		if err != nil {
			return true, err
		}

		err = destDB.SetHead(ctx, tag.GetDoltRef(), stRef)

		return false, err
	})

	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	return nil
}
