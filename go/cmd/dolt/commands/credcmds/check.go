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

package credcmds

import (
	"context"
	"fmt"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	remotesapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/creds"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

var checkShortDesc = "Check authenticating with a credential keypair against a doltremoteapi."
var checkLongDesc = `Tests calling a doltremoteapi with dolt credentials and reports the authentication result.`
var checkSynopsis = []string{"[--endpoint doltremoteapi.dolthub.com:443] [--creds <eak95022q3vskvumn2fcrpibdnheq1dtr8t...>]"}

type CheckCmd struct{}

func (cmd CheckCmd) Name() string {
	return "check"
}

func (cmd CheckCmd) Description() string {
	return checkShortDesc
}

func (cmd CheckCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, checkShortDesc, checkLongDesc, checkSynopsis, ap)
}

func (cmd CheckCmd) RequiresRepo() bool {
	return false
}

func (cmd CheckCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CREDS_CHECK
}

func (cmd CheckCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString("endpoint", "", "", "API endpoint, otherwise taken from config.")
	ap.SupportsString("creds", "", "", "Public Key ID or Public Key for credentials, otherwise taken from config.")
	return ap
}

func (cmd CheckCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, lsShortDesc, lsLongDesc, lsSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	endpoint := loadEndpoint(dEnv, apr)

	dc, verr := loadCred(dEnv, apr)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	verr = checkCredAndPrintSuccess(ctx, dEnv, dc, endpoint)

	return commands.HandleVErrAndExitCode(verr, usage)
}

func loadEndpoint(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) string {
	earg, ok := apr.GetValue("endpoint")
	if ok {
		return earg
	}

	host := dEnv.Config.GetStringOrDefault(env.RemotesApiHostKey, env.DefaultRemotesApiHost)
	port := dEnv.Config.GetStringOrDefault(env.RemotesApiHostPortKey, env.DefaultRemotesApiPort)
	return fmt.Sprintf("%s:%s", *host, *port)
}

func loadCred(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (creds.DoltCreds, errhand.VerboseError) {
	keyIdOrPubKey, argSupplied := apr.GetValue("creds")
	if argSupplied {
		credsdir, err := dEnv.CredsDir()
		if err != nil {
			return creds.EmptyCreds, errhand.BuildDError("error: reading credentials").AddCause(err).Build()
		}

		found, err := dEnv.FindCreds(credsdir, keyIdOrPubKey)
		if err != nil {
			return creds.EmptyCreds, errhand.BuildDError("error: finding credential %s", keyIdOrPubKey).AddCause(err).Build()
		}

		dc, err := creds.JWKCredsReadFromFile(dEnv.FS, found)
		if err != nil {
			return creds.EmptyCreds, errhand.BuildDError("error: reading credentials").AddCause(err).Build()
		}
		return dc, nil
	} else {
		dc, valid, err := dEnv.UserRPCCreds()
		if !valid {
			return creds.EmptyCreds, errhand.BuildDError("error: no user credentials found").Build()
		}
		if err != nil {
			return creds.EmptyCreds, errhand.BuildDError("error: reading credentials").AddCause(err).Build()
		}
		return dc, nil
	}
}

func checkCredAndPrintSuccess(ctx context.Context, dEnv *env.DoltEnv, dc creds.DoltCreds, endpoint string) errhand.VerboseError {
	conn, err := dEnv.GrpcConnWithCreds(endpoint, false, dc)

	if err != nil {
		return errhand.BuildDError("error: unable to connect to server with credentials.").AddCause(err).Build()
	}

	grpcClient := remotesapi.NewCredentialsServiceClient(conn)

	cli.Printf("Calling...\n")
	cli.Printf("  Endpoint: %s\n", endpoint)
	cli.Printf("  Key: %s\n", dc.PubKeyBase32Str())

	var whoAmI *remotesapi.WhoAmIResponse
	whoAmI, err = grpcClient.WhoAmI(ctx, &remotesapi.WhoAmIRequest{})
	if err != nil {
		return errhand.BuildDError("error: calling doltremoteapi with credentials.").AddCause(err).Build()
	}

	cli.Printf("\nSuccess.\n")
	cli.Printf("  User: %s\n", whoAmI.Username)
	cli.Printf("  Email: %s\n", whoAmI.EmailAddress)
	return nil
}
