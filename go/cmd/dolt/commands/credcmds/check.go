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

package credcmds

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

var checkShortDesc = "Check authenticating with a credential keypair against a doltremoteapi."
var checkLongDesc = `Tests calling a doltremoteapi with dolt credentials and reports the authentication result.`
var checkSynopsis = []string{"[--endpoint doltremoteapi.dolthub.com:443] [--creds {{.LessThan}}eak95022q3vskvumn2fcrpibdnheq1dtr8t...{{.GreaterThan}}]"}

var checkDocs = cli.CommandDocumentationContent{
	ShortDesc: "Check authenticating with a credential keypair against a doltremoteapi.",
	LongDesc:  `Tests calling a doltremoteapi with dolt credentials and reports the authentication result.`,
	Synopsis:  []string{"[--endpoint doltremoteapi.dolthub.com:443] [--creds {{.LessThan}}eak95022q3vskvumn2fcrpibdnheq1dtr8t...{{.GreaterThan}}]"},
}

type CheckCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CheckCmd) Name() string {
	return "check"
}

// Description returns a description of the command
func (cmd CheckCmd) Description() string {
	return checkShortDesc
}

func (cmd CheckCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(checkDocs, ap)
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd CheckCmd) RequiresRepo() bool {
	return false
}

// EventType returns the type of the event to log
func (cmd CheckCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CREDS_CHECK
}

func (cmd CheckCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsString("endpoint", "", "", "API endpoint, otherwise taken from config.")
	ap.SupportsString("creds", "", "", "Public Key ID or Public Key for credentials, otherwise taken from config.")
	return ap
}

// Exec executes the command
func (cmd CheckCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, checkDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	host, endpoint := loadEndpoint(dEnv, apr)

	dc, verr := loadCred(dEnv, apr)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	verr = checkCredAndPrintSuccess(ctx, dEnv, dc, host, endpoint)

	return commands.HandleVErrAndExitCode(verr, usage)
}

func loadEndpoint(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (string, string) {
	earg, ok := apr.GetValue("endpoint")
	if ok {
		return getHostFromEndpoint(earg), earg
	}

	host := dEnv.Config.GetStringOrDefault(config.RemotesApiHostKey, env.DefaultRemotesApiHost)
	port := dEnv.Config.GetStringOrDefault(config.RemotesApiHostPortKey, env.DefaultRemotesApiPort)
	return host, fmt.Sprintf("%s:%s", host, port)
}

func getHostFromEndpoint(endpoint string) string {
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		cli.Printf("error getting host name from provided endpoint '%s': %v\nUsing default host instead: %s\n", endpoint, err, env.DefaultRemotesApiHost)
		return env.DefaultRemotesApiHost
	}
	return host
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
		dc, valid, err := dEnv.UserDoltCreds()
		if !valid {
			return creds.EmptyCreds, errhand.BuildDError("error: no user credentials found").Build()
		}
		if err != nil {
			return creds.EmptyCreds, errhand.BuildDError("error: reading credentials").AddCause(err).Build()
		}
		return dc, nil
	}
}

func checkCredAndPrintSuccess(ctx context.Context, dEnv *env.DoltEnv, dc creds.DoltCreds, authHost, endpoint string) errhand.VerboseError {
	cfg, err := dEnv.GetGRPCDialParams(grpcendpoint.Config{
		Endpoint: endpoint,
		Creds:    dc.RPCCreds(authHost),
	})
	if err != nil {
		return errhand.BuildDError("error: unable to build server endpoint options.").AddCause(err).Build()
	}
	conn, err := grpc.Dial(cfg.Endpoint, cfg.DialOptions...)
	if err != nil {
		return errhand.BuildDError("error: unable to connect to server with credentials.").AddCause(err).Build()
	}
	defer conn.Close()

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
