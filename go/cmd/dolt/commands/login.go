// Copyright 2019 Dolthub, Inc.
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
	"time"

	"github.com/skratchdot/open-golang/open"
	"google.golang.org/grpc"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const (
	loginRetryInterval = 5
)

var loginDocs = cli.CommandDocumentationContent{
	ShortDesc: "Login to DoltHub",
	LongDesc: `Login into DoltHub using the email in your config so you can pull from private repos and push to those you have permission to.
`,
	Synopsis: []string{"[{{.LessThan}}creds{{.GreaterThan}}]"},
}

// The LoginCmd doesn't handle its own signals, but should stop cancel global context when receiving SIGINT signal
func (cmd LoginCmd) InstallsSignalHandlers() bool {
	return true
}

type LoginCmd struct{}

var _ cli.SignalCommand = SqlCmd{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd LoginCmd) Name() string {
	return "login"
}

// Description returns a description of the command
func (cmd LoginCmd) Description() string {
	return "Login to a dolt remote host."
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd LoginCmd) RequiresRepo() bool {
	return false
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd LoginCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, loginDocs, ap))
}

func (cmd LoginCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"creds", "A specific credential to use for login."})
	return ap
}

// EventType returns the type of the event to log
func (cmd LoginCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_LOGIN
}

// Exec executes the command
func (cmd LoginCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, loginDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	var verr errhand.VerboseError
	if apr.NArg() == 0 {
		verr = loginWithNewCreds(ctx, dEnv)
	} else if apr.NArg() == 1 {
		verr = loginWithExistingCreds(ctx, dEnv, apr.Arg(0))
	} else {
		verr = errhand.BuildDError("").SetPrintUsage().Build()
	}

	return HandleVErrAndExitCode(verr, usage)
}

// Specifies behavior of the login.
type loginBehavior int

// When logging in with newly minted credentials, they cannot be on the server
// yet. So open the browser immediately before checking the server.
var openBrowserFirst loginBehavior = 1

// When logging in with supplied credentials, they may already be associated
// with an account on the server. Check first before opening a browser.
var checkCredentialsThenOpenBrowser loginBehavior = 2

func loginWithNewCreds(ctx context.Context, dEnv *env.DoltEnv) errhand.VerboseError {
	path, dc, err := actions.NewCredsFile(dEnv)

	if err != nil {
		return errhand.BuildDError("error: Unable to create credentials.").AddCause(err).Build()
	}

	cli.Println("Credentials created successfully.")
	cli.Println("pub key:", dc.PubKeyBase32Str())

	cli.Println(path)

	return loginWithCreds(ctx, dEnv, dc, openBrowserFirst)
}

func loginWithExistingCreds(ctx context.Context, dEnv *env.DoltEnv, idOrPubKey string) errhand.VerboseError {
	credsDir, err := dEnv.CredsDir()

	if err != nil {
		return errhand.BuildDError("error: could not get user home dir").Build()
	}

	jwkFilePath, err := dEnv.FindCreds(credsDir, idOrPubKey)

	if err != nil {
		return errhand.BuildDError("error: failed to find creds '%s'", idOrPubKey).AddCause(err).Build()
	}

	dc, err := creds.JWKCredsReadFromFile(dEnv.FS, jwkFilePath)

	if err != nil {
		return errhand.BuildDError("error: failed to load creds from file").AddCause(err).Build()
	}

	return loginWithCreds(ctx, dEnv, dc, checkCredentialsThenOpenBrowser)
}

func loginWithCreds(ctx context.Context, dEnv *env.DoltEnv, dc creds.DoltCreds, behavior loginBehavior) errhand.VerboseError {
	grpcClient, verr := getCredentialsClient(dEnv, dc)
	if verr != nil {
		return verr
	}

	var whoAmI *remotesapi.WhoAmIResponse
	var err error
	if behavior == checkCredentialsThenOpenBrowser {
		whoAmI, err = grpcClient.WhoAmI(ctx, &remotesapi.WhoAmIRequest{})
	}

	if whoAmI == nil {
		openBrowserForCredsAdd(dEnv, dc)
		cli.Println("Checking remote server looking for key association.")
	}

	linePrinter := func() func(line string) {
		prevMsgLen := 0
		return func(line string) {
			prevMsgLen = cli.DeleteAndPrint(prevMsgLen, line)
		}
	}()

	for whoAmI == nil {
		linePrinter("requesting update")
		whoAmI, err = grpcClient.WhoAmI(ctx, &remotesapi.WhoAmIRequest{})
		if err != nil {
			for i := 0; i < loginRetryInterval; i++ {
				linePrinter(fmt.Sprintf("Retrying in %d", loginRetryInterval-i))
				time.Sleep(time.Second)
			}
		} else {
			cli.Printf("\n\n")
		}
	}

	cli.Printf("Key successfully associated with user: %s email %s\n", whoAmI.Username, whoAmI.EmailAddress)

	updateConfig(dEnv, whoAmI, dc)

	return nil
}

func openBrowserForCredsAdd(dEnv *env.DoltEnv, dc creds.DoltCreds) {
	loginUrl := dEnv.Config.GetStringOrDefault(env.AddCredsUrlKey, env.DefaultLoginUrl)
	url := fmt.Sprintf("%s#%s", loginUrl, dc.PubKeyBase32Str())
	cli.Printf("Opening a browser to:\n\t%s\nPlease associate your key with your account.\n", url)
	open.Start(url)
}

func getCredentialsClient(dEnv *env.DoltEnv, dc creds.DoltCreds) (remotesapi.CredentialsServiceClient, errhand.VerboseError) {
	host := dEnv.Config.GetStringOrDefault(env.RemotesApiHostKey, env.DefaultRemotesApiHost)
	port := dEnv.Config.GetStringOrDefault(env.RemotesApiHostPortKey, env.DefaultRemotesApiPort)
	endpoint, opts, err := dEnv.GetGRPCDialParams(grpcendpoint.Config{
		Endpoint: fmt.Sprintf("%s:%s", host, port),
		Creds:    dc,
	})
	if err != nil {
		return nil, errhand.BuildDError("error: unable to build dial options for connecting to server with credentials.").AddCause(err).Build()
	}
	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return nil, errhand.BuildDError("error: unable to connect to server with credentials.").AddCause(err).Build()
	}
	return remotesapi.NewCredentialsServiceClient(conn), nil
}

func updateConfig(dEnv *env.DoltEnv, whoAmI *remotesapi.WhoAmIResponse, dCreds creds.DoltCreds) {
	gcfg, hasGCfg := dEnv.Config.GetConfig(env.GlobalConfig)

	if !hasGCfg {
		panic("global config not found.  Should create it here if this is a thing.")
	}

	gcfg.SetStrings(map[string]string{env.UserCreds: dCreds.KeyIDBase32Str()})

	userUpdates := map[string]string{env.UserNameKey: whoAmI.DisplayName, env.UserEmailKey: whoAmI.EmailAddress}
	lcfg, hasLCfg := dEnv.Config.GetConfig(env.LocalConfig)

	if hasLCfg {
		lcfg.SetStrings(userUpdates)
	} else {
		gcfg.SetStrings(userUpdates)
	}
}
