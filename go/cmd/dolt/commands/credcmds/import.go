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
	"io"
	"os"

	"google.golang.org/grpc"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var importDocs = cli.CommandDocumentationContent{
	ShortDesc: "Import a dolt credential from an existing .jwk file.",
	LongDesc: `Imports a dolt credential from an existing .jwk file.

Dolt credentials are stored in the creds subdirectory of the global dolt config
directory as files with one key per file in JWK format. This command can import
a JWK from a file or stdin and places the imported key in the correct place for
dolt to find it as a valid credential.

This command will set the newly imported credential as the used credential if
there are currently not credentials. If this command does use the new
credential, it will call doltremoteapi to update user.name and user.email with
information from the remote user profile if those fields are not already
available in the local dolt config.`,
	Synopsis: []string{"[--no-profile] [{{.LessThan}}jwk_filename{{.GreaterThan}}]"},
}

type ImportCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ImportCmd) Name() string {
	return "import"
}

// Description returns a description of the command
func (cmd ImportCmd) Description() string {
	return importDocs.ShortDesc
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd ImportCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return commands.CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, importDocs, ap))
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd ImportCmd) RequiresRepo() bool {
	return false
}

// EventType returns the type of the event to log
func (cmd ImportCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CREDS_IMPORT
}

const noProfileFlag = "no-profile"

func (cmd ImportCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"jwk_filename", "The JWK file. If omitted, import operates on stdin."})
	ap.SupportsFlag(noProfileFlag, "", "If provided, no attempt will be made to contact doltremoteapi and update user.name and user.email.")
	return ap
}

// Exec executes the command
func (cmd ImportCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, importDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	credsDir, verr := actions.EnsureCredsDir(dEnv)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	noprofile := apr.Contains(noProfileFlag)
	var input io.ReadCloser = os.Stdin
	if apr.NArg() == 1 {
		var err error
		input, err = dEnv.FS.OpenForRead(apr.Arg(0))
		if err != nil {
			verr = errhand.BuildDError("error: cannot open %s", apr.Arg(0)).AddCause(err).Build()
			return commands.HandleVErrAndExitCode(verr, usage)
		}
		defer input.Close()
	}

	c, err := creds.JWKCredsRead(input)
	if err != nil {
		verr = errhand.BuildDError("error: could not read JWK").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}
	if !c.IsPrivKeyValid() || !c.IsPubKeyValid() {
		verr = errhand.BuildDError("error: deserialized JWK was not valid").Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	_, err = creds.JWKCredsWriteToDir(dEnv.FS, credsDir, c)
	if err != nil {
		verr = errhand.BuildDError("error: could not write credentials to file").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}
	cli.Println("Imported credential:", c.PubKeyBase32Str())

	err = updateConfigToUseNewCredIfNoExistingCred(dEnv, c)
	if err != nil {
		cli.Println("Warning: could not update profile to use imported credential:", err)
	}

	if !noprofile {
		err := updateProfileWithCredentials(ctx, dEnv, c)
		if err != nil {
			cli.Println("Warning: could not update profile with imported and used credentials:", err)
		}
	}

	return 0
}

func updateProfileWithCredentials(ctx context.Context, dEnv *env.DoltEnv, c creds.DoltCreds) error {
	gcfg, hasGCfg := dEnv.Config.GetConfig(env.GlobalConfig)
	if !hasGCfg {
		panic("Should have global config here...")
	}

	if _, err := gcfg.GetString(env.UserNameKey); err == nil {
		// Already has a name...
		return nil
	}
	if _, err := gcfg.GetString(env.UserEmailKey); err == nil {
		// Already has an email...
		return nil
	}

	host := dEnv.Config.GetStringOrDefault(env.RemotesApiHostKey, env.DefaultRemotesApiHost)
	port := dEnv.Config.GetStringOrDefault(env.RemotesApiHostPortKey, env.DefaultRemotesApiPort)
	hostAndPort := fmt.Sprintf("%s:%s", host, port)
	endpoint, opts, err := dEnv.GetGRPCDialParams(grpcendpoint.Config{
		Endpoint: hostAndPort,
		Creds:    c,
	})
	if err != nil {
		return fmt.Errorf("error: unable to build dial options server with credentials: %w", err)
	}
	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return fmt.Errorf("error: unable to connect to server with credentials: %w", err)
	}
	grpcClient := remotesapi.NewCredentialsServiceClient(conn)
	resp, err := grpcClient.WhoAmI(ctx, &remotesapi.WhoAmIRequest{})
	if err != nil {
		return fmt.Errorf("error: unable to call WhoAmI endpoint: %w", err)
	}
	userUpdates := map[string]string{
		env.UserNameKey:  resp.DisplayName,
		env.UserEmailKey: resp.EmailAddress,
	}
	return gcfg.SetStrings(userUpdates)
}
