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
	"fmt"
	"time"

	"github.com/skratchdot/open-golang/open"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	remotesapi "github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/creds"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

const (
	loginRetryInterval = 5
)

var loginShortDesc = ""
var loginLongDesc = ""
var loginSynopsis = []string{
	"[<creds>]",
}

func Login(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["creds"] = "A specific credential to use for login."
	help, usage := cli.HelpAndUsagePrinters(commandStr, loginShortDesc, loginLongDesc, loginSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	var verr errhand.VerboseError
	if apr.NArg() == 0 {
		verr = loginWithNewCreds(dEnv)
	} else if apr.NArg() == 1 {
		verr = loginWithExistingCreds(dEnv, apr.Arg(0))
	} else {
		verr = errhand.BuildDError("").SetPrintUsage().Build()
	}

	return HandleVErrAndExitCode(verr, usage)
}

func loginWithNewCreds(dEnv *env.DoltEnv) errhand.VerboseError {
	path, dc, err := actions.NewCredsFile(dEnv)

	if err != nil {
		return errhand.BuildDError("error: Unable to create credentials.").AddCause(err).Build()
	}

	cli.Println(path)

	return loginWithCreds(dEnv, dc)
}

func loginWithExistingCreds(dEnv *env.DoltEnv, idOrPubKey string) errhand.VerboseError {
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

	return loginWithCreds(dEnv, dc)
}

func loginWithCreds(dEnv *env.DoltEnv, dc creds.DoltCreds) errhand.VerboseError {
	loginUrl := dEnv.Config.GetStringOrDefault(env.AddCredsUrlKey, env.DefaultLoginUrl)
	url := fmt.Sprintf("%s#%s", *loginUrl, dc.PubKeyBase32Str())

	cli.Printf("Opening a browser to:\n\t%s\nPlease associate your key with your account.\n", url)
	open.Start(url)

	host := dEnv.Config.GetStringOrDefault(env.RemotesApiHostKey, env.DefaultRemotesApiHost)
	port := dEnv.Config.GetStringOrDefault(env.RemotesApiHostPortKey, env.DefaultRemotesApiPort)
	conn, err := dEnv.GrpcConnWithCreds(fmt.Sprintf("%s:%s", *host, *port), false, dc)

	if err != nil {
		return errhand.BuildDError("error: unable to connect to server with credentials.").AddCause(err).Build()
	}

	grpcClient := remotesapi.NewCredentialsServiceClient(conn)

	cli.Println("Checking remote server looking for key association.")

	var prevMsgLen int
	var whoAmI *remotesapi.WhoAmIResponse
	for whoAmI == nil {
		prevMsgLen = cli.DeleteAndPrint(prevMsgLen, "requesting update")
		whoAmI, err = grpcClient.WhoAmI(context.Background(), &remotesapi.WhoAmIRequest{})

		if err != nil {
			for i := 0; i < loginRetryInterval; i++ {
				prevMsgLen = cli.DeleteAndPrint(prevMsgLen, fmt.Sprintf("Retrying in %d", loginRetryInterval-i))
				time.Sleep(time.Second)
			}
		}
	}

	cli.Printf("\n\nKey successfully associated with user: %s email %s\n", whoAmI.Username, whoAmI.EmailAddress)

	updateConfig(dEnv, whoAmI, dc)

	return nil
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
