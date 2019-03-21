package commands

import (
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"time"

	"context"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/creds"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/skratchdot/open-golang/open"
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
		verr = errhand.BuildDError("invalid usage").SetPrintUsage().Build()
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
	url := fmt.Sprintf("%s?pub=%s", *loginUrl, dc.PubKeyBase32Str())

	cli.Printf("Opening a browser to:\n\t%s\nPlease associate your key with your account.\n", url)
	open.Start(url)

	host := dEnv.Config.GetStringOrDefault(env.RemotesApiHostKey, env.DefaultRemotesApiHost)
	port := dEnv.Config.GetStringOrDefault(env.RemotesApiHostPortKey, env.DefaultRemotesApiPort)
	conn, err := dEnv.GrpcConn(fmt.Sprintf("%s:%d", *host, port))

	if err != nil {
		return errhand.BuildDError("error: unable to connect to server with credentials.").AddCause(err).Build()
	}

	grpcClient := remotesapi.NewCredentialsServiceClient(conn)

	var prevMsgLen int
	var whoAmI *remotesapi.WhoAmIResponse
	for whoAmI == nil {
		prevMsgLen = cli.DeleteAndPrint(prevMsgLen, "Checking remote server looking for key association.")
		whoAmI, err = grpcClient.WhoAmI(context.Background(), &remotesapi.WhoAmIRequest{})

		if err != nil {
			for i := 0; i < loginRetryInterval; i++ {
				prevMsgLen = cli.DeleteAndPrint(prevMsgLen, fmt.Sprintf("Retrying in %d", loginRetryInterval-i))
				time.Sleep(time.Second)
			}
		}
	}

	return nil
}
