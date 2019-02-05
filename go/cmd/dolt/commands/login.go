package commands

import (
	"context"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/gen/proto/services/dolt/v1alpha1"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/creds"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/skratchdot/open-golang/open"
	"path"
	"time"
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

	jwkFilePath, err := creds.FindCreds(dEnv, credsDir, idOrPubKey)

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
	host := dEnv.Config.GetStringOrDefault(env.RemotesHostKey, env.DefaultRemotesHost)
	loginPath := dEnv.Config.GetStringOrDefault(env.RemotesLoginPathKey, env.DefaultLoginPath)
	fullPath := path.Join(*host, *loginPath)
	url := fmt.Sprintf("https://%s?pub=%s", fullPath, dc.PubKeyBase32Str())

	cli.Printf("Opening a browser to:\n\t%s\nPlease associate your key with your account.\n", url)
	open.Start(url)

	conn, err := dEnv.GrpcConn(dc)

	if err != nil {
		return errhand.BuildDError("error: unable to connect to server with credentials.").AddCause(err).Build()
	}

	grpcClient := v1alpha1.NewCredentialsServiceClient(conn)

	var prevMsgLen int
	var whoAmI *v1alpha1.WhoAmIResponse
	for whoAmI == nil {
		prevMsgLen = cli.DeleteAndPrint(prevMsgLen, "Checking remote server looking for key association.")
		whoAmI, err = grpcClient.WhoAmI(context.Background(), &v1alpha1.WhoAmIRequest{})

		if err != nil {
			for i := 0; i < loginRetryInterval; i++ {
				prevMsgLen = cli.DeleteAndPrint(prevMsgLen, fmt.Sprintf("Retrying in %d", loginRetryInterval-i))
				time.Sleep(time.Second)
			}
		}
	}

	return nil
}
