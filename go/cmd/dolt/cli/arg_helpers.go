package cli

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/argparser"
	"os"
)

type UsagePrinter func()

func ParseArgs(ap *argparser.ArgParser, args []string, usagePrinter UsagePrinter) *argparser.ArgParseResults {
	apr, err := ap.Parse(args)

	if err != nil {
		if err != argparser.ErrHelp {
			PrintErrln(err.Error())
		}

		usagePrinter()
		os.Exit(1)
	}

	return apr
}

func HelpAndUsagePrinters(commandStr, shortDesc, longDesc string, synopsis []string, ap *argparser.ArgParser) (UsagePrinter, UsagePrinter) {
	return func() {
			PrintHelpText(commandStr, shortDesc, longDesc, synopsis, ap)
		}, func() {
			PrintUsage(commandStr, synopsis, ap)
		}
}
