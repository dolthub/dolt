// Copyright 2023 Dolthub, Inc.
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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fatih/color"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

var profileDocs = cli.CommandDocumentationContent{
	ShortDesc: "Manage dolt profiles for CLI global options.",
	LongDesc: `With no arguments, shows a list of existing profiles. Two subcommands are available to perform operations on the profiles.

{{.EmphasisLeft}}add{{.EmphasisRight}}
Adds a profile named {{.LessThan}}name{{.GreaterThan}}. Returns an error if the profile already exists.

{{.EmphasisLeft}}remove{{.EmphasisRight}}, {{.EmphasisLeft}}rm{{.EmphasisRight}}
Remove the profile named {{.LessThan}}name{{.GreaterThan}}.`,
	Synopsis: []string{
		"[-v | --verbose]",
		"add [-u {{.LessThan}}user{{.GreaterThan}}] [-p {{.LessThan}}password{{.GreaterThan}}] [--host {{.LessThan}}host{{.GreaterThan}}] [--port {{.LessThan}}port{{.GreaterThan}}] [--no-tls] [--data-dir {{.LessThan}}directory{{.GreaterThan}}] [--doltcfg-dir {{.LessThan}}directory{{.GreaterThan}}] [--privilege-file {{.LessThan}}privilege file{{.GreaterThan}}] [--branch-control-file {{.LessThan}}branch control file{{.GreaterThan}}] [--use-db {{.LessThan}}database{{.GreaterThan}}] {{.LessThan}}name{{.GreaterThan}}",
		"remove {{.LessThan}}name{{.GreaterThan}}",
	},
}

const (
	addProfileId          = "add"
	removeProfileId       = "remove"
	GlobalCfgProfileKey   = "profile"
	DefaultProfileName    = "default"
	defaultProfileWarning = "Default profile has been added. All dolt commands taking global arguments will use this default profile until it is removed.\nWARNING: This will alter the behavior of commands which specify no `--profile`.\nIf you are using dolt in contexts where you expect a `.dolt` directory to be accessed, the default profile will be used instead."
)

type ProfileCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ProfileCmd) Name() string {
	return "profile"
}

// Description returns a description of the command
func (cmd ProfileCmd) Description() string {
	return "Manage dolt profiles for CLI global options."
}

func (cmd ProfileCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(profileDocs, ap)
}

func (cmd ProfileCmd) ArgParser() *argparser.ArgParser {
	ap := cli.CreateGlobalArgParser("profile")
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"name", "Defines the name of the profile to add or remove."})
	ap.SupportsFlag(cli.VerboseFlag, "v", "Includes full details when printing list of profiles.")
	return ap
}

// EventType returns the type of the event to log
func (cmd ProfileCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_PROFILE
}

func (cmd ProfileCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
func (cmd ProfileCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, profileDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	var verr errhand.VerboseError

	switch {
	case apr.NArg() == 0:
		verr = printProfiles(dEnv, apr)
	case apr.Arg(0) == addProfileId:
		verr = addProfile(dEnv, apr)
	case apr.Arg(0) == removeProfileId:
		verr = removeProfile(dEnv, apr)
	default:
		verr = errhand.BuildDError("").SetPrintUsage().Build()
	}

	return HandleVErrAndExitCode(verr, usage)
}

func addProfile(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("Only one profile name can be specified").SetPrintUsage().Build()
	}

	profileName := strings.TrimSpace(apr.Arg(1))

	p := newProfile(apr)
	profStr := p.String()

	cfg, ok := dEnv.Config.GetConfig(env.GlobalConfig)
	if !ok {
		return errhand.BuildDError("error: failed to get global config").Build()
	}
	//TODO: enable config to retrieve json objects instead of just strings
	encodedProfiles, err := cfg.GetString(GlobalCfgProfileKey)
	if err != nil && err != config.ErrConfigParamNotFound {
		return errhand.BuildDError("error: failed to get profiles, %s", err).Build()
	}
	profilesJSON := ""
	profileExists := false
	if encodedProfiles != "" {
		profilesJSON, profileExists, err = decodeProfileAndCheckExists(profileName, encodedProfiles)
		if err != nil {
			return errhand.BuildDError("error: failed to decode profiles, %s", err).Build()
		}
	}
	if profileExists {
		return errhand.BuildDError("error: profile %s already exists, please delete this profile and re-add it if you want to edit any values.", profileName).Build()
	}

	profilesJSON, err = sjson.SetRaw(profilesJSON, profileName, profStr)
	if err != nil {
		return errhand.BuildDError("error: failed to add profile, %s", err).Build()
	}
	err = writeProfileToGlobalConfig(profilesJSON, cfg)
	if err != nil {
		return errhand.BuildDError("error: failed to write profile to config, %s", err).Build()
	}

	if profileName == DefaultProfileName {
		cli.Println(color.YellowString(defaultProfileWarning))
	}

	err = setGlobalConfigPermissions(dEnv)
	if err != nil {
		return errhand.BuildDError("error: failed to set permissions, %s", err).Build()
	}

	return nil
}

func removeProfile(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("Only one profile name can be specified").SetPrintUsage().Build()
	}

	profileName := strings.TrimSpace(apr.Arg(1))

	cfg, ok := dEnv.Config.GetConfig(env.GlobalConfig)
	if !ok {
		return errhand.BuildDError("error: failed to get global config").Build()
	}
	encodedProfiles, err := cfg.GetString(GlobalCfgProfileKey)
	if err != nil {
		if err == config.ErrConfigParamNotFound {
			return errhand.BuildDError("error: no existing profiles").Build()
		}
		return errhand.BuildDError("error: failed to get profiles, %s", err).Build()
	}
	profilesJSON, profileExists, err := decodeProfileAndCheckExists(profileName, encodedProfiles)
	if !profileExists {
		return errhand.BuildDError("error: profile %s does not exist", profileName).Build()
	}

	profilesJSON, err = sjson.Delete(profilesJSON, profileName)
	if err != nil {
		return errhand.BuildDError("error: failed to remove profile, %s", err).Build()
	}
	if profilesJSON == "{}" {
		err = cfg.Unset([]string{GlobalCfgProfileKey})
		if err != nil {
			return errhand.BuildDError("error: failed to remove profile, %s", err).Build()
		}
	} else {
		err = writeProfileToGlobalConfig(profilesJSON, cfg)
		if err != nil {
			return errhand.BuildDError("error: failed to write profile to config, %s", err).Build()
		}
	}

	err = setGlobalConfigPermissions(dEnv)
	if err != nil {
		return errhand.BuildDError("error: failed to set permissions, %s", err).Build()
	}

	return nil
}

func printProfiles(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	cfg, ok := dEnv.Config.GetConfig(env.GlobalConfig)
	if !ok {
		return errhand.BuildDError("error: failed to get global config").Build()
	}
	encodedProfiles, err := cfg.GetString(GlobalCfgProfileKey)
	if err != nil {
		if err == config.ErrConfigParamNotFound {
			return nil
		}
		return errhand.BuildDError("error: failed to get profiles, %s", err).Build()
	}
	profilesJSON, err := DecodeProfile(encodedProfiles)
	if err != nil {
		return errhand.BuildDError("error: failed to decode profiles, %s", err).Build()
	}

	profileMap := gjson.Parse(profilesJSON)
	if !profileMap.Exists() {
		return nil
	}

	for profileName, profile := range profileMap.Map() {
		var p Profile
		var val []byte = []byte(profile.String())
		err := json.Unmarshal([]byte(val), &p)
		if err != nil {
			return errhand.BuildDError("error: failed to unmarshal profile, %s", err).Build()
		}
		prettyPrintProfile(profileName, p, apr.Contains(cli.VerboseFlag))
	}

	return nil
}

func prettyPrintProfile(profileName string, profile Profile, verbose bool) {
	cli.Println(profileName)
	if verbose {
		if profile.HasPassword {
			cli.Println(fmt.Sprintf("\tuser: %s\n\tpassword: %s\n\thost: %s\n\tport: %s\n\tno-tls: %t\n\tdata-dir: %s\n\tdoltcfg-dir: %s\n\tprivilege-file: %s\n\tbranch-control-file: %s\n\tuse-db: %s\n",
				profile.User, profile.Password, profile.Host, profile.Port, profile.NoTLS, profile.DataDir, profile.DoltCfgDir, profile.PrivilegeFile, profile.BranchControl, profile.UseDB))
		} else {
			cli.Println(fmt.Sprintf("\tuser: %s\n\thost: %s\n\tport: %s\n\tno-tls: %t\n\tdata-dir: %s\n\tdoltcfg-dir: %s\n\tprivilege-file: %s\n\tbranch-control-file: %s\n\tuse-db: %s\n",
				profile.User, profile.Host, profile.Port, profile.NoTLS, profile.DataDir, profile.DoltCfgDir, profile.PrivilegeFile, profile.BranchControl, profile.UseDB))
		}
	}
}

// setGlobalConfigPermissions sets permissions on global config file to 0600 to protect potentially sensitive information (credentials)
func setGlobalConfigPermissions(dEnv *env.DoltEnv) error {
	homeDir, err := env.GetCurrentUserHomeDir()
	if err != nil {
		return errhand.BuildDError("error: failed to get home directory: %s", err).Build()
	}
	path, err := dEnv.FS.Abs(filepath.Join(homeDir, dbfactory.DoltDir, env.GlobalConfigFile))
	if err != nil {
		return errhand.BuildDError("error: failed to get global config path: %s", err).Build()
	}
	err = os.Chmod(path, 0600)
	if err != nil {
		return errhand.BuildDError("error: failed to set permissions on global config: %s", err).Build()
	}

	return nil
}

// writeProfileToGlobalConfig encodes a given profile JSON (represented by a string) to base64 and writes that encoded profile to the global config
func writeProfileToGlobalConfig(profile string, config config.ReadWriteConfig) error {
	profilesData := []byte(profile)
	encodedProfiles := make([]byte, base64.StdEncoding.EncodedLen(len(profilesData)))
	base64.StdEncoding.Encode(encodedProfiles, profilesData)

	err := config.SetStrings(map[string]string{GlobalCfgProfileKey: string(encodedProfiles)})
	if err != nil {
		return err
	}
	return nil
}

// DecodeProfile decodes a given base64 encoded profile string to a string representing a JSON
func DecodeProfile(encodedProfile string) (string, error) {
	decodedProfile := make([]byte, base64.StdEncoding.DecodedLen(len(encodedProfile)))
	n, err := base64.StdEncoding.Decode(decodedProfile, []byte(encodedProfile))
	if err != nil {
		return "", err
	}
	decodedProfile = decodedProfile[:n]
	return string(decodedProfile), nil
}

// decodeProfileAndCheckExists decodes the given profiles and retrieves the profile named profileName. Returns a
// string representing the profile JSON and a bool indicating whether the profile exists
func decodeProfileAndCheckExists(profileName, encodedProfiles string) (string, bool, error) {
	profilesJSON, err := DecodeProfile(encodedProfiles)
	if err != nil {
		return "", false, err
	}
	profileCheck := gjson.Get(profilesJSON, profileName)
	return profilesJSON, profileCheck.Exists(), nil
}

type Profile struct {
	User          string `json:"user"`
	Password      string `json:"password"`
	HasPassword   bool   `json:"has-password"`
	Host          string `json:"host"`
	Port          string `json:"port"`
	NoTLS         bool   `json:"no-tls"`
	DataDir       string `json:"data-dir"`
	DoltCfgDir    string `json:"doltcfg-dir"`
	PrivilegeFile string `json:"privilege-file"`
	BranchControl string `json:"branch-control-file"`
	UseDB         string `json:"use-db"`
}

func (p Profile) String() string {
	b, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func newProfile(apr *argparser.ArgParseResults) Profile {
	return Profile{
		User:          apr.GetValueOrDefault(cli.UserFlag, ""),
		Password:      apr.GetValueOrDefault(cli.PasswordFlag, ""),
		HasPassword:   apr.Contains(cli.PasswordFlag),
		Host:          apr.GetValueOrDefault(cli.HostFlag, ""),
		Port:          apr.GetValueOrDefault(cli.PortFlag, ""),
		NoTLS:         apr.Contains(cli.NoTLSFlag),
		DataDir:       apr.GetValueOrDefault(DataDirFlag, ""),
		DoltCfgDir:    apr.GetValueOrDefault(CfgDirFlag, ""),
		PrivilegeFile: apr.GetValueOrDefault(PrivsFilePathFlag, ""),
		BranchControl: apr.GetValueOrDefault(BranchCtrlPathFlag, ""),
		UseDB:         apr.GetValueOrDefault(UseDbFlag, ""),
	}
}
