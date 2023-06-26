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

package cli

import (
	"fmt"
	"os"

	"golang.org/x/term"

	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

type UserPassword struct {
	Username  string
	Password  string
	Specified bool // If true, the user and password were provided by the user.
}

const DOLT_ENV_PWD = "DOLT_CLI_PASSWORD"
const DOLT_SILENCE_USER_REQ_FOR_TESTING = "DOLT_SILENCE_USER_REQ_FOR_TESTING"

// BuildUserPasswordPrompt builds a UserPassword struct from the parsed args. The user is prompted for a password if one
// is not provided. If a username is not provided, the default is "root" (which will not be allowed is a password is
// provided). A new instances of ArgParseResults is returned which does not contain the user or password flags.
func BuildUserPasswordPrompt(parsedArgs *argparser.ArgParseResults) (newParsedArgs *argparser.ArgParseResults, credentials *UserPassword, err error) {
	userId, hasUserId := parsedArgs.GetValue(UserFlag)
	password, hasPassword := parsedArgs.GetValue(PasswordFlag)

	if !hasPassword {
		envPassword, hasEnvPassword := os.LookupEnv(DOLT_ENV_PWD)
		if hasEnvPassword {
			password = envPassword
			hasPassword = true
		}
	}

	newParsedArgs = parsedArgs.DropValue(UserFlag)
	newParsedArgs = newParsedArgs.DropValue(PasswordFlag)

	if !hasUserId && !hasPassword {
		// Common "out of box" behavior.
		return newParsedArgs, &UserPassword{Username: "root", Password: "", Specified: false}, nil
	}

	if hasUserId && hasPassword {
		return newParsedArgs, &UserPassword{Username: userId, Password: password, Specified: true}, nil
	}

	if hasUserId && !hasPassword {
		password = ""
		val, hasVal := os.LookupEnv(DOLT_ENV_PWD)
		if hasVal {
			password = val
		} else {
			Printf("Enter password: ")

			// Disable terminal echo
			oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
			if err != nil {
				return nil, nil, err
			}
			// ensure we restore terminal to original state
			defer term.Restore(int(os.Stdin.Fd()), oldState)

			// Read the password
			_, err = fmt.Scan(&password)
			if err != nil {
				return nil, nil, err
			}
			Println()
		}
		return newParsedArgs, &UserPassword{Username: userId, Password: password, Specified: true}, nil
	}

	testOverride, hasTestOverride := os.LookupEnv(DOLT_SILENCE_USER_REQ_FOR_TESTING)
	if hasTestOverride && testOverride == "Y" {
		// Used for BATS testing only. Typical usage will not hit this path, but we have many legacy tests which
		// do not provide a user, and the DOLT_ENV_PWD is set to avoid the prompt.
		return newParsedArgs, &UserPassword{Specified: false}, nil
	}

	return nil, nil, fmt.Errorf("When a password is provided, a user must also be provided. Use the --user flag to provide a username")
}
