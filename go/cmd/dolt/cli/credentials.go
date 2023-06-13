// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package cli

import (
	"fmt"

	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

type UserPassword struct {
	Username string
	Password string
}

// BuildUserPasswordPrompt builds a UserPassword struct from the parsed args. The user is prompted for a password if one
// is not provided. If a username is not provided, the default is "root" (which will not be allowed is a password is
// provided). A new instances of ArgParseResults is returned which does not contain the user or password flags.
func BuildUserPasswordPrompt(parsedArgs *argparser.ArgParseResults) (newParsedArgs *argparser.ArgParseResults, credentials *UserPassword, err error) {
	userId, hasUserId := parsedArgs.GetValue(UserFlag)
	password, hasPassword := parsedArgs.GetValue(PasswordFlag)

	newParsedArgs = parsedArgs.DropValue(UserFlag)
	newParsedArgs = newParsedArgs.DropValue(PasswordFlag)

	if !hasUserId && !hasPassword {
		// Common "out of box" behavior.
		return newParsedArgs, &UserPassword{Username: "root", Password: ""}, nil
	}

	if hasUserId && hasPassword {
		return newParsedArgs, &UserPassword{Username: userId, Password: password}, nil
	}

	if hasUserId && !hasPassword {
		Printf("Password NM4: ")
		fmt.Scan(&password)
		return newParsedArgs, &UserPassword{Username: userId, Password: password}, nil
	}

	return nil, nil, fmt.Errorf("NM4 - gimme a uid if you're going to gimme a pwd!")
}
