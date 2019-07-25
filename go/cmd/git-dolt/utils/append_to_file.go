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

package utils

import (
	"fmt"
	"os"
)

// AppendToFile appends the given string to the given file in the current
// directory, creating it if it does not exist.
func AppendToFile(f string, s string) error {
	giFile, err := os.OpenFile(f, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening %s: %v", f, err)
	}
	defer giFile.Close()

	if _, err = giFile.WriteString(fmt.Sprintf("%s\n", s)); err != nil {
		return fmt.Errorf("error writing to %s at %v", f, err)
	}

	return nil
}
