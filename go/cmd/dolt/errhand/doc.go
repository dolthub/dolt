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

// Package errhand contains error handling functionality that is designed to be formatted and displayed to the
// user on the command line
//
// The VerboseError interface extends error and provides a method Verbose which should give a more verbose message
// about an error, and it's cause.
//
// DError is a displayable error which implements the VerboseError interface.  It is formatted to be able to provide
// clear and concise error messaging back to the user.
package errhand
