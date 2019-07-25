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

// Package cli provides utilities for the dolt command line.
//
// cli provides:
//
//   * the interface for creating and managing hierarchical dolt commands. These typically have command lines that look like:
//    app command [<options>]
//    app command subcommand [<options>]
//    app command subcommand1 subcommand2 [<options>]
//    etc.
//
//   * Command help and usage printing
//
//   * The interface for writing output to the user
//
//   * Argument parsing utility methods
package cli
