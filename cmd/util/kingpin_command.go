// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package util

import kingpin "gopkg.in/alecthomas/kingpin.v2"

type KingpinHandler func(input string) (exitCode int)
type KingpinCommand func(*kingpin.Application) (*kingpin.CmdClause, KingpinHandler)
