// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package util

import (
	"context"
	"github.com/attic-labs/kingpin"
)

type KingpinHandler func(input string) (exitCode int)
type KingpinCommand func(context.Context, *kingpin.Application) (*kingpin.CmdClause, KingpinHandler)
