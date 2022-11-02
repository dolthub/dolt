// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package spec

import (
	"context"
	"fmt"
	"time"

	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/store/datas"
)

const CommitMetaDateFormat = time.RFC3339

var (
	commitMetaDate    string
	commitMetaMessage string
)

// RegisterCommitMetaFlags registers command line flags used for creating commit meta structs.
func RegisterCommitMetaFlags(flags *flag.FlagSet) {
	flags.StringVar(&commitMetaDate, "date", "", "date for a new commit. '<date>' must be iso8601-formatted. If '<date>' is empty, it defaults to the current date.")
	flags.StringVar(&commitMetaMessage, "message", "", "message for a new commit.")
}

// Return the CommitMeta for an invocation of `noms` with CLI flags.
func CommitMetaFromFlags(ctx context.Context) (*datas.CommitMeta, error) {
	date := commitMetaDate
	t := time.Now().UTC()
	var usertime time.Time

	if date == "" {
		usertime = t
	} else {
		var err error
		usertime, err = time.Parse(CommitMetaDateFormat, date)
		if err != nil {
			return nil, fmt.Errorf("unable to parse date: %s, error: %s", date, err)
		}
	}

	return &datas.CommitMeta{
		UserTimestamp: usertime.UnixMilli(),
		Timestamp:     uint64(t.UnixMilli()),
		Description:   commitMetaMessage,
	}, nil
}
