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

package mvdata

import (
	"context"
	"errors"
	"os"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

type StdIODataLocation struct {
	Format DataFormat
}

func (dl StdIODataLocation) String() string {
	return StdIO.ReadableStr()
}

// Exists returns true if the DataLocation already exists
func (dl StdIODataLocation) Exists(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS) (bool, error) {
	return true, nil
}

// NewReader creates a TableReadCloser for the DataLocation
func (dl StdIODataLocation) NewReader(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS, schPath string, opts interface{}) (rdCl table.TableReadCloser, sorted bool, err error) {
	switch dl.Format {
	case CsvFile:
		delim := ","

		if opts != nil {
			csvOpts, _ := opts.(CsvOptions)

			if len(csvOpts.Delim) != 0 {
				delim = csvOpts.Delim
			}
		}

		rd, err := csv.NewCSVReader(root.VRW().Format(), os.Stdin, csv.NewCSVInfo().SetDelim(delim))

		return rd, false, err

	case PsvFile:
		rd, err := csv.NewCSVReader(root.VRW().Format(), os.Stdin, csv.NewCSVInfo().SetDelim("|"))
		return rd, false, err
	}

	return nil, false, errors.New(string(dl.Format) + "is an unsupported format to read from stdin")
}

// NewCreatingWriter will create a TableWriteCloser for a DataLocation that will create a new table, or overwrite an existing table.
func (dl StdIODataLocation) NewCreatingWriter(ctx context.Context, mvOpts *MoveOptions, root *doltdb.RootValue, fs filesys.WritableFS, sortedInput bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
	switch dl.Format {
	case CsvFile:
		return csv.NewCSVWriter(iohelp.NopWrCloser(cli.CliOut), outSch, csv.NewCSVInfo())

	case PsvFile:
		return csv.NewCSVWriter(iohelp.NopWrCloser(cli.CliOut), outSch, csv.NewCSVInfo().SetDelim("|"))
	}

	return nil, errors.New(string(dl.Format) + "is an unsupported format to write to stdout")
}

// NewUpdatingWriter will create a TableWriteCloser for a DataLocation that will update and append rows based on their primary key.
func (dl StdIODataLocation) NewUpdatingWriter(ctx context.Context, mvOpts *MoveOptions, root *doltdb.RootValue, fs filesys.WritableFS, srcIsSorted bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
	panic("Updating is not supported for stdout")
}
