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

package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/profile"
)

var (
	count    = flag.Int("c", 10, "Number of iterations to run")
	dataSize = flag.Uint64("data", 4096, "MiB of data to test with")
	mtMiB    = flag.Uint64("mem", 64, "Size in MiB of memTable")
	useNBS   = flag.String("useNBS", "", "Existing Database to use for not-WriteNovel benchmarks")
	toNBS    = flag.String("toNBS", "", "Write to an NBS store in the given directory")
	useAWS   = flag.String("useAWS", "", "Name of existing Database to use for not-WriteNovel benchmarks")
	toAWS    = flag.String("toAWS", "", "Write to an NBS store in AWS")
	toFile   = flag.String("toFile", "", "Write to a file in the given directory")
)

const s3Bucket = "attic-nbs"
const dynamoTable = "attic-nbs"

type panickingBencher struct {
	n int
}

func (pb panickingBencher) Errorf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

func (pb panickingBencher) N() int {
	return pb.n
}

func (pb panickingBencher) ResetTimer() {}
func (pb panickingBencher) StartTimer() {}
func (pb panickingBencher) StopTimer()  {}

func main() {
	profile.RegisterProfileFlags(flag.CommandLine)
	flag.Parse(true)

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	pb := panickingBencher{*count}

	src, err := getInput((*dataSize) * humanize.MiByte)
	d.PanicIfError(err)
	defer func() {
		err := src.Close()
		d.PanicIfError(err)
	}()

	bufSize := (*mtMiB) * humanize.MiByte

	open := newNullBlockStore
	wrote := false
	var writeDB func()
	var refresh func() (chunks.ChunkStore, error)
	if *toNBS != "" || *toFile != "" || *toAWS != "" {
		var reset func()
		if *toNBS != "" {
			dir := makeTempDir(*toNBS, pb)
			defer func() {
				err := file.RemoveAll(dir)
				d.PanicIfError(err)
			}()
			open = func() (chunks.ChunkStore, error) {
				return nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), dir, bufSize, nbs.NewUnlimitedMemQuotaProvider())
			}
			reset = func() {
				err := file.RemoveAll(dir)
				d.PanicIfError(err)
				err = os.MkdirAll(dir, 0777)
				d.PanicIfError(err)
			}

		} else if *toFile != "" {
			dir := makeTempDir(*toFile, pb)
			defer func() {
				err := file.RemoveAll(dir)
				d.PanicIfError(err)
			}()
			open = func() (chunks.ChunkStore, error) {
				f, err := os.CreateTemp(dir, "")
				d.Chk.NoError(err)
				return newFileBlockStore(f)
			}
			reset = func() {
				err := file.RemoveAll(dir)
				d.PanicIfError(err)
				err = os.MkdirAll(dir, 0777)
				d.PanicIfError(err)
			}

		} else if *toAWS != "" {
			cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-west-2"))
			d.PanicIfError(err)
			open = func() (chunks.ChunkStore, error) {
				return nbs.NewAWSStore(context.Background(), types.Format_Default.VersionString(), dynamoTable, *toAWS, s3Bucket, s3.NewFromConfig(cfg), dynamodb.NewFromConfig(cfg), bufSize, nbs.NewUnlimitedMemQuotaProvider())
			}
			reset = func() {
				ddb := dynamodb.NewFromConfig(cfg)
				_, err := ddb.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
					TableName: aws.String(dynamoTable),
					Key: map[string]ddbtypes.AttributeValue{
						"db": &ddbtypes.AttributeValueMemberS{
							Value: *toAWS,
						},
					},
				})
				d.PanicIfError(err)
			}
		}

		writeDB = func() { wrote = ensureNovelWrite(wrote, open, src, pb) }
		refresh = func() (chunks.ChunkStore, error) {
			reset()
			return open()
		}
	} else {
		if *useNBS != "" {
			open = func() (chunks.ChunkStore, error) {
				return nbs.NewLocalStore(context.Background(), types.Format_Default.VersionString(), *useNBS, bufSize, nbs.NewUnlimitedMemQuotaProvider())
			}
		} else if *useAWS != "" {
			cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-west-2"))
			d.PanicIfError(err)
			open = func() (chunks.ChunkStore, error) {
				return nbs.NewAWSStore(context.Background(), types.Format_Default.VersionString(), dynamoTable, *useAWS, s3Bucket, s3.NewFromConfig(cfg), dynamodb.NewFromConfig(cfg), bufSize, nbs.NewUnlimitedMemQuotaProvider())
			}
		}
		writeDB = func() {}
		refresh = func() (chunks.ChunkStore, error) { panic("WriteNovel unsupported with --useLDB and --useNBS") }
	}

	benchmarks := []struct {
		name  string
		setup func()
		run   func()
	}{
		{"WriteNovel", func() {}, func() { wrote = benchmarkNovelWrite(refresh, src, pb) }},
		{"WriteDuplicate", writeDB, func() { benchmarkNoRefreshWrite(open, src, pb) }},
		{"ReadSequential", writeDB, func() {
			benchmarkRead(open, src.GetHashes(), src, pb)
		}},
		{"ReadHashOrder", writeDB, func() {
			ordered := src.GetHashes()
			sort.Sort(ordered)
			benchmarkRead(open, ordered, src, pb)
		}},
		{"ReadManySequential", writeDB, func() { benchmarkReadMany(open, src.GetHashes(), src, 1<<8, 6, pb) }},
		{"ReadManyHashOrder", writeDB, func() {
			ordered := src.GetHashes()
			sort.Sort(ordered)
			benchmarkReadMany(open, ordered, src, 1<<8, 6, pb)
		}},
	}
	w := 0
	for _, bm := range benchmarks {
		if len(bm.name) > w {
			w = len(bm.name)
		}
	}
	defer profile.MaybeStartProfile().Stop()
	for _, bm := range benchmarks {
		if matched, _ := regexp.MatchString(flag.Arg(0), bm.name); matched {
			trialName := fmt.Sprintf("%dMiB/%sbuffer/%-[3]*s", *dataSize, humanize.IBytes(bufSize), w, bm.name)
			bm.setup()
			dur := time.Duration(0)
			var trials []time.Duration
			for i := 0; i < *count; i++ {
				d.Chk.NoError(dropCache())
				src.PrimeFilesystemCache()

				t := time.Now()
				bm.run()
				trialDur := time.Since(t)
				trials = append(trials, trialDur)
				dur += trialDur
			}
			fmt.Printf("%s\t%d\t%ss/iter %v\n", trialName, *count, humanize.FormatFloat("", (dur/time.Duration(*count)).Seconds()), formatTrials(trials))
		}
	}
}

func makeTempDir(tmpdir string, t assert.TestingT) (dir string) {
	dir, err := os.MkdirTemp(tmpdir, "")
	assert.NoError(t, err)
	return
}

func formatTrials(trials []time.Duration) (formatted []string) {
	for _, trial := range trials {
		formatted = append(formatted, humanize.FormatFloat("", trial.Seconds()))
	}
	return
}
