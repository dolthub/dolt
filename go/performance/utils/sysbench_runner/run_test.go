// Copyright 2022 Dolthub, Inc.
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

package sysbench_runner

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestRunner(t *testing.T) {
	t.Skip()
	dir, err := ioutil.TempDir("", "prefix")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	err = os.Chdir(dir)
	if err != nil {
		log.Fatal(err)
	}

	conf := &Config{
		Tests: selectTests("oltp_read_write", "oltp_update_index", "oltp_delete"),
		//Tests: selectTests("oltp_read_write", "oltp_update_index", "oltp_update_non_index", "oltp_insert", "bulk_insert", "oltp_write_only", "oltp_delete"),
		Servers: []*ServerConfig{
			{
				Id:            "test",
				Server:        Dolt,
				Version:       "0.39.2",
				ResultsFormat: CsvFormat,
				ServerExec:    "/Users/max-hoffman/go/bin/dolt",
			},
		},
		ScriptDir: "/Users/max-hoffman/Documents/dolthub/sysbench-lua-scripts",
		TestOptions: []string{
			"--rand-seed=1",
			"--table-size=10000",
			"--rand-type=uniform",
			"--time=120",
			"--percentile=50",
		},
		InitBigRepo: true,
	}

	err = Run(conf)
	if err != nil {
		log.Fatal(err)
	}
}

func selectTests(names ...string) []*ConfigTest {
	tests := make([]*ConfigTest, len(names))
	for i := range names {
		tests[i] = &ConfigTest{Name: names[i], FromScript: false}
	}
	return tests
}
