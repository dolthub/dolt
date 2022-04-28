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
				ServerExec:    "//Users/max-hoffman/go/bin/dolt",
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
