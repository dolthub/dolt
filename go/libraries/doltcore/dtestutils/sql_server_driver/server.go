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

package sql_server_driver

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/creasty/defaults"
	"gopkg.in/yaml.v3"
)

// |Connection| represents a single connection to a sql-server instance defined
// in the test. The connection will be established and every |Query| in
// |Queries| will be run against it. At the end, the connection will be torn down.
// If |RestartServer| is non-nil, the server which the connection targets will
// be restarted after the connection is terminated.
type Connection struct {
	On            string       `yaml:"on"`
	Queries       []Query      `yaml:"queries"`
	RestartServer *RestartArgs `yaml:"restart_server"`

	// Rarely needed, allows the entire connection assertion to be retried
	// on an assertion failure. Use this is only for idempotent connection
	// interactions and only if the sql-server is prone to tear down the
	// connection based on things that are happening, such as cluster role
	// transitions.
	RetryAttempts int `yaml:"retry_attempts"`

	// The user to connect as.
	User string `default:"root" yaml:"user"`
	// The password to connect with.
	Pass     string `yaml:"password"`
	PassFile string `yaml:"password_file"`
	// Any driver params to pass in the DSN.
	DriverParams map[string]string `yaml:"driver_params"`
}

func (c *Connection) UnmarshalYAML(unmarshal func(interface{}) error) error {
	defaults.Set(c)
	type plain Connection
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	return nil
}

func (c Connection) Password() (string, error) {
	if c.PassFile != "" {
		passFile := c.PassFile
		if v := os.Getenv("TESTGENDIR"); v != "" {
			passFile = strings.ReplaceAll(passFile, "$TESTGENDIR", v)
		}
		bs, err := os.ReadFile(passFile)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(bs)), nil
	}
	return c.Pass, nil
}

// |RestartArgs| are possible arguments, to change the arguments which are
// provided to the sql-server process when it is restarted. This is used, for
// example, to change server config on a restart.
type RestartArgs struct {
	Args *[]string `yaml:"args"`
	Envs *[]string `yaml:"envs"`
}

// |TestRepo| represents an init'd dolt repository that is available to a
// server instance. It can be created with some files and with remotes defined.
// |Name| can include path components separated by `/`, which will create the
// repository in a subdirectory.
type TestRepo struct {
	Name        string       `yaml:"name"`
	WithFiles   []WithFile   `yaml:"with_files"`
	WithRemotes []WithRemote `yaml:"with_remotes"`

	// Only valid on Test.Repos, not in Test.MultiRepos.Repos. If set, a
	// sql-server process will be run against this TestRepo. It will be
	// available as TestRepo.Name.
	Server         *Server         `yaml:"server"`
	ExternalServer *ExternalServer `yaml:"external-server"`
}

// |MultiRepo| is a subdirectory where many |TestRepo|s can be defined. You can
// start a sql-server on a |MultiRepo|, in which case there will be no default
// database to connect to.
type MultiRepo struct {
	Name      string     `yaml:"name"`
	Repos     []TestRepo `yaml:"repos"`
	WithFiles []WithFile `yaml:"with_files"`

	// If set, a sql-server process will be run against this TestRepo. It
	// will be available as MultiRepo.Name.
	Server *Server `yaml:"server"`
}

// |WithRemote| defines remotes which should be defined on the repository
// before the sql-server is started.
type WithRemote struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"`
}

// |WithFile| defines a file and its contents to be created in a |Repo| or
// |MultiRepo| before the servers are started.
type WithFile struct {
	Name string `yaml:"name"`

	// The contents of the file, provided inline in the YAML.
	Contents string `yaml:"contents"`

	// A source file path to copy to |Name|. Mutually exclusive with
	// Contents.
	SourcePath string `yaml:"source_path"`

	// If this is non-nil, the template will be applied to the
	// contents of the file as they are written through |WriteAtDir|.
	Template func(string) string
}

func (f WithFile) WriteAtDir(dir string) error {
	path := filepath.Join(dir, f.Name)
	d := filepath.Dir(path)
	err := os.MkdirAll(d, 0750)
	if err != nil {
		return err
	}
	if f.SourcePath != "" {
		sourcePath := f.SourcePath
		if v := os.Getenv("TESTGENDIR"); v != "" {
			sourcePath = strings.ReplaceAll(sourcePath, "$TESTGENDIR", v)
		}
		source, err := os.Open(sourcePath)
		if err != nil {
			return err
		}
		defer source.Close()
		dest, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0550)
		if err != nil {
			return err
		}
		contents, err := io.ReadAll(source)
		if err != nil {
			return err
		}
		if f.Template != nil {
			str := f.Template(string(contents))
			contents = []byte(str)
		}
		_, err = dest.Write(contents)
		return err
	} else {
		contents := f.Contents
		if f.Template != nil {
			contents = f.Template(contents)
		}
		return os.WriteFile(path, []byte(contents), 0550)
	}
}

// |Server| defines a sql-server process to start. |Name| must match the
// top-level |Name| of a |TestRepo| or |MultiRepo|.
type Server struct {
	Name string   `yaml:"name"`
	Args []string `yaml:"args"`
	Envs []string `yaml:"envs"`

	// The |Port| which the server will be running on. For now, it is up to
	// the |Args| to make sure this is true. Defaults to 3306.
	Port int `yaml:"port"`

	// This can be used with templating of dynamic ports to
	// specify the SQL listener port which will have been filled
	// in by a call to `{{get_port "server_name"}}` within the
	// config or args of the server.
	//
	// A |Port| != 0 with a |DynamicPort| != "" is an error.
	DynamicPort string `yaml:"dynamic_port"`

	// DebugPort if set to a non-zero value will cause this server to be started with |dlv| listening for a debugger
	// connection on the port given.
	DebugPort int `yaml:"debug_port"`

	// Assertions to be run against the log output of the server process
	// after the server process successfully terminates.
	LogMatches []string `yaml:"log_matches"`

	// Assertions to be run against the log output of the server process
	// after the server process exits with an error. If |ErrorMatches| is
	// defined, then the server process must exit with a non-0 exit code
	// after it is launched. This will be asserted before any |Connections|
	// interactions are performed.
	ErrorMatches []string `yaml:"error_matches"`
}

type ExternalServer struct {
	Name     string `yaml:"name"`
	Host     string `yaml:"host"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	// The |Port| which the server will be running on. For now, it is up to
	// the |Args| to make sure this is true. Defaults to 3306.
	Port int `yaml:"port"`
}

// The primary interaction of a |Connection|. Either |Query| or |Exec| should
// be set, not both.
type Query struct {
	// Run a query against the connection.
	Query string `yaml:"query"`

	// Run a command against the connection.
	Exec string `yaml:"exec"`

	// Args to be passed as query parameters to either Query or Exec.
	Args []string `yaml:"args"`

	// This can only be non-empty for a |Query|. Asserts the results of the
	// |Query|.
	Result QueryResult `yaml:"result"`

	// If this is non-empty, asserts the |Query| or the |Exec|
	// generates an error that matches this string.
	ErrorMatch string `yaml:"error_match"`

	// If this is non-zero, it represents the number of times to try the
	// |Query| or the |Exec| and to check its assertions before we fail the
	// test as a result of failed assertions. When interacting with queries
	// that introspect things like replication state, this can be used to
	// wait for quiescence in an inherently racey process. Interactions
	// will be delayed slightly between each failure.
	RetryAttempts int `yaml:"retry_attempts"`
}

// |QueryResult| specifies assertions on the results of a |Query|. Columns must
// be specified for a |Query| and the query results must fully match. If Rows
// are omitted, anything is allowed as long as all rows are read successfully.
// All assertions here are string equality.
type QueryResult struct {
	Columns []string   `yaml:"columns"`
	Rows    ResultRows `yaml:"rows"`
}

type ResultRows struct {
	Or *[][][]string
}

func (r *ResultRows) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.SequenceNode {
		res := make([][][]string, 1)
		r.Or = &res
		return value.Decode(&(*r.Or)[0])
	}
	var or struct {
		Or *[][][]string `yaml:"or"`
	}
	err := value.Decode(&or)
	if err != nil {
		return err
	}
	r.Or = or.Or
	return nil
}
