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

package main

import (
	"context"
	"os"
	"testing"

	"database/sql"
	"database/sql/driver"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

type TestDef struct {
	Tests []Test `yaml:"tests"`
}

type Test struct {
	Name    string       `yaml:"name"`
	Repos   []TestRepo   `yaml:"repos"`
	Servers []Server     `yaml:"servers"`
	Conns   []Connection `yaml:"connections"`
}

type Connection struct {
	On            string  `yaml:"on"`
	Queries       []Query `yaml:"queries"`
	RestartServer bool    `yaml:"restart_server"`
}

type TestRepo struct {
	Name      string     `yaml:"name"`
	WithFiles []WithFile `yaml:"with_files"`
}

type WithFile struct {
	Name     string `yaml:"name"`
	Contents string `yaml:"contents"`
}

type Server struct {
	Name       string   `yaml:"name"`
	Args       []string `yaml:"args"`
	LogMatches []string `yaml:"log_matches"`
}

type Query struct {
	Query      string      `yaml:"query"`
	Exec       string      `yaml:"exec"`
	Args       []string    `yaml:"args"`
	Result     QueryResult `yaml:"result"`
	ErrorMatch string      `yaml:"error_match"`
}

type QueryResult struct {
	Columns []string   `yaml:"columns"`
	Rows    [][]string `yaml:"rows"`
}

func ParseTestsFile(path string) (TestDef, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return TestDef{}, err
	}
	var res TestDef
	err = yaml.Unmarshal(contents, &res)
	return res, err
}

func RunTestsFile(t *testing.T, path string) {
	def, err := ParseTestsFile(path)
	require.NoError(t, err)
	for _, test := range def.Tests {
		t.Run(test.Name, func(t *testing.T) {
			u, err := NewDoltUser()
			require.NoError(t, err)
			rs, err := u.MakeRepoStore()
			require.NoError(t, err)
			repos := make(map[string]Repo)
			for _, r := range test.Repos {
				var repo Repo
				repo, err = rs.MakeRepo(r.Name)
				require.NoError(t, err)
				repos[r.Name] = repo
				for _, f := range r.WithFiles {
					require.NoError(t, repo.WriteFile(f.Name, f.Contents))
				}
			}
			servers := make(map[string]*SqlServer)
			for _, sl := range test.Servers {
				s := sl
				var server *SqlServer
				server, err = repos[s.Name].StartSqlServer(s.Args...)
				require.NoError(t, err)
				servers[s.Name] = server
				defer func() {
					err := server.GracefulStop()
					require.NoError(t, err)
					output := string(server.Output.Bytes())
					for _, a := range s.LogMatches {
						require.Regexp(t, a, output)
					}
				}()
			}
			dbs := make(map[string]*sql.DB)
			for nl, s := range servers {
				n := nl
				db, err := s.DB(n)
				require.NoError(t, err)
				dbs[n] = db
				defer func() {
					dbs[n].Close()
				}()
			}
			for i, c := range test.Conns {
				db := dbs[c.On]
				require.NotNilf(t, db, "error in test spec: could not find database %s for connection %d", c.On, i)
				conn, err := db.Conn(context.Background())
				require.NoError(t, err)
				func() {
					// Do not return this connection to the connection pool.
					defer conn.Raw(func(any) error {
						return driver.ErrBadConn
					})
					for _, q := range c.Queries {
						args := make([]any, len(q.Args))
						for i := range q.Args {
							args[i] = q.Args[i]
						}
						if q.Query != "" {
							rows, err := conn.QueryContext(context.Background(), q.Query, args...)
							if q.ErrorMatch != "" {
								require.Error(t, err)
								require.Regexp(t, q.ErrorMatch, err.Error())
							}
							require.NoError(t, err)
							defer rows.Close()
							cols, err := rows.Columns()
							require.NoError(t, err)
							require.Equal(t, q.Result.Columns, cols)
							for _, r := range q.Result.Rows {
								require.True(t, rows.Next())
								scanned := make([]any, len(r))
								for j := range scanned {
									scanned[j] = new(string)
								}
								require.NoError(t, rows.Scan(scanned...))
								printed := make([]string, len(r))
								for j := range scanned {
									s := scanned[j].(*string)
									if s == nil {
										printed[j] = "NULL"
									} else {
										printed[j] = *s
									}
								}
								require.Equal(t, r, printed)
							}
							require.False(t, rows.Next())
							require.NoError(t, rows.Err())
						} else {
							_, err := conn.ExecContext(context.Background(), q.Exec, args...)
							if q.ErrorMatch == "" {
								require.NoError(t, err)
							} else {
								require.Error(t, err)
								require.Regexp(t, q.ErrorMatch, err.Error())
							}
						}
					}
				}()
				if c.RestartServer {
					olddb := dbs[c.On]
					olddb.Close()
					require.NotNilf(t, olddb, "error in test spec: could not find database %s for connection %d", c.On, i)
					s := servers[c.On]
					require.NotNilf(t, s, "error in test spec: could not find server %s for connection %d", c.On, i)
					err := s.Restart()
					require.NoError(t, err)
					db, err := s.DB(c.On)
					require.NoError(t, err)
					dbs[c.On] = db
				}
			}
		})
	}
}
