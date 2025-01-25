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
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var DoltPath string
var DelvePath string

const TestUserName = "Bats Tests"
const TestEmailAddress = "bats@email.fake"

const ConnectAttempts = 50
const RetrySleepDuration = 50 * time.Millisecond

const EnvDoltBinPath = "DOLT_BIN_PATH"

func init() {
	path := os.Getenv(EnvDoltBinPath)
	if path == "" {
		path = "dolt"
	}
	path = filepath.Clean(path)
	var err error

	DoltPath, err = exec.LookPath(path)
	if err != nil {
		log.Printf("did not find dolt binary: %v\n", err.Error())
	}

	DelvePath, _ = exec.LookPath("dlv")
}

// DoltUser is an abstraction for a user account that calls `dolt` CLI
// commands. All of our dolt binary invocations are done through DoltUser.
//
// For our purposes, it does the following:
// * owns a tmpdir, to which it sets DOLT_ROOT_PATH when invoking dolt.
// * some initial dolt global config,
//   - user.name
//   - user.email
//   - metrics.disabled = true
//
// * can create repo stores, which will be a tmpdir to store a repo and/or subrepos.
type DoltUser struct {
	tmpdir string
}

var _ DoltCmdable = DoltUser{}
var _ DoltDebuggable = DoltUser{}

func NewDoltUser(tmpdir string) (DoltUser, error) {
	res := DoltUser{tmpdir}
	err := res.DoltExec("config", "--global", "--add", "metrics.disabled", "true")
	if err != nil {
		return DoltUser{}, err
	}
	err = res.DoltExec("config", "--global", "--add", "user.name", TestUserName)
	if err != nil {
		return DoltUser{}, err
	}
	err = res.DoltExec("config", "--global", "--add", "user.email", TestEmailAddress)
	if err != nil {
		return DoltUser{}, err
	}
	return res, nil
}

func (u DoltUser) DoltCmd(args ...string) *exec.Cmd {
	cmd := exec.Command(DoltPath, args...)
	cmd.Dir = u.tmpdir
	cmd.Env = append(os.Environ(), "DOLT_ROOT_PATH="+u.tmpdir)
	ApplyCmdAttributes(cmd)
	return cmd
}

func (u DoltUser) DoltDebug(debuggerPort int, args ...string) *exec.Cmd {
	if DelvePath != "" {
		dlvArgs := []string{
			fmt.Sprintf("--listen=:%d", debuggerPort),
			"--headless",
			"--api-version=2",
			"--accept-multiclient",
			"exec",
			DoltPath,
			"--",
		}
		cmd := exec.Command(DelvePath, append(dlvArgs, args...)...)
		cmd.Dir = u.tmpdir
		cmd.Env = append(os.Environ(), "DOLT_ROOT_PATH="+u.tmpdir)
		ApplyCmdAttributes(cmd)
		return cmd
	} else {
		panic("dlv not found")
	}
}

func (u DoltUser) DoltExec(args ...string) error {
	cmd := u.DoltCmd(args...)
	return cmd.Run()
}

func (u DoltUser) MakeRepoStore() (RepoStore, error) {
	tmpdir, err := os.MkdirTemp(u.tmpdir, "repo-store-")
	if err != nil {
		return RepoStore{}, err
	}
	return RepoStore{u, tmpdir}, nil
}

func (u DoltUser) Cleanup() error {
	return os.RemoveAll(u.tmpdir)
}

type RepoStore struct {
	user DoltUser
	Dir  string
}

var _ DoltCmdable = RepoStore{}
var _ DoltDebuggable = RepoStore{}

func (rs RepoStore) MakeRepo(name string) (Repo, error) {
	path := filepath.Join(rs.Dir, name)
	err := os.Mkdir(path, 0750)
	if err != nil {
		return Repo{}, err
	}
	ret := Repo{rs.user, path}
	err = ret.DoltExec("init")
	if err != nil {
		return Repo{}, err
	}
	return ret, nil
}

func (rs RepoStore) DoltCmd(args ...string) *exec.Cmd {
	cmd := rs.user.DoltCmd(args...)
	cmd.Dir = rs.Dir
	return cmd
}

func (rs RepoStore) DoltDebug(debuggerPort int, args ...string) *exec.Cmd {
	cmd := rs.user.DoltDebug(debuggerPort, args...)
	cmd.Dir = rs.Dir
	return cmd
}

type Repo struct {
	user DoltUser
	Dir  string
}

func (r Repo) DoltCmd(args ...string) *exec.Cmd {
	cmd := r.user.DoltCmd(args...)
	cmd.Dir = r.Dir
	return cmd
}

func (r Repo) DoltExec(args ...string) error {
	cmd := r.DoltCmd(args...)
	err := cmd.Start()
	if err != nil {
		return err
	}
	return cmd.Wait()
}

func (r Repo) CreateRemote(name, url string) error {
	cmd := r.DoltCmd("remote", "add", name, url)
	return cmd.Run()
}

type SqlServer struct {
	Name        string
	Done        chan struct{}
	Cmd         *exec.Cmd
	Port        int
	DebugPort   int
	Output      *bytes.Buffer
	DBName      string
	RecreateCmd func(args ...string) *exec.Cmd
}

type SqlServerOpt func(s *SqlServer)

func WithArgs(args ...string) SqlServerOpt {
	return func(s *SqlServer) {
		s.Cmd.Args = append(s.Cmd.Args, args...)
	}
}

func WithName(name string) SqlServerOpt {
	return func(s *SqlServer) {
		s.Name = name
	}
}

func WithEnvs(envs ...string) SqlServerOpt {
	return func(s *SqlServer) {
		s.Cmd.Env = append(s.Cmd.Env, envs...)
	}
}

func WithPort(port int) SqlServerOpt {
	return func(s *SqlServer) {
		s.Port = port
	}
}

func WithDebugPort(port int) SqlServerOpt {
	return func(s *SqlServer) {
		s.DebugPort = port
	}
}

type DoltCmdable interface {
	DoltCmd(args ...string) *exec.Cmd
}

type DoltDebuggable interface {
	DoltDebug(debuggerPort int, args ...string) *exec.Cmd
}

func StartSqlServer(dc DoltCmdable, opts ...SqlServerOpt) (*SqlServer, error) {
	cmd := dc.DoltCmd("sql-server")
	return runSqlServerCommand(dc, opts, cmd)
}

func DebugSqlServer(dc DoltCmdable, debuggerPort int, opts ...SqlServerOpt) (*SqlServer, error) {
	ddb, ok := dc.(DoltDebuggable)
	if !ok {
		return nil, fmt.Errorf("%T does not implement DoltDebuggable", dc)
	}

	cmd := ddb.DoltDebug(debuggerPort, "sql-server")
	return runSqlServerCommand(dc, append(opts, WithDebugPort(debuggerPort)), cmd)
}

func runSqlServerCommand(dc DoltCmdable, opts []SqlServerOpt, cmd *exec.Cmd) (*SqlServer, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = cmd.Stdout
	output := new(bytes.Buffer)
	var wg sync.WaitGroup
	wg.Add(1)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	server := &SqlServer{
		Done:   done,
		Cmd:    cmd,
		Port:   3306,
		Output: output,
	}
	for _, o := range opts {
		o(server)
	}

	go func() {
		defer wg.Done()
		multiCopyWithNamePrefix(os.Stdout, output, stdout, server.Name)
	}()

	server.RecreateCmd = func(args ...string) *exec.Cmd {
		if server.DebugPort > 0 {
			ddb, ok := dc.(DoltDebuggable)
			if !ok {
				panic(fmt.Sprintf("%T does not implement DoltDebuggable", dc))
			}
			return ddb.DoltDebug(server.DebugPort, args...)
		} else {
			return dc.DoltCmd(args...)
		}
	}

	err = server.Cmd.Start()
	if err != nil {
		return nil, err
	}
	return server, nil
}

func (s *SqlServer) ErrorStop() error {
	<-s.Done
	return s.Cmd.Wait()
}

func multiCopyWithNamePrefix(stdout, captured io.Writer, in io.Reader, name string) {
	reader := bufio.NewReader(in)
	multiOut := io.MultiWriter(stdout, captured)
	wantsPrefix := true
	for {
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			return
		}
		if wantsPrefix && name != "" {
			stdout.Write([]byte("["))
			stdout.Write([]byte(name))
			stdout.Write([]byte("] "))
		}
		multiOut.Write(line)
		if isPrefix {
			wantsPrefix = false
		} else {
			multiOut.Write([]byte("\n"))
			wantsPrefix = true
		}
	}
}

func (s *SqlServer) Restart(newargs *[]string, newenvs *[]string) error {
	err := s.GracefulStop()
	if err != nil {
		return err
	}
	args := s.Cmd.Args[1:]
	if newargs != nil {
		args = append([]string{"sql-server"}, (*newargs)...)
	}
	s.Cmd = s.RecreateCmd(args...)
	if newenvs != nil {
		s.Cmd.Env = append(s.Cmd.Env, (*newenvs)...)
	}
	stdout, err := s.Cmd.StdoutPipe()
	if err != nil {
		return err
	}
	s.Cmd.Stderr = s.Cmd.Stdout
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		multiCopyWithNamePrefix(os.Stdout, s.Output, stdout, s.Name)
	}()
	s.Done = make(chan struct{})
	go func() {
		wg.Wait()
		close(s.Done)
	}()
	return s.Cmd.Start()
}

func (s *SqlServer) DB(c Connection) (*sql.DB, error) {
	var pass string
	pass, err := c.Password()
	if err != nil {
		return nil, err
	}
	return ConnectDB(c.User, pass, s.DBName, "127.0.0.1", s.Port, c.DriverParams)
}

func ConnectDB(user, password, name, host string, port int, driverParams map[string]string) (*sql.DB, error) {
	params := make(url.Values)
	params.Set("allowAllFiles", "true")
	params.Set("tls", "preferred")
	for k, v := range driverParams {
		params.Set(k, v)
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", user, password, host, port, name, params.Encode())

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	for i := 0; i < ConnectAttempts; i++ {
		err = db.Ping()
		if err == nil {
			return db, nil
		}
		time.Sleep(RetrySleepDuration)
	}
	if err != nil {
		return nil, err
	}
	return db, nil
}
