// Copyright 2025 Dolthub, Inc.
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

package sqlserver

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"

	pkgmcp "github.com/dolthub/dolt-mcp/mcp/pkg"
	mcpdb "github.com/dolthub/dolt-mcp/mcp/pkg/db"
	"github.com/dolthub/dolt-mcp/mcp/pkg/toolsets"
	"github.com/dolthub/dolt/go/libraries/utils/svcs"
)

// MCPConfig encapsulates MCP-specific configuration for the sql-server
type MCPConfig struct {
	Port     *int
	User     *string
	Password *string
	Database *string
}

// logrusZapCore implements a zapcore.Core that forwards entries to a logrus.Logger
type logrusZapCore struct {
	l      *logrus.Logger
	prefix string
}

func newZapFromLogrusWithPrefix(l *logrus.Logger, prefix string) *zap.Logger {
	core := &logrusZapCore{l: l, prefix: prefix}
	return zap.New(core, zap.AddCallerSkip(1))
}

func (c *logrusZapCore) With(fields []zapcore.Field) zapcore.Core {
	// zap will call Write with fields; we don't need to carry state here
	return c
}

func (c *logrusZapCore) Enabled(lvl zapcore.Level) bool {
	// Respect logrus current level: allow messages at or above the configured level.
	// Note: logrus levels are ordered with lower numeric values being more severe.
	// So we log when entryLevel <= loggerLevel (e.g., Info(4) should pass when logger is Debug(5)).
	return zapToLogrusLevel(lvl) <= c.l.GetLevel()
}

func (c *logrusZapCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

func (c *logrusZapCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fields {
		f.AddTo(enc)
	}
	// Build fields map
	lf := logrus.Fields(enc.Fields)
	msg := c.prefix + ent.Message
	c.l.WithFields(lf).Log(zapToLogrusLevel(ent.Level), msg)
	return nil
}

func (c *logrusZapCore) Sync() error { return nil }

func zapToLogrusLevel(l zapcore.Level) logrus.Level {
	switch l {
	case zapcore.DebugLevel:
		return logrus.DebugLevel
	case zapcore.InfoLevel:
		return logrus.InfoLevel
	case zapcore.WarnLevel:
		return logrus.WarnLevel
	case zapcore.ErrorLevel:
		return logrus.ErrorLevel
	case zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
		return logrus.FatalLevel
	default:
		return logrus.InfoLevel
	}
}

// mcpInit validates and reserves the MCP port
func mcpInit(cfg *Config, state *svcs.ServiceState) func(context.Context) error {
	return func(context.Context) error {
		addr := net.JoinHostPort("0.0.0.0", strconv.Itoa(*cfg.MCP.Port))
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		_ = l.Close()
		state.Swap(svcs.ServiceState_Init)
		return nil
	}
}

// mcpRun starts the MCP HTTP server and wires lifecycle to errgroup
func mcpRun(cfg *Config, lgr *logrus.Logger, state *svcs.ServiceState, cancelPtr *context.CancelFunc, groupPtr **errgroup.Group) func(context.Context) {
	return func(ctx context.Context) {
		if !state.CompareAndSwap(svcs.ServiceState_Init, svcs.ServiceState_Run) {
			return
		}

		// Logger for MCP (prefix and level inherited from logrus)
		logger := newZapFromLogrusWithPrefix(lgr, "[dolt-mcp] ")

		if cfg.MCP.User == nil || *cfg.MCP.User == "" {
			lgr.WithField("component", "dolt-mcp").Error("MCP user not defined")
			return
		}

		// Build DB config
		password := ""
		if cfg.MCP.Password != nil {
			password = *cfg.MCP.Password
		}
		dbName := ""
		if cfg.MCP.Database != nil {
			dbName = *cfg.MCP.Database
		}
		dbConf := mcpdb.Config{
			Host:         "127.0.0.1",
			Port:         cfg.ServerConfig.Port(),
			User:         *cfg.MCP.User,
			Password:     password,
			DatabaseName: dbName,
		}

		// Announce MCP startup using a concise config string similar to the main server
		// HP is the MCP HTTP bind host:port; SQL_HP is the SQL server host:port used by MCP.
		mcpBindHost := "0.0.0.0"
		confInfo := fmt.Sprintf("HP=\"%s:%d\"|SQL_HP=\"%s:%d\"|U=\"%s\"|DB=\"%s\"", mcpBindHost, *cfg.MCP.Port, dbConf.Host, dbConf.Port, *cfg.MCP.User, dbName)
		lgr.Infof("Starting Dolt MCP server with Config %s", confInfo)

		srv, err := pkgmcp.NewMCPHTTPServer(
			logger,
			dbConf,
			*cfg.MCP.Port,
			nil, // jwkClaimsMap
			"",  // jwkUrl
			nil, // tlsConfig
			toolsets.WithToolSet(&toolsets.PrimitiveToolSetV1{}),
		)
		if err != nil {
			lgr.WithField("component", "dolt-mcp").Errorf("failed to start Dolt MCP HTTP server: %v", err)
			return
		}

		runCtx, cancel := context.WithCancel(ctx)
		*cancelPtr = cancel
		g, gctx := errgroup.WithContext(runCtx)
		g.Go(func() error {
			// Log readiness from the parent logger so it appears in Dolt output
			lgr.Infof("Dolt MCP server ready. Accepting connections.")
			srv.ListenAndServe(gctx)
			return nil
		})
		*groupPtr = g
	}
}

// mcpStop gracefully stops the MCP server by cancelling context and waiting for the errgroup
func mcpStop(cancel context.CancelFunc, group *errgroup.Group, state *svcs.ServiceState) func() error {
	return func() error {
		if cancel != nil {
			cancel()
		}
		if group != nil {
			if err := group.Wait(); err != nil {
				return err
			}
		}
		state.Swap(svcs.ServiceState_Stopped)
		return nil
	}
}

// registerMCPService wires the MCP service into the controller using helper funcs
func registerMCPService(controller *svcs.Controller, cfg *Config, lgr *logrus.Logger) {
	if cfg.MCP == nil || cfg.MCP.Port == nil || *cfg.MCP.Port <= 0 {
		return
	}
	var state svcs.ServiceState
	var cancel context.CancelFunc
	var group *errgroup.Group

	svc := &svcs.AnonService{
		InitF: mcpInit(cfg, &state),
		RunF:  mcpRun(cfg, lgr, &state, &cancel, &group),
		StopF: mcpStop(cancel, group, &state),
	}
	_ = controller.Register(svc)
}
