// Copyright 2020 Dolthub, Inc.
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

package dsess

import "github.com/dolthub/go-mysql-server/sql"

type PersistedSession struct {
	DoltSession
	ps sql.PersistableSession
}

var _ DoltSession = &PersistedSession{}

func NewPersistedSession(dsess DoltSession, sqlSess sql.PersistableSession) *PersistedSession {
	return &PersistedSession{dsess, sqlSess}
}

// PersistVariable implements the sql.PersistableSession interface.
func (s *PersistedSession) PersistVariable(ctx *sql.Context, sysVarName string, value interface{}) error {
	return s.ps.PersistVariable(ctx, sysVarName, value)

}

// ResetPersistVariable implements the Session interface.
func (s *PersistedSession) ResetPersistVariable(ctx *sql.Context, sysVarName string) error {
	return s.ps.ResetPersistVariable(ctx, sysVarName)

}

// ResetPersistAll implements the Session interface.
func (s *PersistedSession) ResetPersistAll(ctx *sql.Context) error {
	return s.ps.ResetPersistAll(ctx)
}
